use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use super::{
    OutputMode, aggregate, build_output_envelope, build_partial_batch, partial_schema, spill,
    state::{AggregateState, GroupState},
};
use crate::{
    Error,
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::super::value::CellValue;

#[tokio::test]
async fn output_materialization_transfers_its_workspace_into_the_batch_lease() {
    let groups = vec![BoundExpr::column(0, DataType::Utf8, "key")];
    let aggregates = vec![AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    }];
    let mut state = GroupState::new(
        vec![CellValue::Utf8("leased-output".repeat(128))],
        &aggregates,
    );
    state.aggregates[0].update(&aggregates[0], None).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("rows", DataType::Int64, false),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();

    let output = build_output_envelope(
        &[state],
        &groups,
        &aggregates,
        schema,
        OutputMode::Final,
        &context,
        0,
    )
    .await
    .unwrap();
    assert_eq!(context.memory.used(), output.memory_size());
    drop(output);
    assert_eq!(context.memory.used(), 0);
}

#[test]
fn decimal_aggregates_preserve_scale_nulls_and_overflow() {
    let data_type = DataType::Decimal128(5, 2);
    let sum_expr = aggregate_expr(AggregateFunction::Sum, data_type.clone());
    let mut sum = AggregateState::new(&sum_expr);
    sum.update(&sum_expr, Some(CellValue::Null)).unwrap();
    assert_eq!(sum.finish().unwrap(), CellValue::Null);
    sum.update(&sum_expr, Some(CellValue::Decimal128(125)))
        .unwrap();
    sum.update(&sum_expr, Some(CellValue::Decimal128(275)))
        .unwrap();
    assert_eq!(sum.finish().unwrap(), CellValue::Decimal128(400));

    let avg_expr = aggregate_expr(AggregateFunction::Avg, data_type);
    let mut avg = AggregateState::new(&avg_expr);
    avg.update(&avg_expr, Some(CellValue::Decimal128(100)))
        .unwrap();
    avg.update(&avg_expr, Some(CellValue::Null)).unwrap();
    avg.update(&avg_expr, Some(CellValue::Decimal128(200)))
        .unwrap();
    assert_eq!(avg.finish().unwrap(), CellValue::Float64(1.5));

    let overflow_expr = aggregate_expr(AggregateFunction::Sum, DataType::Decimal128(3, 0));
    let mut overflow = AggregateState::new(&overflow_expr);
    overflow
        .update(&overflow_expr, Some(CellValue::Decimal128(999)))
        .unwrap();
    overflow
        .update(&overflow_expr, Some(CellValue::Decimal128(1)))
        .unwrap();
    assert!(overflow.finish().is_err());
}

#[test]
fn decimal_average_returns_double_without_truncating_to_input_scale() {
    let expression = AggregateExpr {
        function: AggregateFunction::Avg,
        expr: Some(BoundExpr::column(0, DataType::Decimal128(15, 2), "value")),
        data_type: DataType::Float64,
        display_name: "avg(value)".into(),
    };
    let mut average = AggregateState::new(&expression);

    // 9,999 rows of 25.35 and one row of 70.68 average to 25.354533.
    for _ in 0..9_999 {
        average
            .update(&expression, Some(CellValue::Decimal128(2_535)))
            .unwrap();
    }
    average
        .update(&expression, Some(CellValue::Decimal128(7_068)))
        .unwrap();

    let CellValue::Float64(value) = average.finish().unwrap() else {
        panic!("decimal AVG should return FLOAT64");
    };
    assert!(
        (value - 25.354_533).abs() < 1e-12,
        "actual average: {value}"
    );
}

#[test]
fn decimal_average_partial_batch_preserves_exact_sum() {
    let expression = AggregateExpr {
        function: AggregateFunction::Avg,
        expr: Some(BoundExpr::column(0, DataType::Decimal128(15, 2), "value")),
        data_type: DataType::Float64,
        display_name: "avg(value)".into(),
    };
    let mut group = GroupState::new(Vec::new(), std::slice::from_ref(&expression));
    group.aggregates[0]
        .update(&expression, Some(CellValue::Decimal128(100)))
        .unwrap();
    group.aggregates[0]
        .update(&expression, Some(CellValue::Decimal128(201)))
        .unwrap();

    let schema = partial_schema(&[], std::slice::from_ref(&expression));
    assert_eq!(schema.field(0).data_type(), &DataType::Binary);
    assert_eq!(schema.field(1).data_type(), &DataType::UInt64);
    let batch =
        build_partial_batch(&[group], &[], std::slice::from_ref(&expression), schema).unwrap();
    let mut merged = AggregateState::new(&expression);
    let mut column = 0;
    merged
        .merge_partial(&expression, &batch, 0, &mut column)
        .unwrap();

    let CellValue::Float64(value) = merged.finish().unwrap() else {
        panic!("merged decimal AVG should return FLOAT64");
    };
    assert!((value - 1.505).abs() < 1e-12, "actual average: {value}");
}

#[test]
fn signed_sum_partial_can_cross_i64_boundary_then_cancel() {
    let expression = aggregate_expr(AggregateFunction::Sum, DataType::Int64);
    let direct = finished_state(
        &expression,
        [
            CellValue::Int64(i64::MAX),
            CellValue::Int64(1),
            CellValue::Int64(-1),
        ],
    );
    let merged = merge_partial_states(
        &expression,
        [
            vec![CellValue::Int64(i64::MAX), CellValue::Int64(1)],
            vec![CellValue::Int64(-1)],
        ],
    );

    assert_eq!(direct.unwrap(), CellValue::Int64(i64::MAX));
    assert_eq!(merged.unwrap(), CellValue::Int64(i64::MAX));
}

#[test]
fn decimal_sum_partial_can_cross_precision_then_cancel() {
    let expression = aggregate_expr(AggregateFunction::Sum, DataType::Decimal128(3, 0));
    let direct = finished_state(
        &expression,
        [
            CellValue::Decimal128(999),
            CellValue::Decimal128(1),
            CellValue::Decimal128(-1),
        ],
    );
    let merged = merge_partial_states(
        &expression,
        [
            vec![CellValue::Decimal128(999), CellValue::Decimal128(1)],
            vec![CellValue::Decimal128(-1)],
        ],
    );

    assert_eq!(direct.unwrap(), CellValue::Decimal128(999));
    assert_eq!(merged.unwrap(), CellValue::Decimal128(999));
}

#[test]
fn decimal_average_partial_preserves_full_i128_sum() {
    let expression = aggregate_expr(AggregateFunction::Avg, DataType::Decimal128(38, 0));
    let unit = 10_i128.pow(37);
    let values = [
        CellValue::Decimal128(9 * unit),
        CellValue::Decimal128(6 * unit),
        CellValue::Decimal128(-8 * unit),
        CellValue::Decimal128(-6 * unit),
    ];
    let direct = finished_state(&expression, values.clone());
    let merged = merge_partial_states(&expression, [values[..2].to_vec(), values[2..].to_vec()]);

    assert_eq!(merged.unwrap(), direct.unwrap());
}

#[test]
fn unsigned_sum_partial_defers_overflow_until_final_value() {
    let expression = aggregate_expr(AggregateFunction::Sum, DataType::UInt64);
    let direct = finished_state(
        &expression,
        [CellValue::UInt64(u64::MAX), CellValue::UInt64(1)],
    )
    .unwrap_err();
    let merged = merge_partial_states(
        &expression,
        [vec![CellValue::UInt64(u64::MAX), CellValue::UInt64(1)]],
    )
    .unwrap_err();

    assert_eq!(direct.to_string(), "execution error: sum overflowed UINT64");
    assert_eq!(merged.to_string(), direct.to_string());
}

fn finished_state(
    expression: &AggregateExpr,
    values: impl IntoIterator<Item = CellValue>,
) -> crate::Result<CellValue> {
    let mut state = AggregateState::new(expression);
    for value in values {
        state.update(expression, Some(value))?;
    }
    state.finish()
}

fn merge_partial_states(
    expression: &AggregateExpr,
    partitions: impl IntoIterator<Item = Vec<CellValue>>,
) -> crate::Result<CellValue> {
    let schema = partial_schema(&[], std::slice::from_ref(expression));
    assert_eq!(schema.field(0).data_type(), &DataType::Binary);
    let mut merged = AggregateState::new(expression);
    for values in partitions {
        let mut group = GroupState::new(Vec::new(), std::slice::from_ref(expression));
        for value in values {
            group.aggregates[0].update(expression, Some(value))?;
        }
        let batch = build_partial_batch(
            &[group],
            &[],
            std::slice::from_ref(expression),
            Arc::clone(&schema),
        )?;
        let mut column = 0;
        merged.merge_partial(expression, &batch, 0, &mut column)?;
    }
    merged.finish()
}

fn aggregate_expr(function: AggregateFunction, data_type: DataType) -> AggregateExpr {
    AggregateExpr {
        function,
        expr: Some(BoundExpr::column(0, data_type.clone(), "value")),
        data_type: if function == AggregateFunction::Avg {
            DataType::Float64
        } else {
            data_type
        },
        display_name: function.to_string(),
    }
}

#[test]
fn spill_merge_streams_and_accounts_many_batches_from_one_file() {
    const BATCHES: usize = 128;
    const ROWS_PER_BATCH: usize = 64;

    let groups = vec![BoundExpr::column(0, DataType::Int64, "key")];
    let count = AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    };
    let aggregates = vec![count];
    let schema = partial_schema(&groups, &aggregates);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7; ROWS_PER_BATCH])),
            Arc::new(Int64Array::from(vec![1; ROWS_PER_BATCH])),
        ],
    )
    .unwrap();
    let batch_bytes = batch.get_array_memory_size().max(1);
    let total_batch_bytes = batch_bytes * BATCHES;
    let memory_limit = batch_bytes.saturating_mul(3).max(32 * 1_024);
    assert!(total_batch_bytes > memory_limit);

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(memory_limit), temp.path()).unwrap();
    let mut writer = context
        .spill
        .writer("aggregate-many-batches", Arc::clone(&schema))
        .unwrap();
    for _ in 0..BATCHES {
        writer.write_batch(&batch).unwrap();
    }
    let file = writer.finish(1).unwrap();
    let state_pool = context
        .memory
        .child("aggregate-test", memory_limit.saturating_mul(2) / 3);
    let mut reservation = state_pool.reservation();

    let outcome = spill::merge_partition(
        std::slice::from_ref(&file),
        &groups,
        &aggregates,
        &context,
        &mut reservation,
    )
    .unwrap();
    let spill::MergeOutcome::Merged(states) = outcome else {
        panic!("one repeated key should fit without repartitioning");
    };
    assert_eq!(states.len(), 1);
    assert_eq!(
        states[0].aggregates[0].finish().unwrap(),
        CellValue::Int64((BATCHES * ROWS_PER_BATCH) as i64)
    );
    assert!(context.memory.peak() >= batch_bytes);
    assert!(context.memory.peak() < total_batch_bytes);
    assert!(context.memory.peak() <= memory_limit);

    drop(states);
    reservation.try_resize(0).unwrap();
    context.spill.remove_file(&file).unwrap();
    assert_eq!(context.memory.used(), 0);
}

#[test]
fn spill_merge_reports_when_one_ipc_batch_exceeds_budget() {
    let groups = vec![BoundExpr::column(0, DataType::Utf8, "key")];
    let count = AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    };
    let aggregates = vec![count];
    let schema = partial_schema(&groups, &aggregates);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["x".repeat(64 * 1_024)])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    assert!(batch.get_array_memory_size() > 1_024);

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(32 * 1_024), temp.path()).unwrap();
    let file = context
        .spill
        .write_record_batches("aggregate-oversized-batch", Arc::clone(&schema), [batch])
        .unwrap();
    let state_pool = context.memory.child("aggregate-test", 21_845);
    let mut reservation = state_pool.reservation();
    let error = spill::merge_partition(
        std::slice::from_ref(&file),
        &groups,
        &aggregates,
        &context,
        &mut reservation,
    )
    .err()
    .expect("oversized spill batch should fail its reservation");
    assert!(matches!(
        error,
        Error::ResourceExhausted(message)
            if message.contains("one IPC batch")
                && message.contains("query limit 32768 bytes")
    ));
    assert!(context.memory.used() > 0);
    context.spill.remove_file(&file).unwrap();
    assert_eq!(context.memory.used(), 0);
}

#[test]
fn spill_merge_accounts_for_the_index_copy_of_long_string_keys() {
    let groups = vec![BoundExpr::column(0, DataType::Utf8, "key")];
    let aggregates = vec![AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    }];
    let schema = partial_schema(&groups, &aggregates);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["x".repeat(16 << 10)])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(256 << 10), temp.path()).unwrap();
    let file = context
        .spill
        .write_record_batches("aggregate-long-index-key", schema, [batch])
        .unwrap();
    let state_pool = context.memory.child("aggregate-long-index", 24 << 10);
    let mut reservation = state_pool.reservation();
    let error = spill::merge_partition(
        std::slice::from_ref(&file),
        &groups,
        &aggregates,
        &context,
        &mut reservation,
    )
    .err()
    .expect("state plus hash-index key copies must exceed the child budget");
    assert!(matches!(
        error,
        Error::ResourceExhausted(message) if message.contains("one aggregate group")
    ));
    context.spill.remove_file(&file).unwrap();
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn spilling_signed_sum_preserves_transient_wide_partial_and_cleans_up() {
    const GROUPS: i64 = 10_000;
    const LOW_MEMORY_LIMIT: usize = 2 << 20;
    const HIGH_MEMORY_LIMIT: usize = 64 << 20;

    let mut keys = Vec::with_capacity(GROUPS as usize + 3);
    let mut values = Vec::with_capacity(GROUPS as usize + 3);
    keys.extend([0, 0]);
    values.extend([i64::MAX, 1]);
    for key in 1..=GROUPS {
        keys.push(key);
        values.push(0);
    }
    keys.push(0);
    values.push(-1);
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();

    let high_temp = tempfile::tempdir().unwrap();
    let high_context =
        QueryContext::shared(MemoryPool::new(HIGH_MEMORY_LIMIT), high_temp.path()).unwrap();
    let expected = collect_grouped_sums(input.clone(), Arc::clone(&high_context))
        .await
        .unwrap();
    assert_eq!(expected[&0], i64::MAX);
    assert_eq!(high_context.metrics.snapshot().spill_bytes, 0);
    drop(high_context);

    let low_temp = tempfile::tempdir().unwrap();
    let low_context =
        QueryContext::shared(MemoryPool::new(LOW_MEMORY_LIMIT), low_temp.path()).unwrap();
    let spill_directory = low_context.spill.directory().to_owned();
    let actual = collect_grouped_sums(input, Arc::clone(&low_context))
        .await
        .unwrap();

    assert_eq!(actual, expected);
    let metrics = low_context.metrics.snapshot();
    assert!(metrics.spill_bytes > 0);
    assert!(metrics.spill_partitions > 0);
    assert!(metrics.peak_memory_bytes <= LOW_MEMORY_LIMIT as u64);
    assert_eq!(low_context.memory.used(), 0);
    assert!(
        std::fs::read_dir(&spill_directory)
            .unwrap()
            .find(|entry| {
                entry.as_ref().is_ok_and(|entry| {
                    entry
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "arrow")
                })
            })
            .is_none(),
        "aggregate left spill files after stream completion"
    );
    drop(low_context);
    assert!(!spill_directory.exists());
}

async fn collect_grouped_sums(
    input: RecordBatch,
    context: Arc<QueryContext>,
) -> crate::Result<HashMap<i64, i64>> {
    let stream = boxed_record_batch_stream(futures::stream::once(async move { Ok(input) }));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("total", DataType::Int64, true),
    ]));
    let sum = AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(1, DataType::Int64, "value")),
        data_type: DataType::Int64,
        display_name: "sum(value)".into(),
    };
    let batches = aggregate(
        stream,
        vec![BoundExpr::column(0, DataType::Int64, "key")],
        vec![sum],
        output_schema,
        context,
        256,
    )
    .try_collect::<Vec<_>>()
    .await?;

    let mut values = HashMap::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.insert(keys.value(row), sums.value(row));
        }
    }
    Ok(values)
}

#[tokio::test]
async fn recursively_repartitions_a_seed_skewed_spill_partition() {
    const MEMORY_LIMIT: usize = 2 << 20;
    const GROUPS: usize = 24_000;

    let keys = (0_i64..1_000_000)
        .filter(|key| {
            spill::partition_for_key(&[CellValue::Int64(*key)], spill::SPILL_PARTITIONS, 0) == 0
        })
        .take(GROUPS)
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), GROUPS);
    let values = keys.iter().chain(keys.iter()).copied().collect::<Vec<_>>();
    let input_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let input_batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let input = boxed_record_batch_stream(futures::stream::once(async move { Ok(input_batch) }));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("rows", DataType::Int64, false),
    ]));
    let count = AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    };
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();

    let batches = aggregate(
        input,
        vec![BoundExpr::column(0, DataType::Int64, "key")],
        vec![count],
        output_schema,
        Arc::clone(&context),
        11,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let mut actual = HashMap::new();
    for batch in batches {
        let output_keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            actual.insert(output_keys.value(row), counts.value(row));
        }
    }
    assert_eq!(actual.len(), GROUPS);
    assert!(actual.values().all(|count| *count == 2));
    assert!(context.metrics.snapshot().spill_partitions > spill::SPILL_PARTITIONS as u64);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn reports_when_one_group_cannot_fit() {
    let value = "x".repeat(1_024);
    let input_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
    let input_batch =
        RecordBatch::try_new(input_schema, vec![Arc::new(StringArray::from(vec![value]))]).unwrap();
    let input = boxed_record_batch_stream(futures::stream::once(async move { Ok(input_batch) }));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("rows", DataType::Int64, false),
    ]));
    let count = AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    };
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(256), temp.path()).unwrap();

    let error = aggregate(
        input,
        vec![BoundExpr::column(0, DataType::Utf8, "key")],
        vec![count],
        output_schema,
        context,
        8,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        Error::ResourceExhausted(message)
            if message.contains("aggregate input") && message.contains("query limit")
    ));
}

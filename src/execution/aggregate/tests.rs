use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, DictionaryArray, Float64Array, Int64Array,
        StringArray, UInt32Array,
    },
    datatypes::{DataType, Field, Schema, UInt32Type},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use super::{
    OutputMode, aggregate, build_output_envelope, build_partial_batch, dense_dictionary,
    key::{GroupIndex, GroupKeyEncoder},
    output_chunk_len, partial_schema, spill,
    state::{AggregateState, GroupState},
    try_apply_dense_dictionary_batch,
};
use crate::{
    Error,
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::super::value::CellValue;

#[tokio::test]
async fn low_cardinality_encoded_groups_preserve_count_and_decimal_sum() {
    let decimal_type = DataType::Decimal128(10, 2);
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("quantity", decimal_type.clone(), true),
    ]));
    let batch = |keys: Vec<Option<&str>>, values: Vec<Option<i128>>| {
        let quantities = Decimal128Array::from(values)
            .with_precision_and_scale(10, 2)
            .unwrap();
        RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(StringArray::from(keys)), Arc::new(quantities)],
        )
        .unwrap()
    };
    let batches = vec![
        batch(
            vec![Some("A"), Some("A"), Some("B"), None],
            vec![Some(100), None, Some(250), Some(400)],
        ),
        batch(
            vec![Some("B"), Some("A"), None, Some("B")],
            vec![Some(50), Some(300), None, Some(75)],
        ),
    ];
    let aggregates = vec![
        AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        },
        AggregateExpr {
            function: AggregateFunction::Sum,
            expr: Some(BoundExpr::column(1, decimal_type, "quantity")),
            distinct: false,
            data_type: DataType::Decimal128(38, 2),
            display_name: "sum(quantity)".into(),
        },
    ];
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("rows", DataType::Int64, false),
        Field::new("quantity", DataType::Decimal128(38, 2), true),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    context.configure_compute_lanes(4);
    let output = aggregate(
        boxed_record_batch_stream(futures::stream::iter(batches.into_iter().map(Ok))),
        vec![BoundExpr::column(0, DataType::Utf8, "key")],
        aggregates,
        output_schema,
        Arc::clone(&context),
        64,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let mut actual = HashMap::new();
    for batch in &output {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = (!keys.is_null(row)).then(|| keys.value(row).to_owned());
            actual.insert(key, (counts.value(row), sums.value(row)));
        }
    }
    assert_eq!(actual[&Some("A".into())], (3, 400));
    assert_eq!(actual[&Some("B".into())], (3, 375));
    assert_eq!(actual[&None], (2, 400));
    drop(output);
    assert_eq!(context.memory.used(), 0);
}

#[test]
fn dense_workspace_shortage_falls_back_without_state_pollution() {
    let group_arrays = vec![Arc::new(
        DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from(vec![Some(0), Some(0)]),
            Arc::new(StringArray::from(vec!["A"])),
        )
        .unwrap(),
    ) as ArrayRef];
    let key_encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Utf8, "key")]);
    let encoded_groups = key_encoder.encode(&group_arrays).unwrap();
    let aggregate = AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(1, DataType::Decimal128(38, 0), "value")),
        distinct: false,
        data_type: DataType::Decimal128(38, 0),
        display_name: "sum(value)".into(),
    };
    let values = Arc::new(
        Decimal128Array::from(vec![1, 2])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    ) as ArrayRef;
    let aggregates = vec![aggregate];
    let aggregate_arrays = vec![Some(values)];
    let required = dense_dictionary::workspace_estimate(aggregates.len());
    let workspace_pool = MemoryPool::new(required);
    let blocker = workspace_pool.try_reserve(required).unwrap();
    let mut workspace = workspace_pool.reservation();
    let state_pool = MemoryPool::new(1 << 20);
    let mut state_memory = state_pool.reservation();
    let mut group_index = GroupIndex::new();
    let mut states = Vec::new();

    let applied = try_apply_dense_dictionary_batch(
        &key_encoder,
        &encoded_groups,
        &group_arrays,
        &aggregates,
        &aggregate_arrays,
        2,
        &mut group_index,
        &mut states,
        &mut state_memory,
        &mut workspace,
    )
    .unwrap();
    assert!(!applied);
    assert!(group_index.is_empty());
    assert!(states.is_empty());
    assert_eq!(workspace.size(), 0);
    assert_eq!(state_memory.size(), 0);

    drop(blocker);
    let no_state_pool = MemoryPool::new(0);
    let mut no_state_memory = no_state_pool.reservation();
    let applied = try_apply_dense_dictionary_batch(
        &key_encoder,
        &encoded_groups,
        &group_arrays,
        &aggregates,
        &aggregate_arrays,
        2,
        &mut group_index,
        &mut states,
        &mut no_state_memory,
        &mut workspace,
    )
    .unwrap();
    assert!(!applied);
    assert!(group_index.is_empty());
    assert!(states.is_empty());
    assert_eq!(workspace.size(), 0);
    assert_eq!(no_state_memory.size(), 0);
}

#[test]
fn dense_workspace_long_utf8_binary_payload_restores_original_reservation() {
    let long_utf8 = "u".repeat(8 << 10);
    let long_binary = vec![7_u8; 8 << 10];
    let group_arrays = vec![
        Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                UInt32Array::from(vec![Some(0)]),
                Arc::new(StringArray::from(vec![long_utf8.as_str()])),
            )
            .unwrap(),
        ) as ArrayRef,
        Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                UInt32Array::from(vec![Some(0)]),
                Arc::new(BinaryArray::from(vec![long_binary.as_slice()])),
            )
            .unwrap(),
        ) as ArrayRef,
    ];
    let key_encoder = GroupKeyEncoder::new(&[
        BoundExpr::column(0, DataType::Utf8, "text_key"),
        BoundExpr::column(1, DataType::Binary, "binary_key"),
    ]);
    let encoded_groups = key_encoder.encode(&group_arrays).unwrap();
    let aggregates = vec![AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(2, DataType::Decimal128(38, 0), "value")),
        distinct: false,
        data_type: DataType::Decimal128(38, 0),
        display_name: "sum(value)".into(),
    }];
    let aggregate_arrays = vec![Some(Arc::new(
        Decimal128Array::from(vec![1])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    ) as ArrayRef)];
    let base = dense_dictionary::workspace_estimate(aggregates.len());
    let original = 97;
    let workspace_pool = MemoryPool::new(original + base + 128);
    let mut workspace = workspace_pool.try_reserve(original).unwrap();
    let state_pool = MemoryPool::new(1 << 20);
    let mut state_memory = state_pool.reservation();
    let mut group_index = GroupIndex::new();
    let mut states = Vec::new();

    let applied = try_apply_dense_dictionary_batch(
        &key_encoder,
        &encoded_groups,
        &group_arrays,
        &aggregates,
        &aggregate_arrays,
        1,
        &mut group_index,
        &mut states,
        &mut state_memory,
        &mut workspace,
    )
    .unwrap();

    assert!(!applied);
    assert!(group_index.is_empty());
    assert!(states.is_empty());
    assert_eq!(state_memory.size(), 0);
    assert_eq!(workspace.size(), original);
    assert_eq!(workspace_pool.used(), original);
}

#[tokio::test]
async fn output_materialization_transfers_its_workspace_into_the_batch_lease() {
    let groups = vec![BoundExpr::column(0, DataType::Utf8, "key")];
    let aggregates = vec![AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct: false,
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
fn output_chunks_are_bounded_by_estimated_variable_width_workspace() {
    let aggregates = vec![AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct: false,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    }];
    let states = (0..16)
        .map(|index| {
            GroupState::new(
                vec![CellValue::Utf8(format!("{index}-{}", "x".repeat(2 << 10)))],
                &aggregates,
            )
        })
        .collect::<Vec<_>>();
    let one = states[0].output_workspace_bytes(2);
    let rows = output_chunk_len(&states, 8_192, 2, one.saturating_mul(3));

    assert_eq!(rows, 2);
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

    let overflow_expr = aggregate_expr(AggregateFunction::Sum, DataType::Decimal128(38, 0));
    let mut overflow = AggregateState::new(&overflow_expr);
    overflow
        .update(
            &overflow_expr,
            Some(CellValue::Decimal128(10_i128.pow(38) - 1)),
        )
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
        distinct: false,
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
        distinct: false,
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

    assert_eq!(direct.unwrap(), CellValue::Decimal128(i128::from(i64::MAX)));
    assert_eq!(merged.unwrap(), CellValue::Decimal128(i128::from(i64::MAX)));
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
fn unsigned_sum_widens_past_u64() {
    let expression = aggregate_expr(AggregateFunction::Sum, DataType::UInt64);
    let direct = finished_state(
        &expression,
        [CellValue::UInt64(u64::MAX), CellValue::UInt64(1)],
    )
    .unwrap();
    let merged = merge_partial_states(
        &expression,
        [vec![CellValue::UInt64(u64::MAX), CellValue::UInt64(1)]],
    )
    .unwrap();

    let expected = CellValue::Decimal128(i128::from(u64::MAX) + 1);
    assert_eq!(direct, expected);
    assert_eq!(merged, expected);
}

#[test]
fn unsigned_average_is_exact_across_parallel_partials() {
    let expression = aggregate_expr(AggregateFunction::Avg, DataType::UInt64);
    let values = [
        CellValue::UInt64(u64::MAX),
        CellValue::UInt64(1_948_194_877_894_919_561),
        CellValue::UInt64(610_074),
    ];
    let direct = finished_state(&expression, values.clone()).unwrap();
    let merged =
        merge_partial_states(&expression, [values[..1].to_vec(), values[1..].to_vec()]).unwrap();

    assert_eq!(merged, direct);
    assert_eq!(
        partial_schema(&[], &[expression]).field(0).data_type(),
        &DataType::Binary
    );
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
    let output_type = match (&function, &data_type) {
        (AggregateFunction::Avg, _) => DataType::Float64,
        (AggregateFunction::Sum, DataType::Decimal128(_, scale)) => {
            DataType::Decimal128(38, *scale)
        }
        (
            AggregateFunction::Sum,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64,
        ) => DataType::Decimal128(38, 0),
        _ => data_type.clone(),
    };
    AggregateExpr {
        function,
        expr: Some(BoundExpr::column(0, data_type.clone(), "value")),
        distinct: false,
        data_type: output_type,
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
        distinct: false,
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
        distinct: false,
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
        distinct: false,
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
    assert_eq!(expected[&0], i128::from(i64::MAX));
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while spill_directory.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(!spill_directory.exists());
}

async fn collect_grouped_sums(
    input: RecordBatch,
    context: Arc<QueryContext>,
) -> crate::Result<HashMap<i64, i128>> {
    let stream = boxed_record_batch_stream(futures::stream::once(async move { Ok(input) }));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("total", DataType::Decimal128(38, 0), true),
    ]));
    let sum = AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(1, DataType::Int64, "value")),
        distinct: false,
        data_type: DataType::Decimal128(38, 0),
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
            .downcast_ref::<Decimal128Array>()
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
        distinct: false,
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
        distinct: false,
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

#[tokio::test]
async fn multiple_distinct_aggregates_share_group_scope_and_preserve_ordinary_rows() {
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "a", "a", "b", "b", "b"])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(2),
                None,
                None,
                Some(3),
                Some(3),
            ])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("x"),
                Some("y"),
                Some("z"),
                None,
                Some("q"),
                Some("q"),
            ])),
        ],
    )
    .unwrap();
    let aggregates = vec![
        distinct_expr(
            AggregateFunction::Count,
            1,
            DataType::Int64,
            DataType::Int64,
        ),
        distinct_expr(
            AggregateFunction::Sum,
            1,
            DataType::Int64,
            DataType::Decimal128(38, 0),
        ),
        distinct_expr(
            AggregateFunction::Avg,
            1,
            DataType::Int64,
            DataType::Float64,
        ),
        AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        },
        distinct_expr(AggregateFunction::Count, 2, DataType::Utf8, DataType::Int64),
    ];
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Utf8, false),
        Field::new("distinct_count", DataType::Int64, false),
        Field::new("distinct_sum", DataType::Decimal128(38, 0), true),
        Field::new("distinct_avg", DataType::Float64, true),
        Field::new("all_rows", DataType::Int64, false),
        Field::new("distinct_text", DataType::Int64, false),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    context.configure_compute_lanes(4);
    let batches = aggregate(
        boxed_record_batch_stream(futures::stream::once(async move { Ok(batch) })),
        vec![BoundExpr::column(0, DataType::Utf8, "grp")],
        aggregates,
        output_schema,
        Arc::clone(&context),
        64,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let mut rows = HashMap::new();
    for batch in &batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let averages = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ordinary = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.insert(
                groups.value(row).to_owned(),
                (
                    counts.value(row),
                    sums.value(row),
                    averages.value(row),
                    ordinary.value(row),
                    text.value(row),
                ),
            );
        }
    }
    assert_eq!(rows["a"], (2, 3, 1.5, 4, 3));
    assert_eq!(rows["b"], (1, 3, 3.0, 3, 1));
    drop(batches);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn decimal_distinct_sum_and_average_keep_exact_dedup_values() {
    let decimal_type = DataType::Decimal128(10, 2);
    let values = Decimal128Array::from(vec![Some(100), Some(100), Some(250), None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        decimal_type.clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(input_schema, vec![Arc::new(values)]).unwrap();
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("total", DataType::Decimal128(38, 2), true),
        Field::new("average", DataType::Float64, true),
        Field::new("count", DataType::Int64, false),
    ]));
    let aggregates = vec![
        distinct_expr(
            AggregateFunction::Sum,
            0,
            decimal_type.clone(),
            DataType::Decimal128(38, 2),
        ),
        distinct_expr(
            AggregateFunction::Avg,
            0,
            decimal_type.clone(),
            DataType::Float64,
        ),
        distinct_expr(AggregateFunction::Count, 0, decimal_type, DataType::Int64),
    ];
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(16 << 20), temp.path()).unwrap();
    let batches = aggregate(
        boxed_record_batch_stream(futures::stream::once(async move { Ok(batch) })),
        Vec::new(),
        aggregates,
        output_schema,
        Arc::clone(&context),
        64,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let average = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let count = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 350);
    assert_eq!(average.value(0), 1.75);
    assert_eq!(count.value(0), 2);
    drop(batches);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_distinct_values_merge_once_across_bounded_task_lanes() {
    const UNIQUE: i64 = 12_000;
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let values = (0..UNIQUE)
        .chain(0..UNIQUE)
        .collect::<Vec<_>>()
        .chunks(256)
        .map(|values| {
            RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("unique", DataType::Int64, false),
        Field::new("distinct_total", DataType::Decimal128(38, 0), true),
        Field::new("rows", DataType::Int64, false),
    ]));
    let aggregates = vec![
        distinct_expr(
            AggregateFunction::Count,
            0,
            DataType::Int64,
            DataType::Int64,
        ),
        distinct_expr(
            AggregateFunction::Sum,
            0,
            DataType::Int64,
            DataType::Decimal128(38, 0),
        ),
        AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        },
    ];
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(4 << 20), temp.path()).unwrap();
    context.configure_compute_lanes_unbounded_for_test(4);
    let batches = aggregate(
        boxed_record_batch_stream(futures::stream::iter(values.into_iter().map(Ok))),
        Vec::new(),
        aggregates,
        output_schema,
        Arc::clone(&context),
        256,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let unique = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let total = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let rows = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(unique.value(0), UNIQUE);
    assert_eq!(
        total.value(0),
        i128::from(UNIQUE) * i128::from(UNIQUE - 1) / 2
    );
    assert_eq!(rows.value(0), UNIQUE * 2);
    let metrics = context.metrics.snapshot();
    assert!(metrics.spill_files > 0);
    assert!(metrics.peak_active_lanes >= 2, "metrics: {metrics:?}");
    drop(batches);
    assert_eq!(context.memory.used(), 0);
    assert!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .all(|entry| entry
                .unwrap()
                .path()
                .extension()
                .is_none_or(|ext| ext != "arrow"))
    );
}

fn distinct_expr(
    function: AggregateFunction,
    column: usize,
    input_type: DataType,
    output_type: DataType,
) -> AggregateExpr {
    AggregateExpr {
        function,
        expr: Some(BoundExpr::column(column, input_type, "value")),
        distinct: true,
        data_type: output_type,
        display_name: format!("{function}(distinct value)"),
    }
}

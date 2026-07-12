use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt};

use super::{condition::JoinPredicates, grace, join, parallel, sort_merge, spill};
use crate::{
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{BinaryOp, BoundExpr, ExprKind, JoinType, ScalarValue},
};

#[tokio::test]
async fn null_aware_global_group_obeys_empty_and_null_rhs_semantics() {
    let empty_mark = run_null_aware_join(
        JoinType::Mark,
        false,
        vec![Some(1), Some(2), None],
        Vec::new(),
        vec![Some(7); 3],
        Vec::new(),
    )
    .await;
    assert_eq!(
        mark_values(&empty_mark),
        vec![(0, Some(false)), (1, Some(false)), (2, Some(false))]
    );

    let null_mark = run_null_aware_join(
        JoinType::Mark,
        false,
        vec![Some(1), Some(2), None],
        vec![Some(1), None],
        vec![Some(7); 3],
        vec![Some(8); 2],
    )
    .await;
    assert_eq!(
        mark_values(&null_mark),
        vec![(0, Some(true)), (1, None), (2, None)]
    );

    let empty_anti = run_null_aware_join(
        JoinType::NullAwareAnti,
        false,
        vec![Some(1), Some(2), None],
        Vec::new(),
        vec![Some(7); 3],
        Vec::new(),
    )
    .await;
    assert_eq!(third_column_ids(&empty_anti), vec![0, 1, 2]);

    let null_anti = run_null_aware_join(
        JoinType::NullAwareAnti,
        false,
        vec![Some(1), Some(2), None],
        vec![Some(1), None],
        vec![Some(7); 3],
        vec![Some(8); 2],
    )
    .await;
    assert!(third_column_ids(&null_anti).is_empty());
}

#[tokio::test]
async fn null_correlation_key_forms_an_empty_rhs_group() {
    let mark = run_null_aware_join(
        JoinType::Mark,
        true,
        vec![None],
        vec![None],
        vec![None],
        vec![None],
    )
    .await;
    assert_eq!(mark_values(&mark), vec![(0, Some(false))]);

    let anti = run_null_aware_join(
        JoinType::NullAwareAnti,
        true,
        vec![None],
        vec![None],
        vec![None],
        vec![None],
    )
    .await;
    assert_eq!(third_column_ids(&anti), vec![0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grace_and_sort_merge_keep_null_correlation_as_an_empty_group() {
    for join_type in [JoinType::Mark, JoinType::NullAwareAnti] {
        let grace = run_grace_null_aware(join_type, None, None, None, None).await;
        let fallback = run_fallback_null_aware(join_type, None, None, None, None).await;
        if join_type == JoinType::Mark {
            assert_eq!(mark_values(&grace), vec![(0, Some(false))]);
            assert_eq!(mark_values(&fallback), vec![(0, Some(false))]);
        } else {
            assert_eq!(third_column_ids(&grace), vec![0]);
            assert_eq!(third_column_ids(&fallback), vec![0]);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grace_and_sort_merge_scope_rhs_nulls_to_the_correlation_group() {
    for join_type in [JoinType::Mark, JoinType::NullAwareAnti] {
        let grace = run_grace_null_aware(join_type, Some(1), Some(2), Some(1), None).await;
        let fallback = run_fallback_null_aware(join_type, Some(1), Some(2), Some(1), None).await;
        if join_type == JoinType::Mark {
            assert_eq!(mark_values(&grace), vec![(0, None)]);
            assert_eq!(mark_values(&fallback), vec![(0, None)]);
        } else {
            assert!(third_column_ids(&grace).is_empty());
            assert!(third_column_ids(&fallback).is_empty());
        }
    }
}

#[tokio::test]
async fn residual_filters_left_single_and_enforces_cardinality() {
    let (left_schema, right_schema) = predicate_schemas();
    let left_batch = predicate_batch(&left_schema, vec![Some(1)], vec![Some(5)]);
    let right_batch = predicate_batch(
        &right_schema,
        vec![Some(1), Some(1)],
        vec![Some(10), Some(20)],
    );
    let output = run_predicate_join(
        joined_right_value_comparison(3, BinaryOp::Gt, 10),
        left_batch.clone(),
        right_batch.clone(),
        Arc::clone(&left_schema),
        Arc::clone(&right_schema),
    )
    .await
    .unwrap();
    assert_eq!(int64(&output[0], 4).value(0), 20);

    let error = run_predicate_join(
        joined_right_value_comparison(3, BinaryOp::GtEq, 10),
        left_batch,
        right_batch,
        left_schema,
        right_schema,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("more than one row"));

    let fallback = run_fallback_left_single(BinaryOp::Gt).await.unwrap();
    assert_eq!(int64(&fallback[0], 4).value(0), 20);
    let error = run_fallback_left_single(BinaryOp::GtEq).await.unwrap_err();
    assert!(error.to_string().contains("more than one row"), "{error}");
}

#[tokio::test]
async fn constant_false_guard_does_not_poll_an_unreachable_right_plan() {
    let (left_schema, right_schema) = predicate_schemas();
    let left = predicate_batch(&left_schema, vec![Some(1)], vec![Some(5)]);
    let output_schema = output_schema(JoinType::LeftSingle, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
    let right = boxed_record_batch_stream(futures::stream::once(async {
        Err(crate::Error::Execution(
            "unreachable right plan was polled".into(),
        ))
    }));

    let batches = join(
        boxed_record_batch_stream(futures::stream::once(async move { Ok(left) })),
        right,
        vec![correlation_keys()],
        Some(BoundExpr::literal(ScalarValue::Boolean(false))),
        None,
        left_schema,
        right_schema,
        JoinType::LeftSingle,
        output_schema,
        context,
        4,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    for column in 3..6 {
        assert!(batches[0].column(column).is_null(0));
    }
}

#[tokio::test]
async fn sort_merge_left_single_reports_cardinality_before_any_output() {
    let (mut output, _temp) = fallback_left_single_stream(BinaryOp::GtEq, 1).unwrap();
    let first = output
        .next()
        .await
        .expect("fallback must emit a terminal result");
    let error = first.unwrap_err();
    assert!(error.to_string().contains("more than one row"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_probe_applies_null_aware_mark_semantics() {
    let (left_schema, right_schema) = predicate_schemas();
    let left_batches = (0..64_i64)
        .map(|id| {
            RecordBatch::try_new(
                Arc::clone(&left_schema),
                vec![
                    Arc::new(Int64Array::from(vec![Some(1)])),
                    Arc::new(Int64Array::from(vec![Some(if id % 2 == 0 {
                        1
                    } else {
                        2
                    })])),
                    Arc::new(Int64Array::from(vec![id])),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let right = predicate_batch(&right_schema, vec![Some(1), Some(1)], vec![Some(1), None]);
    let schema = output_schema(JoinType::Mark, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(128 << 20), temp.path()).unwrap();
    context.configure_compute_lanes(4);
    parallel::synchronize_probe_start(context.query_id);
    let batches = join(
        boxed_record_batch_stream(futures::stream::iter(left_batches.into_iter().map(Ok))),
        boxed_record_batch_stream(futures::stream::once(async move { Ok(right) })),
        vec![correlation_keys()],
        None,
        Some(membership_values()),
        left_schema,
        right_schema,
        JoinType::Mark,
        schema,
        Arc::clone(&context),
        8,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let mut true_count = 0;
    let mut null_count = 0;
    for batch in batches {
        let markers = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if markers.is_null(row) {
                null_count += 1;
            } else if markers.value(row) {
                true_count += 1;
            }
        }
    }
    assert_eq!((true_count, null_count), (32, 32));
    assert_eq!(context.metrics.snapshot().peak_active_lanes, 4);
}

async fn run_null_aware_join(
    join_type: JoinType,
    correlated: bool,
    left_values: Vec<Option<i64>>,
    right_values: Vec<Option<i64>>,
    left_correlation: Vec<Option<i64>>,
    right_correlation: Vec<Option<i64>>,
) -> Vec<RecordBatch> {
    let (left_schema, right_schema) = predicate_schemas();
    let left = predicate_batch(&left_schema, left_correlation, left_values);
    let right = predicate_batch(&right_schema, right_correlation, right_values);
    let on = if correlated {
        vec![correlation_keys()]
    } else {
        Vec::new()
    };
    let schema = output_schema(join_type, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    join(
        boxed_record_batch_stream(futures::stream::once(async move { Ok(left) })),
        boxed_record_batch_stream(futures::stream::once(async move { Ok(right) })),
        on,
        None,
        Some(membership_values()),
        left_schema,
        right_schema,
        join_type,
        schema,
        context,
        4,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect()
    .await
    .unwrap()
}

async fn run_grace_null_aware(
    join_type: JoinType,
    left_correlation: Option<i64>,
    left_value: Option<i64>,
    right_correlation: Option<i64>,
    right_value: Option<i64>,
) -> Vec<RecordBatch> {
    let (left_schema, right_schema) = predicate_schemas();
    let left = predicate_batch(&left_schema, vec![left_correlation], vec![left_value]);
    let right = predicate_batch(&right_schema, vec![right_correlation], vec![right_value]);
    let (left_key, right_key) = correlation_keys();
    let left_keys = vec![left_key];
    let right_keys = vec![right_key];
    let predicates =
        JoinPredicates::new(None, Some(membership_values()), &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    context.configure_compute_lanes(2);
    let mut left_spiller = spill::PartitionSpiller::new(&context, "null-key-grace-left");
    spill::spill_batch(
        left,
        &left_keys,
        spill::Side::Left,
        join_type,
        &mut left_spiller,
        0,
    )
    .unwrap();
    let mut right_spiller = spill::PartitionSpiller::new(&context, "null-key-grace-right");
    spill::spill_batch(
        right,
        &right_keys,
        spill::Side::Right,
        join_type,
        &mut right_spiller,
        0,
    )
    .unwrap();
    let tasks = spill::initial_tasks(
        left_spiller.finish().unwrap(),
        right_spiller.finish().unwrap(),
    );
    let schema = output_schema(join_type, &left_schema, &right_schema);
    grace::join(
        tasks,
        left_keys,
        right_keys,
        left_schema,
        right_schema,
        predicates,
        join_type,
        schema,
        context,
        4,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect()
    .await
    .unwrap()
}

async fn run_fallback_null_aware(
    join_type: JoinType,
    left_correlation: Option<i64>,
    left_value: Option<i64>,
    right_correlation: Option<i64>,
    right_value: Option<i64>,
) -> Vec<RecordBatch> {
    let (left_schema, right_schema) = predicate_schemas();
    let left = predicate_batch(&left_schema, vec![left_correlation], vec![left_value]);
    let right = predicate_batch(&right_schema, vec![right_correlation], vec![right_value]);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    let left_file = context
        .spill
        .write_record_batches("null-key-fallback-left", Arc::clone(&left_schema), [left])
        .unwrap();
    let right_file = context
        .spill
        .write_record_batches(
            "null-key-fallback-right",
            Arc::clone(&right_schema),
            [right],
        )
        .unwrap();
    let task = spill::PartitionTask {
        left: vec![left_file],
        right: vec![right_file],
        depth: spill::MAX_REPARTITION_DEPTH,
        stagnant_repartitions: 0,
    };
    let predicates =
        JoinPredicates::new(None, Some(membership_values()), &left_schema, &right_schema);
    let schema = output_schema(join_type, &left_schema, &right_schema);
    let (left_key, right_key) = correlation_keys();
    sort_merge::fallback(
        task,
        vec![left_key],
        vec![right_key],
        left_schema,
        right_schema,
        predicates,
        join_type,
        schema,
        context,
        4,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect()
    .await
    .unwrap()
}

async fn run_fallback_left_single(op: BinaryOp) -> crate::Result<Vec<RecordBatch>> {
    let (output, _temp) = fallback_left_single_stream(op, 4)?;
    output
        .map_ok(|batch| batch.into_public())
        .try_collect()
        .await
}

fn fallback_left_single_stream(
    op: BinaryOp,
    batch_size: usize,
) -> crate::Result<(crate::runtime::MemoryBatchStream, tempfile::TempDir)> {
    let (left_schema, right_schema) = predicate_schemas();
    let left = predicate_batch(&left_schema, vec![Some(1)], vec![Some(5)]);
    let right = predicate_batch(
        &right_schema,
        vec![Some(1), Some(1)],
        vec![Some(10), Some(20)],
    );
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    let left_file = context.spill.write_record_batches(
        "single-fallback-left",
        Arc::clone(&left_schema),
        [left],
    )?;
    let right_file = context.spill.write_record_batches(
        "single-fallback-right",
        Arc::clone(&right_schema),
        [right],
    )?;
    let task = spill::PartitionTask {
        left: vec![left_file],
        right: vec![right_file],
        depth: spill::MAX_REPARTITION_DEPTH,
        stagnant_repartitions: 0,
    };
    let predicates = JoinPredicates::new(
        Some(joined_right_value_comparison(3, op, 10)),
        None,
        &left_schema,
        &right_schema,
    );
    let schema = output_schema(JoinType::LeftSingle, &left_schema, &right_schema);
    let (left_key, right_key) = correlation_keys();
    let output = sort_merge::fallback(
        task,
        vec![left_key],
        vec![right_key],
        left_schema,
        right_schema,
        predicates,
        JoinType::LeftSingle,
        schema,
        context,
        batch_size,
    );
    Ok((output, temp))
}

async fn run_predicate_join(
    residual: BoundExpr,
    left: RecordBatch,
    right: RecordBatch,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
) -> crate::Result<Vec<RecordBatch>> {
    let schema = output_schema(JoinType::LeftSingle, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    join(
        boxed_record_batch_stream(futures::stream::once(async move { Ok(left) })),
        boxed_record_batch_stream(futures::stream::once(async move { Ok(right) })),
        vec![correlation_keys()],
        Some(residual),
        None,
        left_schema,
        right_schema,
        JoinType::LeftSingle,
        schema,
        context,
        4,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect()
    .await
}

fn correlation_keys() -> (BoundExpr, BoundExpr) {
    (
        BoundExpr::column(0, DataType::Int64, "left.correlation"),
        BoundExpr::column(0, DataType::Int64, "right.correlation"),
    )
}

fn membership_values() -> (BoundExpr, BoundExpr) {
    (
        BoundExpr::column(1, DataType::Int64, "left.value"),
        BoundExpr::column(1, DataType::Int64, "right.value"),
    )
}

fn predicate_schemas() -> (SchemaRef, SchemaRef) {
    let fields = || {
        vec![
            Field::new("correlation", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("id", DataType::Int64, false),
        ]
    };
    (
        Arc::new(Schema::new(fields())),
        Arc::new(Schema::new(fields())),
    )
}

fn predicate_batch(
    schema: &SchemaRef,
    correlation: Vec<Option<i64>>,
    values: Vec<Option<i64>>,
) -> RecordBatch {
    let rows = values.len();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(correlation)),
            Arc::new(Int64Array::from(values)),
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
        ],
    )
    .unwrap()
}

fn joined_right_value_comparison(left_columns: usize, op: BinaryOp, value: i64) -> BoundExpr {
    BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(
                left_columns + 1,
                DataType::Int64,
                "right.value",
            )),
            op,
            right: Box::new(BoundExpr::literal(ScalarValue::Int64(value))),
        },
        data_type: DataType::Boolean,
        display_name: "right.value comparison".into(),
    }
}

fn output_schema(join_type: JoinType, left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
    if join_type == JoinType::NullAwareAnti {
        return Arc::clone(left);
    }
    let mut fields = left.fields().iter().cloned().collect::<Vec<_>>();
    if join_type == JoinType::Mark {
        fields.push(Arc::new(Field::new("marker", DataType::Boolean, true)));
    } else {
        fields.extend(
            right
                .fields()
                .iter()
                .map(|field| Arc::new(Field::new(field.name(), field.data_type().clone(), true))),
        );
    }
    Arc::new(Schema::new(fields))
}

fn mark_values(batches: &[RecordBatch]) -> Vec<(i64, Option<bool>)> {
    let mut values = batches
        .iter()
        .flat_map(|batch| {
            let ids = int64(batch, 2);
            let markers = batch
                .column(3)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    (
                        ids.value(row),
                        markers.is_valid(row).then(|| markers.value(row)),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort_unstable_by_key(|(id, _)| *id);
    values
}

fn third_column_ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids = batches
        .iter()
        .flat_map(|batch| int64(batch, 2).values().iter().copied())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn int64(batch: &RecordBatch, column: usize) -> &Int64Array {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

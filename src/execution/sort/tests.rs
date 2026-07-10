use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{TryStreamExt, stream};
use tempfile::tempdir;

use super::{MERGE_FAN_IN, sort};
use crate::runtime::{MemoryPool, QueryContext, boxed_record_batch_stream};
use crate::sql::{BoundExpr, SortExpr};

#[tokio::test]
async fn orders_multiple_keys_with_explicit_null_placement() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(1), Some(2)])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("z"),
                Some("c"),
                None,
            ])),
        ],
    )
    .unwrap();
    let expressions = vec![
        SortExpr {
            expr: BoundExpr::column(0, DataType::Int64, "group"),
            descending: false,
            nulls_first: false,
        },
        SortExpr {
            expr: BoundExpr::column(1, DataType::Utf8, "name"),
            descending: true,
            nulls_first: false,
        },
    ];
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(1024 * 1024), directory.path()).unwrap());
    let input = boxed_record_batch_stream(stream::iter(vec![Ok(batch)]));

    let batches = sort(input, expressions, None, schema, context, 2)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let groups: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..values.len())
                .map(|row| (!values.is_null(row)).then(|| values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect();
    let names: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..values.len())
                .map(|row| (!values.is_null(row)).then(|| values.value(row).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(groups, vec![Some(1), Some(1), Some(2), None]);
    assert_eq!(
        names,
        vec![Some("c".into()), Some("a".into()), None, Some("z".into())]
    );
}

#[tokio::test]
async fn spills_and_merges_top_k_with_bounded_memory() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let mut input_batches = Vec::new();
    for chunk in (0_i64..24).rev() {
        let values: Vec<_> = (0_i64..128)
            .rev()
            .map(|offset| chunk * 128 + offset)
            .collect();
        input_batches.push(Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()));
    }
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024), directory.path()).unwrap());
    let input = boxed_record_batch_stream(stream::iter(input_batches));
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Int64, "value"),
        descending: true,
        nulls_first: false,
    };

    let batches = sort(
        input,
        vec![expression],
        Some(50),
        Arc::clone(&schema),
        Arc::clone(&context),
        64,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values, (3022_i64..3072).rev().collect::<Vec<_>>());
    let metrics = context.metrics.snapshot();
    assert!(metrics.spill_bytes > 0);
    assert!(metrics.spill_partitions > MERGE_FAN_IN as u64);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .count(),
        0
    );
    assert!(context.memory.peak() <= context.memory.limit());
}

#[tokio::test]
async fn slices_a_single_input_batch_that_exceeds_the_sort_budget() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let values = (0_i64..1_024).rev().collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let directory = tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(8 * 1024), directory.path()).unwrap());
    let input = boxed_record_batch_stream(stream::iter([Ok(batch)]));
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Int64, "value"),
        descending: false,
        nulls_first: false,
    };

    let batches = sort(
        input,
        vec![expression],
        None,
        Arc::clone(&schema),
        Arc::clone(&context),
        128,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (0_i64..1_024).collect::<Vec<_>>());
    assert!(context.metrics.snapshot().spill_bytes > 0);
    assert!(context.memory.peak() <= context.memory.limit());
}

use std::{sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, BinaryArray, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{TryStreamExt, stream};

use crate::{
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{BoundExpr, JoinType},
};

use super::super::join::join;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_bounded_lanes_let_nested_join_queues_reach_spill() {
    const MEMORY_LIMIT: usize = 128 << 20;
    const REQUESTED_LANES: usize = 18;
    const BATCHES: i64 = 5;
    const ROWS: usize = 4;
    const PAYLOAD_COLUMNS: usize = 4;
    const PAYLOAD_BYTES: usize = 512 << 10;

    let input_schema = wide_schema("input", PAYLOAD_COLUMNS);
    let input_batches = (0..BATCHES)
        .map(|batch| {
            wide_batch(
                Arc::clone(&input_schema),
                batch,
                ROWS,
                PAYLOAD_COLUMNS,
                PAYLOAD_BYTES,
            )
        })
        .collect::<Vec<_>>();
    let lookup_schema = Arc::new(Schema::new(vec![
        Field::new("lookup_key", DataType::Int64, false),
        Field::new("lookup_value", DataType::Int64, false),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    context.configure_compute_lanes(REQUESTED_LANES);
    assert_eq!(context.scheduler.configured_lanes(), 4);

    let first_schema = inner_schema(&input_schema, &lookup_schema);
    let first = join(
        boxed_record_batch_stream(stream::iter(input_batches.into_iter().map(Ok))),
        lookup(Arc::clone(&lookup_schema)),
        equality(0, 0),
        None,
        None,
        Arc::clone(&input_schema),
        Arc::clone(&lookup_schema),
        JoinType::Inner,
        Arc::clone(&first_schema),
        Arc::clone(&context),
        ROWS,
    );

    let second_schema = inner_schema(&first_schema, &lookup_schema);
    let second = join(
        first,
        lookup(Arc::clone(&lookup_schema)),
        equality(0, 0),
        None,
        None,
        first_schema,
        Arc::clone(&lookup_schema),
        JoinType::Inner,
        Arc::clone(&second_schema),
        Arc::clone(&context),
        ROWS,
    );

    let probe_schema = Arc::new(Schema::new(vec![Field::new(
        "probe_id",
        DataType::Int64,
        false,
    )]));
    let probe = RecordBatch::try_new(
        Arc::clone(&probe_schema),
        vec![Arc::new(Int64Array::from(vec![-1]))],
    )
    .unwrap();
    let output_schema = inner_schema(&probe_schema, &second_schema);
    let output = join(
        boxed_record_batch_stream(stream::once(async move { Ok(probe) })),
        second,
        equality(0, 1),
        None,
        None,
        probe_schema,
        second_schema,
        JoinType::Inner,
        output_schema,
        Arc::clone(&context),
        ROWS,
    )
    .try_collect::<Vec<_>>();

    let batches = tokio::time::timeout(Duration::from_secs(60), output)
        .await
        .expect("memory-bounded nested join lanes must not deadlock")
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        0
    );
    assert!(context.metrics.snapshot().spill_write_bytes > 0);
    drop(batches);
    context.cleanup_spill_after_tasks().await.unwrap();
    assert_eq!(context.memory.used(), 0);
}

fn wide_schema(prefix: &str, payload_columns: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new(format!("{prefix}_lookup_key"), DataType::Int64, false),
        Field::new(format!("{prefix}_id"), DataType::Int64, false),
    ];
    fields.extend((0..payload_columns).map(|column| {
        Field::new(
            format!("{prefix}_payload_{column}"),
            DataType::Binary,
            false,
        )
    }));
    Arc::new(Schema::new(fields))
}

fn wide_batch(
    schema: SchemaRef,
    id: i64,
    rows: usize,
    payload_columns: usize,
    payload_bytes: usize,
) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(payload_columns + 2);
    columns.push(Arc::new(Int64Array::from(vec![1; rows])));
    columns.push(Arc::new(Int64Array::from(vec![id; rows])));
    let payload = vec![b'x'; payload_bytes];
    for _ in 0..payload_columns {
        columns.push(Arc::new(BinaryArray::from_iter_values(
            (0..rows).map(|_| payload.as_slice()),
        )));
    }
    RecordBatch::try_new(schema, columns).unwrap()
}

fn lookup(schema: SchemaRef) -> crate::runtime::RecordBatchStream {
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![7])),
        ],
    )
    .unwrap();
    boxed_record_batch_stream(stream::once(async move { Ok(batch) }))
}

fn equality(left: usize, right: usize) -> Vec<(BoundExpr, BoundExpr)> {
    vec![(
        BoundExpr::column(left, DataType::Int64, "left_key"),
        BoundExpr::column(right, DataType::Int64, "right_key"),
    )]
}

fn inner_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        left.fields()
            .iter()
            .chain(right.fields())
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

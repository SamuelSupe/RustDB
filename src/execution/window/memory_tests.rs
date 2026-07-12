use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BinaryArray, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{TryStreamExt, stream};

use crate::runtime::{MemoryPool, QueryContext, boxed_record_batch_stream};
use crate::sql::{
    AggregateExpr, AggregateFunction, BoundExpr, SortExpr, WindowExpr, WindowFrame,
    WindowFrameBound, WindowFrameUnits, WindowFunction,
};

const PAYLOAD_BYTES: usize = 1 << 20;

#[tokio::test]
async fn long_whole_values_split_output_within_hard_budget() {
    let rows = 32usize;
    let (input_schema, batch) = long_batch(rows);
    let output_schema = append_fields(
        &input_schema,
        [
            Field::new("min_text", DataType::Utf8, true),
            Field::new("min_bytes", DataType::Binary, true),
        ],
    );
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowFrameBound::UnboundedPreceding,
        end: WindowFrameBound::UnboundedFollowing,
    };
    let expressions = vec![
        aggregate_window(0, DataType::Utf8, "text", vec![], frame),
        aggregate_window(1, DataType::Binary, "bytes", vec![], frame),
    ];
    let (context, mut output, spill_directory, temp) = execute(
        batch,
        input_schema,
        output_schema,
        expressions,
        rows,
        16 << 20,
    );
    let mut output_rows = 0usize;
    let mut output_batches = 0usize;
    while let Some(batch) = output.try_next().await.unwrap() {
        assert_long_outputs(&batch, 2, 3);
        output_rows += batch.num_rows();
        output_batches += 1;
        drop(batch);
    }
    assert_eq!(output_rows, rows);
    assert!(output_batches > 1, "large output was not split");
    finish(context, output, spill_directory, temp, 16 << 20).await;
}

#[tokio::test]
async fn long_partition_range_key_and_peer_values_stay_within_hard_budget() {
    let rows = 3usize;
    let (input_schema, batch) = repeated_long_key_batch(rows);
    let output_schema = append_fields(
        &input_schema,
        [Field::new("running_min", DataType::Binary, true)],
    );
    let order_by = vec![SortExpr {
        expr: BoundExpr::column(0, DataType::Utf8, "text"),
        descending: false,
        nulls_first: false,
    }];
    let mut expression = aggregate_window(
        1,
        DataType::Binary,
        "bytes",
        order_by,
        WindowFrame {
            units: WindowFrameUnits::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::CurrentRow,
        },
    );
    expression.partition_by = vec![BoundExpr::column(0, DataType::Utf8, "text")];
    let limit = 32 << 20;
    let (context, mut output, spill_directory, temp) = execute(
        batch,
        input_schema,
        output_schema,
        vec![expression],
        rows,
        limit,
    );
    let mut output_rows = 0usize;
    while let Some(batch) = output.try_next().await.unwrap() {
        let binary = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(binary.value(row).len(), PAYLOAD_BYTES);
            assert_eq!(binary.value(row)[0], 0);
        }
        output_rows += batch.num_rows();
        drop(batch);
    }
    assert_eq!(output_rows, rows);
    finish(context, output, spill_directory, temp, limit).await;
}

fn long_batch(rows: usize) -> (SchemaRef, RecordBatch) {
    let long_text = "a".repeat(PAYLOAD_BYTES);
    let long_binary = vec![0u8; PAYLOAD_BYTES];
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("bytes", DataType::Binary, false),
    ]));
    let text = StringArray::from_iter_values(
        (0..rows).map(|row| if row == 0 { long_text.as_str() } else { "z" }),
    );
    let binary = BinaryArray::from_iter_values((0..rows).map(|row| {
        if row == 0 {
            long_binary.as_slice()
        } else {
            b"\xff".as_slice()
        }
    }));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(text) as ArrayRef, Arc::new(binary) as ArrayRef],
    )
    .unwrap();
    (schema, batch)
}

fn repeated_long_key_batch(rows: usize) -> (SchemaRef, RecordBatch) {
    let long_text = "a".repeat(PAYLOAD_BYTES);
    let long_binary = vec![0u8; PAYLOAD_BYTES];
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("bytes", DataType::Binary, false),
    ]));
    let text = StringArray::from_iter_values((0..rows).map(|_| long_text.as_str()));
    let binary = BinaryArray::from_iter_values((0..rows).map(|row| {
        if row == 0 {
            long_binary.as_slice()
        } else {
            b"\xff".as_slice()
        }
    }));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(text) as ArrayRef, Arc::new(binary) as ArrayRef],
    )
    .unwrap();
    (schema, batch)
}

fn aggregate_window(
    column: usize,
    data_type: DataType,
    name: &str,
    order_by: Vec<SortExpr>,
    frame: WindowFrame,
) -> WindowExpr {
    WindowExpr {
        function: WindowFunction::Aggregate(AggregateExpr {
            function: AggregateFunction::Min,
            expr: Some(BoundExpr::column(column, data_type.clone(), name)),
            distinct: false,
            data_type: data_type.clone(),
            display_name: format!("min({name})"),
        }),
        partition_by: vec![],
        order_by,
        frame,
        data_type,
        display_name: format!("min({name}) OVER (...)"),
    }
}

fn append_fields<const N: usize>(schema: &SchemaRef, fields: [Field; N]) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .chain(fields)
            .collect::<Vec<_>>(),
    ))
}

fn execute(
    batch: RecordBatch,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    expressions: Vec<WindowExpr>,
    batch_size: usize,
    limit: usize,
) -> (
    Arc<QueryContext>,
    super::MemoryBatchStream,
    std::path::PathBuf,
    tempfile::TempDir,
) {
    let input = boxed_record_batch_stream(stream::once(async move { Ok(batch) }));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(limit), temp.path()).unwrap();
    let spill_directory = context.spill.directory().to_path_buf();
    let output = super::window(
        input,
        expressions,
        input_schema,
        output_schema,
        Arc::clone(&context),
        batch_size,
    );
    (context, output, spill_directory, temp)
}

async fn finish(
    context: Arc<QueryContext>,
    output: super::MemoryBatchStream,
    spill_directory: std::path::PathBuf,
    temp: tempfile::TempDir,
    limit: usize,
) {
    drop(output);
    assert!(context.memory.peak() <= limit);
    context.cleanup_spill_after_tasks().await.unwrap();
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
    drop(context);
    assert!(!spill_directory.exists());
    drop(temp);
}

fn assert_long_outputs(batch: &RecordBatch, text_column: usize, binary_column: usize) {
    let text = batch
        .column(text_column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let binary = batch
        .column(binary_column)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    for row in 0..batch.num_rows() {
        assert_eq!(text.value(row).len(), PAYLOAD_BYTES);
        assert_eq!(text.value(row).as_bytes()[0], b'a');
        assert_eq!(binary.value(row).len(), PAYLOAD_BYTES);
        assert_eq!(binary.value(row)[0], 0);
    }
}

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt, stream};
use tempfile::tempdir;

use super::{
    MERGE_FAN_IN, make_converter,
    merge::MergeIterator,
    run::{MAX_PENDING_RUNS, sort_batches},
    sort,
};
use crate::sql::{BoundExpr, SortExpr};
use crate::{
    Error,
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
};

#[tokio::test]
async fn output_materialization_transfers_its_workspace_into_the_batch_lease() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values((0_i64..128).rev()))],
    )
    .unwrap();
    let directory = tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap());
    let input = boxed_record_batch_stream(stream::iter([Ok(batch)]));
    let mut output = sort(
        input,
        vec![SortExpr {
            expr: BoundExpr::column(0, DataType::Int64, "value"),
            descending: false,
            nulls_first: false,
        }],
        None,
        schema,
        Arc::clone(&context),
        64,
    );

    let batch = output.next().await.unwrap().unwrap();
    drop(output);
    assert_eq!(context.memory.used(), batch.memory_size());
    drop(batch);
    assert_eq!(context.memory.used(), 0);
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_lanes_generate_runs_for_a_global_merge() {
    const LANES: usize = 4;
    const BATCHES: i64 = 24;
    const ROWS_PER_BATCH: i64 = 2_048;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let input_batches = (0..BATCHES)
        .rev()
        .map(|batch| {
            let start = batch * ROWS_PER_BATCH;
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(
                    (start..start + ROWS_PER_BATCH).rev(),
                ))],
            )
            .unwrap())
        })
        .collect::<Vec<_>>();
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(128 << 20), directory.path()).unwrap());
    context.configure_compute_lanes(LANES);
    let input = boxed_record_batch_stream(stream::iter(input_batches));
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
        512,
    )
    .map_ok(|batch| batch.into_public())
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

    assert_eq!(
        values,
        (0_i64..BATCHES * ROWS_PER_BATCH).collect::<Vec<_>>()
    );
    let peak = context.metrics.snapshot().peak_active_lanes;
    // The coordinator still creates one run task per input batch, but a busy
    // test runtime may execute these short tasks serially. Concurrency itself
    // is covered by the scheduler/pipeline barriers and the fixed-hardware
    // performance gate; this correctness test only requires the lane cap.
    assert!((1..=LANES as u64).contains(&peak), "unexpected peak {peak}");
    assert_eq!(context.metrics.snapshot().spill_files, 0);
    assert!(context.memory.peak() <= context.memory.limit());
    assert_eq!(context.memory.used(), 0);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .filter(|entry| entry.as_ref().is_ok_and(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "arrow")
            }))
            .count(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_spill_compacts_runs_while_input_is_still_arriving() {
    const BATCHES: usize = 40;
    const KEY_BYTES: usize = 1 << 20;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let input_batches = (0..BATCHES)
        .rev()
        .map(|value| {
            let key = format!("{value:04}{}", "x".repeat(KEY_BYTES));
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec![key]))],
            )
            .unwrap())
        })
        .collect::<Vec<_>>();
    let directory = tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(64 << 20), directory.path()).unwrap());
    context.configure_compute_lanes(4);
    let input = boxed_record_batch_stream(stream::iter(input_batches));
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Utf8, "value"),
        descending: false,
        nulls_first: false,
    };

    let batches = sort(
        input,
        vec![expression],
        None,
        schema,
        Arc::clone(&context),
        64,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let prefixes = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            values.iter().map(|value| value.unwrap()[..4].to_owned())
        })
        .collect::<Vec<_>>();

    assert_eq!(
        prefixes,
        (0..BATCHES)
            .map(|value| format!("{value:04}"))
            .collect::<Vec<_>>()
    );
    let metrics = context.metrics.snapshot();
    assert!(metrics.spill_files > MAX_PENDING_RUNS as u64);
    assert!(metrics.peak_active_spill_files <= (MAX_PENDING_RUNS * 2) as u64);
    assert_eq!(metrics.active_spill_files, 0);
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn spills_and_merges_top_k_with_bounded_memory() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let mut input_batches = Vec::new();
    for chunk in (0_i64..96).rev() {
        let values: Vec<_> = (0_i64..256)
            .rev()
            .map(|offset| chunk * 256 + offset)
            .collect();
        input_batches.push(Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()));
    }
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(128 * 1024), directory.path()).unwrap());
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
    assert_eq!(values, (24_526_i64..24_576).rev().collect::<Vec<_>>());
    let metrics = context.metrics.snapshot();
    assert!(metrics.spill_bytes > 0);
    assert!(metrics.spill_read_bytes > 0);
    assert!(metrics.spill_write_bytes > 0);
    assert!(metrics.spill_partitions > MERGE_FAN_IN as u64);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .filter(|entry| entry.as_ref().is_ok_and(|entry| entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "arrow")))
            .count(),
        0
    );
    assert!(context.memory.peak() <= context.memory.limit());
}

#[test]
fn merge_heap_copies_of_long_keys_are_memory_accounted() {
    const RUNS: usize = MERGE_FAN_IN;
    const KEY_BYTES: usize = 1 << 20;
    const MEMORY_LIMIT: usize = 20 << 20;
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(MEMORY_LIMIT), directory.path()).unwrap());
    let files = (0..RUNS)
        .map(|run| {
            let key = format!("{run:02}{}", "x".repeat(KEY_BYTES));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec![key]))],
            )
            .unwrap();
            context
                .spill
                .write_record_batches("long-merge-key", Arc::clone(&schema), [batch])
                .unwrap()
        })
        .collect::<Vec<_>>();
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Utf8, "key"),
        descending: false,
        nulls_first: false,
    };

    let error = match MergeIterator::new(
        &files,
        vec![expression],
        None,
        schema,
        Arc::clone(&context),
        context.memory.reservation(),
        1,
    ) {
        Ok(_) => panic!("merge unexpectedly fit unaccounted long heap keys"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::ResourceExhausted(_)), "{error:?}");
    assert!(context.memory.peak() <= MEMORY_LIMIT);
}

#[tokio::test]
async fn slices_a_single_input_batch_that_exceeds_the_sort_budget() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let values = (0_i64..8_192).rev().collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(128 * 1024), directory.path()).unwrap());
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
    assert_eq!(values, (0_i64..8_192).collect::<Vec<_>>());
    assert!(context.metrics.snapshot().spill_bytes > 0);
    assert!(context.memory.peak() <= context.memory.limit());
}

#[tokio::test]
async fn rejects_an_input_batch_whose_retained_buffers_exceed_the_budget() {
    const MEMORY_LIMIT: usize = 32 << 10;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0_i64..8_192))],
    )
    .unwrap();
    assert!(batch.get_array_memory_size() > MEMORY_LIMIT);
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(MEMORY_LIMIT), directory.path()).unwrap());
    let input = boxed_record_batch_stream(stream::iter([Ok(batch)]));
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Int64, "value"),
        descending: false,
        nulls_first: false,
    };

    let error = sort(
        input,
        vec![expression],
        None,
        schema,
        Arc::clone(&context),
        128,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap_err();
    assert!(
        matches!(
            &error,
            Error::ResourceExhausted(message)
                if message.contains("sort input batch")
                    && message.contains("query limit 32768")
        ),
        "unexpected error: {error:?}"
    );
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn spills_buffered_input_before_retrying_the_next_batch_reservation() {
    const MEMORY_LIMIT: usize = 128 << 10;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(7_200_i64..8_224))],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0_i64..7_200))],
    )
    .unwrap();
    assert!(second.get_array_memory_size() > 56 << 10);
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(MEMORY_LIMIT), directory.path()).unwrap());
    let input = boxed_record_batch_stream(stream::iter([Ok(first), Ok(second)]));
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Int64, "value"),
        descending: false,
        nulls_first: false,
    };

    let batches = sort(
        input,
        vec![expression],
        None,
        schema,
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
    assert_eq!(values, (0_i64..8_224).collect::<Vec<_>>());
    assert!(context.metrics.snapshot().spill_bytes > 0);
    assert!(context.memory.peak() <= MEMORY_LIMIT);
}

#[tokio::test]
async fn wide_metadata_schema_uses_dynamic_spill_headroom_under_low_memory() {
    const MEMORY_LIMIT: usize = 256 << 10;
    const COLUMNS: usize = 8;
    const ROWS: i64 = 1_024;
    let fields = (0..COLUMNS)
        .map(|column| Field::new(format!("column_{column}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("wide-metadata".into(), "m".repeat(32 << 10))]),
    ));
    let columns = (0..COLUMNS)
        .map(|column| {
            Arc::new(Int64Array::from_iter_values(
                (0_i64..ROWS).rev().map(|value| value + column as i64),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(MEMORY_LIMIT), directory.path()).unwrap());
    let spill_headroom = context
        .spill
        .writer_headroom_bytes("sort-merge", schema.as_ref());
    assert!(spill_headroom > 80 << 10);
    let input = boxed_record_batch_stream(stream::iter([Ok(batch)]));
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Int64, "column_0"),
        descending: false,
        nulls_first: false,
    };

    let batches = sort(
        input,
        vec![expression],
        None,
        schema,
        Arc::clone(&context),
        64,
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
    assert_eq!(values, (0_i64..ROWS).collect::<Vec<_>>());
    assert!(context.metrics.snapshot().spill_bytes > 0);
    assert!(context.memory.peak() <= MEMORY_LIMIT);
}

#[tokio::test]
async fn dropping_after_one_output_cleans_sort_runs_and_reservations() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values((0_i64..8_192).rev()))],
    )
    .unwrap();
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(128 << 10), directory.path()).unwrap());
    let input = boxed_record_batch_stream(stream::iter([Ok(batch)]));
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Int64, "value"),
        descending: false,
        nulls_first: false,
    };
    let mut output = sort(
        input,
        vec![expression],
        None,
        schema,
        Arc::clone(&context),
        128,
    );

    assert!(output.try_next().await.unwrap().is_some());
    drop(output);
    assert_eq!(context.memory.used(), 0);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .filter(|entry| entry.as_ref().is_ok_and(|entry| entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "arrow")))
            .count(),
        0
    );
}

#[tokio::test]
async fn compacts_run_metadata_during_a_long_spilling_input() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let input_batches = (0..160)
        .rev()
        .map(|value| {
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![value as i64; 256]))],
            )
            .unwrap())
        })
        .collect::<Vec<_>>();
    let directory = tempdir().unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(128 * 1024), directory.path()).unwrap());
    let input = boxed_record_batch_stream(stream::iter(input_batches));
    let expression = SortExpr {
        expr: BoundExpr::column(0, DataType::Int64, "value"),
        descending: false,
        nulls_first: false,
    };

    let batches = sort(
        input,
        vec![expression],
        Some(8),
        Arc::clone(&schema),
        Arc::clone(&context),
        16,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        8
    );
    assert!(context.metrics.snapshot().spill_partitions > MAX_PENDING_RUNS as u64);
    assert!(context.memory.peak() <= context.memory.limit());
}

#[test]
fn top_k_interleaves_selected_rows_from_multiple_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("priority", DataType::Int64, true),
        Field::new("tie", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(3), None, Some(3), Some(1)])),
            Arc::new(Int64Array::from(vec![Some(2), Some(0), None, Some(9)])),
            Arc::new(StringArray::from(vec!["3/2-a", "null/0", "3/null", "1/9"])),
        ],
    )
    .unwrap();
    let empty = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(4), Some(2), None, Some(3)])),
            Arc::new(Int64Array::from(vec![Some(1), Some(9), None, Some(1)])),
            Arc::new(StringArray::from(vec!["4/1", "2/9", "null/null", "3/1"])),
        ],
    )
    .unwrap();
    let expressions = vec![
        SortExpr {
            expr: BoundExpr::column(0, DataType::Int64, "priority"),
            descending: true,
            nulls_first: false,
        },
        SortExpr {
            expr: BoundExpr::column(1, DataType::Int64, "tie"),
            descending: false,
            nulls_first: true,
        },
    ];
    let converter = make_converter(&expressions).unwrap();
    let output = sort_batches(
        &[first, empty, second],
        &expressions,
        &converter,
        Some(5),
        &schema,
    )
    .unwrap();

    let priority = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let tie = output
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let payload = output
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        priority.iter().collect::<Vec<_>>(),
        vec![Some(4), Some(3), Some(3), Some(3), Some(2),]
    );
    assert_eq!(
        tie.iter().collect::<Vec<_>>(),
        vec![Some(1), None, Some(1), Some(2), Some(9),]
    );
    assert_eq!(
        payload
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect::<Vec<_>>(),
        vec![
            Some("4/1".to_owned()),
            Some("3/null".to_owned()),
            Some("3/1".to_owned()),
            Some("3/2-a".to_owned()),
            Some("2/9".to_owned()),
        ]
    );
}

#[test]
fn top_k_fetch_zero_returns_an_empty_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("priority", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2_i64, 1])),
            Arc::new(StringArray::from(vec!["two", "one"])),
        ],
    )
    .unwrap();
    let expressions = vec![SortExpr {
        expr: BoundExpr::column(0, DataType::Int64, "priority"),
        descending: false,
        nulls_first: false,
    }];
    let converter = make_converter(&expressions).unwrap();
    let output = sort_batches(&[batch], &expressions, &converter, Some(0), &schema).unwrap();
    assert_eq!(output.num_rows(), 0);
    assert_eq!(output.num_columns(), 2);
}

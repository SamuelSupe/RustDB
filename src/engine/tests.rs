use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::Schema,
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt, future::try_join_all};

use crate::{
    CsvHeader, CsvOptions, Engine, EngineConfig, Error, QueryResult, SpillConfig,
    sql::StatementPlan,
};

mod copy;
mod csv_lifecycle;
mod join_aggregate;
mod maintenance;
mod memory_snapshot;
mod native;
mod native_backup;
mod native_gc;
mod native_import;
mod native_integrity;
mod native_parquet;
mod native_quota;
mod native_schema;
mod native_types;

#[test]
fn rejects_zero_sized_batches() {
    let config = EngineConfig {
        batch_size: 0,
        ..EngineConfig::default()
    };
    assert!(Engine::new(config).is_err());
}

#[tokio::test]
async fn session_stream_preserves_producer_error_after_task_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let values = directory.path().join("values.csv");
    std::fs::write(&values, "id\n1\n2\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();

    let mut result = session.execute("SELECT 1 / 0").await.unwrap();
    let error = result.stream().next().await.unwrap().unwrap_err();
    assert!(
        matches!(&error, Error::Execution(message) if message.contains("division by zero")),
        "producer error was replaced by {error}"
    );
    assert!(result.stream().next().await.is_none());

    let sql = format!(
        "SELECT (SELECT id FROM read_csv('{}', header = true))",
        values.display()
    );
    let mut result = session.execute(&sql).await.unwrap();
    let error = result.stream().next().await.unwrap().unwrap_err();
    assert!(
        matches!(&error, Error::Execution(message) if message.contains("scalar subquery returned more than one row")),
        "producer error was replaced by {error}"
    );
    assert!(result.stream().next().await.is_none());
}

#[tokio::test]
async fn query_result_streams_keep_runtime_alive_after_session_drop() {
    let directory = tempfile::tempdir().unwrap();
    let mut result = {
        let engine = Engine::new(
            EngineConfig::builder()
                .compute_threads(1)
                .spill_directory(directory.path().join("spill"))
                .build(),
        )
        .unwrap();
        let session = engine.session();
        let result = session.execute("SELECT 1 AS value").await.unwrap();
        drop(session);
        drop(engine);
        result
    };
    let batches = tokio::time::timeout(
        Duration::from_secs(2),
        result.stream().try_collect::<Vec<_>>(),
    )
    .await
    .expect("QueryResult must outlive its creating Engine and Session")
    .unwrap();
    assert_single_value(&batches);
    drop(result);

    let stream = {
        let engine = Engine::new(
            EngineConfig::builder()
                .compute_threads(1)
                .spill_directory(directory.path().join("spill"))
                .build(),
        )
        .unwrap();
        let session = engine.session();
        let stream = session
            .execute("SELECT 1 AS value")
            .await
            .unwrap()
            .into_stream();
        drop(session);
        drop(engine);
        stream
    };

    let batches = tokio::time::timeout(Duration::from_secs(2), stream.try_collect::<Vec<_>>())
        .await
        .expect("stream must outlive its creating Engine and Session")
        .unwrap();
    assert_single_value(&batches);
}

fn assert_single_value(batches: &[RecordBatch]) {
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[tokio::test]
async fn file_table_function_binding_preserves_original_source_position() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("values.csv");
    std::fs::write(&path, "id\n1\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    let sql = format!(
        "SELECT id, count(*)\nFROM read_csv('{}', header = true)\nGROUP BY 3",
        path.display()
    );

    let error = match session.execute(&sql).await {
        Err(error) => error.to_string(),
        Ok(_) => panic!("invalid GROUP BY ordinal unexpectedly succeeded"),
    };
    assert_eq!(
        error,
        "invalid argument: GROUP BY position 3 is out of range (select list has 2 items) at line 3, column 10"
    );
    assert!(session.catalog().table_names().is_empty());

    let view_sql = format!(
        "CREATE TEMP VIEW invalid_view AS\nSELECT id, count(*)\nFROM read_csv('{}', header = true)\nGROUP BY 3",
        path.display()
    );
    let error = match session.execute(&view_sql).await {
        Err(error) => error.to_string(),
        Ok(_) => panic!("invalid CREATE VIEW ordinal unexpectedly succeeded"),
    };
    assert_eq!(
        error,
        "invalid argument: GROUP BY position 3 is out of range (select list has 2 items) at line 4, column 10"
    );
    assert!(session.catalog().table_names().is_empty());
}

#[tokio::test]
async fn creates_describes_queries_and_drops_temp_views() {
    let directory = tempfile::tempdir().unwrap();
    let config = EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    };
    let session = Engine::new(config).unwrap().session();

    collect(
        session
            .execute("CREATE TEMP VIEW answer AS SELECT 42 AS value")
            .await
            .unwrap(),
    )
    .await;
    let show = collect(session.execute("SHOW TABLES").await.unwrap()).await;
    assert_eq!(
        show[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "answer"
    );
    let describe = collect(session.execute("DESCRIBE answer").await.unwrap()).await;
    assert_eq!(
        describe[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "value"
    );
    let rows = collect(session.execute("SELECT value FROM answer").await.unwrap()).await;
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );

    collect(
        session
            .execute("CREATE OR REPLACE TEMP VIEW answer AS SELECT 7 AS value")
            .await
            .unwrap(),
    )
    .await;
    let rows = collect(session.execute("SELECT * FROM answer").await.unwrap()).await;
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );

    collect(session.execute("DROP VIEW answer").await.unwrap()).await;
    assert!(session.catalog().table("answer").is_none());
    assert!(session.execute("DROP VIEW answer").await.is_err());
    collect(session.execute("DROP VIEW IF EXISTS answer").await.unwrap()).await;
}

#[tokio::test]
async fn command_results_report_admission_and_parse_phases() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .max_concurrent_queries(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();

    // Holding the first QueryResult keeps its query permit alive and makes the
    // command's admission wait deterministic.
    let blocker = session.execute("SELECT 1").await.unwrap();
    let command_session = session.clone();
    let command = tokio::spawn(async move { command_session.execute("SHOW TABLES").await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(blocker);

    let result = command.await.unwrap().unwrap();
    let metrics = result.metrics();
    collect(result).await;
    let metrics = metrics.snapshot();
    assert!(metrics.query_admission_wait >= Duration::from_millis(10));
    assert!(metrics.sql_parse_time > Duration::ZERO);
    // `elapsed` starts after admission, but its duration is otherwise
    // independent of the admission wait. Under a loaded test runner the
    // command can be descheduled long enough for either duration to be larger.
    assert!(metrics.elapsed > Duration::ZERO);
    assert_eq!(metrics.table_function_prepare_time, Duration::ZERO);
    assert_eq!(metrics.bind_time, Duration::ZERO);
    assert_eq!(metrics.provider_prepare_time, Duration::ZERO);
    assert_eq!(metrics.optimize_time, Duration::ZERO);
}

#[tokio::test]
async fn explain_analyze_metrics_exclude_the_explanation_row() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    let mut result = session.execute("EXPLAIN ANALYZE SELECT 1").await.unwrap();
    let batches = result.stream().try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let metrics = result.metrics().snapshot();
    assert_eq!(metrics.rows_returned, 1);
    assert_eq!(metrics.batches_returned, 1);
}

#[tokio::test]
async fn view_keeps_file_provider_and_drop_view_preserves_external_table() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("values.csv");
    std::fs::write(&path, "id,label\n1,one\n2,two\n").unwrap();
    let config = EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    };
    let session = Engine::new(config).unwrap().session();
    session
        .register_csv(
            "external",
            [path.to_string_lossy().into_owned()],
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await
        .unwrap();
    let direct = format!(
        "SELECT id FROM read_csv('{}', header = true) LIMIT 1",
        path.display()
    );
    collect(session.execute(&direct).await.unwrap()).await;
    assert_eq!(session.catalog().table_names(), vec!["external".to_owned()]);

    let create = format!(
        "CREATE TEMP VIEW file_view AS SELECT id, label FROM read_csv('{}', header = true)",
        path.display()
    );
    collect(session.execute(&create).await.unwrap()).await;
    assert_eq!(
        session.catalog().table_names(),
        vec!["external".to_owned(), "file_view".to_owned()]
    );

    let rows = collect(
        session
            .execute("SELECT label FROM file_view LIMIT 1")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "one"
    );
    assert!(session.execute("DROP VIEW external").await.is_err());
    assert!(session.catalog().table("external").is_some());
}

#[tokio::test]
async fn view_reuses_the_file_set_fixed_during_query_preparation() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("parts");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(data.join("a.csv"), "id\n1\n").unwrap();
    let session = Engine::new(EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap()
    .session();
    let create = format!(
        "CREATE TEMP VIEW file_view AS SELECT id FROM read_csv('{}/*.csv', header = true)",
        data.display()
    );
    collect(session.execute(&create).await.unwrap()).await;

    let StatementPlan::Query(plan) = session
        .prepare_statement("SELECT count(*) FROM file_view")
        .await
        .unwrap()
    else {
        panic!("expected query plan");
    };
    let context = session.query_context().unwrap();
    crate::execution::prepare_plan(&plan, Arc::clone(&context))
        .await
        .unwrap();
    context.seal_object_snapshots();

    std::fs::write(data.join("b.csv"), "id\n2\n").unwrap();
    let batches = crate::execution::execute(StatementPlan::Query(plan), context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[tokio::test]
async fn view_rebinds_replaced_dependencies_and_rejects_cycles() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.csv");
    let second = directory.path().join("second.csv");
    std::fs::write(&first, "id\n1\n").unwrap();
    std::fs::write(&second, "id\n2\n").unwrap();
    let session = Engine::new(EngineConfig {
        compute_threads: 2,
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap()
    .session();
    let options = CsvOptions {
        header: CsvHeader::Present,
        ..CsvOptions::default()
    };
    session
        .register_csv(
            "base",
            [first.to_string_lossy().into_owned()],
            options.clone(),
        )
        .await
        .unwrap();
    collect(
        session
            .execute("CREATE TEMP VIEW current_base AS SELECT id FROM base")
            .await
            .unwrap(),
    )
    .await;
    session
        .register_csv("base", [second.to_string_lossy().into_owned()], options)
        .await
        .unwrap();
    let rows = collect(
        session
            .execute("SELECT id FROM current_base")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    collect(
        session
            .execute("CREATE TEMP VIEW a AS SELECT 1 AS value")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("CREATE TEMP VIEW b AS SELECT value FROM a")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("CREATE OR REPLACE TEMP VIEW a AS SELECT value FROM b")
            .await
            .unwrap(),
    )
    .await;
    let error = tokio::time::timeout(Duration::from_secs(3), async {
        match session.execute("SELECT value FROM a").await {
            Ok(result) => result
                .into_stream()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err(),
            Err(error) => error,
        }
    })
    .await
    .expect("cyclic views must fail without hanging");
    assert!(error.to_string().contains("view cycle"));
}

#[tokio::test]
async fn concurrent_file_queries_clean_only_their_generated_tables() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("values.csv");
    std::fs::write(&path, "id\n1\n2\n3\n").unwrap();
    let session = Engine::new(EngineConfig {
        max_concurrent_queries: 8,
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap()
    .session();
    let sql = format!(
        "SELECT count(*) FROM read_csv('{}', header = true)",
        path.display()
    );

    let queries = (0..32).map(|_| {
        let session = session.clone();
        let sql = sql.clone();
        async move {
            let result = session.execute(&sql).await?;
            let batches = result.into_stream().try_collect::<Vec<_>>().await?;
            let values = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            if values.value(0) != 3 {
                return Err(Error::Execution("unexpected concurrent count".into()));
            }
            Ok(())
        }
    });
    try_join_all(queries).await.unwrap();

    assert!(
        session
            .catalog()
            .table_names()
            .iter()
            .all(|name| !name.starts_with("__rustdb_file_"))
    );
}

#[tokio::test]
async fn registered_csv_discovers_files_per_query_and_freezes_each_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("parts");
    std::fs::create_dir(&data).unwrap();
    std::fs::write(data.join("a.csv"), "id\n1\n").unwrap();
    let session = Engine::new(EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap()
    .session();
    session
        .register_csv(
            "dynamic_csv",
            [format!("{}/*.csv", data.display())],
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(query_count(&session, "dynamic_csv").await, 1);
    std::fs::write(data.join("b.csv"), "id\n2\n").unwrap();
    assert_eq!(query_count(&session, "dynamic_csv").await, 2);

    let StatementPlan::Query(plan) = session
        .prepare_statement("SELECT count(*) FROM dynamic_csv")
        .await
        .unwrap()
    else {
        panic!("expected query plan");
    };
    let context = session.query_context().unwrap();
    crate::execution::prepare_plan(&plan, Arc::clone(&context))
        .await
        .unwrap();
    context.seal_object_snapshots();
    std::fs::write(data.join("c.csv"), "id\n3\n").unwrap();
    let fixed = crate::execution::execute(StatementPlan::Query(plan), context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(first_i64(&fixed), 2);

    assert_eq!(query_count(&session, "dynamic_csv").await, 3);
    std::fs::remove_file(data.join("a.csv")).unwrap();
    assert_eq!(query_count(&session, "dynamic_csv").await, 2);
}

#[tokio::test]
async fn dynamic_statistics_are_query_scoped_and_drive_join_planning() {
    let directory = tempfile::tempdir().unwrap();
    let left = directory.path().join("left");
    let right = directory.path().join("right");
    std::fs::create_dir_all(&left).unwrap();
    std::fs::create_dir_all(&right).unwrap();
    let left_rows = (0..100)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(left.join("a.csv"), format!("id\n{left_rows}\n")).unwrap();
    std::fs::write(right.join("a.csv"), "id\n1\n").unwrap();

    let session = Engine::new(EngineConfig {
        compute_threads: 3,
        max_concurrent_queries: 4,
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap()
    .session();
    let options = CsvOptions {
        header: CsvHeader::Present,
        ..CsvOptions::default()
    };
    session
        .register_csv(
            "left_rows",
            [format!("{}/*.csv", left.display())],
            options.clone(),
        )
        .await
        .unwrap();
    session
        .register_csv(
            "right_rows",
            [format!("{}/*.csv", right.display())],
            options,
        )
        .await
        .unwrap();

    let added_rows = (0..2_000)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(right.join("b.csv"), format!("id\n{added_rows}\n")).unwrap();
    let sql = "EXPLAIN SELECT l.id FROM left_rows l \
               JOIN right_rows r ON l.id = r.id";
    let first = session.execute(sql).await.unwrap();

    // Keep the first QueryResult alive while a second query fixes a newer
    // file snapshot. Neither snapshot may be published to the Catalog.
    std::fs::write(right.join("c.csv"), "id\n2001\n").unwrap();
    let second = session.execute(sql).await.unwrap();
    let first = explain_value(collect(first).await);
    let second = explain_value(collect(second).await);

    let first_right = scan_line(&first, "right_rows");
    let second_right = scan_line(&second, "right_rows");
    assert!(first_right.contains("files=2"), "{first_right}");
    assert!(second_right.contains("files=3"), "{second_right}");
    assert!(first.contains("lane_limit=3"), "{first}");
    assert!(first.contains("partitions=adaptive(2..256)"), "{first}");
    assert!(first.contains("repartition=bounded"), "{first}");
    assert!(first.contains("fallback=sort_merge"), "{first}");

    // The newly discovered right side is now larger, so the optimizer
    // swaps the inner join and keeps the smaller left relation as build.
    assert!(
        first.find("Scan table=right_rows").unwrap() < first.find("Scan table=left_rows").unwrap(),
        "{first}"
    );
    assert_eq!(
        session
            .catalog()
            .table("right_rows")
            .unwrap()
            .provider()
            .statistics()
            .file_count,
        1
    );
}

#[tokio::test]
async fn refresh_table_atomically_replaces_the_registered_csv_schema() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("parts");
    std::fs::create_dir(&data).unwrap();
    let first = data.join("a.csv");
    std::fs::write(&first, "id\n1\n").unwrap();
    let session = Engine::new(EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap()
    .session();
    session
        .register_csv(
            "dynamic_csv",
            [format!("{}/*.csv", data.display())],
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await
        .unwrap();

    std::fs::remove_file(first).unwrap();
    std::fs::write(data.join("b.csv"), "id,label\n2,two\n").unwrap();
    assert!(session.execute("SELECT * FROM dynamic_csv").await.is_err());

    let schema = session.refresh_table("dynamic_csv").await.unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(1).name(), "label");
    let rows = collect(
        session
            .execute("SELECT label FROM dynamic_csv")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "two"
    );

    std::fs::write(data.join("b.csv"), "id,label,extra\n2,two,x\n").unwrap();
    collect(session.execute("REFRESH TABLE dynamic_csv").await.unwrap()).await;
    let described = collect(session.execute("DESCRIBE dynamic_csv").await.unwrap()).await;
    assert_eq!(described[0].num_rows(), 3);
}

#[tokio::test]
async fn refresh_table_canonicalizes_the_default_schema_only() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("parts");
    std::fs::create_dir(&data).unwrap();
    let first = data.join("a.csv");
    std::fs::write(&first, "id\n1\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();

    session
        .register_csv(
            "main.events",
            [format!("{}/*.csv", data.display())],
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await
        .unwrap();

    std::fs::remove_file(first).unwrap();
    std::fs::write(data.join("b.csv"), "id,label\n2,two\n").unwrap();
    let schema = session.refresh_table("main.events").await.unwrap();
    assert_eq!(schema.fields().len(), 2);

    let error = session.refresh_table("analytics.events").await.unwrap_err();
    assert!(
        matches!(&error, Error::Catalog(message) if message.contains("analytics.events")),
        "qualified refresh fell back to the default schema: {error}"
    );
}

#[tokio::test]
async fn stream_error_cleans_spill_before_query_result_is_dropped() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap();
    let session = engine.session();
    let context = session.query_context().unwrap();
    let schema = Arc::new(Schema::empty());
    context
        .spill
        .write_record_batches(
            "before-error",
            Arc::clone(&schema),
            [RecordBatch::new_empty(Arc::clone(&schema))],
        )
        .unwrap();
    let query_directory = context.spill.directory().to_owned();
    assert!(query_directory.is_dir());

    let permit = Arc::clone(&engine.inner.admission)
        .acquire_owned()
        .await
        .unwrap();
    let input = crate::runtime::boxed_record_batch_stream(futures::stream::once(async {
        Err(Error::Execution("injected stream failure".into()))
    }));
    let mut result = super::query_result(schema, input, context, permit, engine.clone());
    assert!(matches!(
        result.stream().next().await,
        Some(Err(Error::Execution(message))) if message == "injected stream failure"
    ));

    // `result` intentionally remains alive for this assertion.
    assert!(!query_directory.exists());
}

#[tokio::test]
async fn stream_prefers_first_task_failure_to_cancelled_sibling() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let context = engine.session().query_context().unwrap();
    context.record_task_failure(&Error::ResourceExhausted(
        "injected operator resource failure".to_owned(),
    ));
    let permit = Arc::clone(&engine.inner.admission)
        .acquire_owned()
        .await
        .unwrap();
    let schema = Arc::new(Schema::empty());
    let input = crate::runtime::boxed_record_batch_stream(futures::stream::once(async {
        Err(Error::Cancelled)
    }));
    let mut result = super::query_result(schema, input, context, permit, engine.clone());

    let error = result.stream().next().await.unwrap().unwrap_err();
    assert!(
        matches!(error, Error::ResourceExhausted(message) if message == "injected operator resource failure")
    );
    assert!(result.stream().next().await.is_none());
}

#[tokio::test]
async fn successful_stream_returns_terminal_cleanup_failure_once() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap();
    let context = engine.session().query_context().unwrap();
    let cleanup_attempts = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&cleanup_attempts);
    context.set_spill_cleanup_hook(move || {
        observed.fetch_add(1, Ordering::Relaxed);
        Err(Error::ResourceExhausted(
            "injected successful-query cleanup failure".to_owned(),
        ))
    });
    let permit = Arc::clone(&engine.inner.admission)
        .acquire_owned()
        .await
        .unwrap();
    let schema = Arc::new(Schema::empty());
    let input = crate::runtime::boxed_record_batch_stream(futures::stream::empty());
    let mut result = super::query_result(schema, input, context, permit, engine.clone());

    let error = result.stream().next().await.unwrap().unwrap_err();
    assert!(
        matches!(error, Error::ResourceExhausted(message) if message == "injected successful-query cleanup failure")
    );
    assert!(result.stream().next().await.is_none());
    drop(result);
    assert_eq!(cleanup_attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn stream_error_preserves_execution_and_cleanup_failures() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap();
    let context = engine.session().query_context().unwrap();
    context.set_spill_cleanup_hook(|| {
        Err(Error::ResourceExhausted(
            "injected error-path cleanup failure".to_owned(),
        ))
    });
    let permit = Arc::clone(&engine.inner.admission)
        .acquire_owned()
        .await
        .unwrap();
    let schema = Arc::new(Schema::empty());
    let input = crate::runtime::boxed_record_batch_stream(futures::stream::once(async {
        Err(Error::Execution("injected operator failure".to_owned()))
    }));
    let mut result = super::query_result(schema, input, context, permit, engine.clone());

    let error = result.stream().next().await.unwrap().unwrap_err();
    let message = error.to_string();
    assert!(message.contains("injected operator failure"));
    assert!(message.contains("injected error-path cleanup failure"));
}

#[tokio::test]
async fn cancelled_stream_preserves_cleanup_failure() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(EngineConfig {
        spill: SpillConfig {
            directory: directory.path().join("spill"),
            ..SpillConfig::default()
        },
        ..EngineConfig::default()
    })
    .unwrap();
    let context = engine.session().query_context().unwrap();
    context.set_spill_cleanup_hook(|| {
        Err(Error::ResourceExhausted(
            "injected cancellation cleanup failure".to_owned(),
        ))
    });
    let permit = Arc::clone(&engine.inner.admission)
        .acquire_owned()
        .await
        .unwrap();
    let schema = Arc::new(Schema::empty());
    let input_schema = Arc::clone(&schema);
    let input = crate::runtime::boxed_record_batch_stream(futures::stream::once(async move {
        Ok(RecordBatch::new_empty(input_schema))
    }));
    let mut result = super::query_result(schema, input, context, permit, engine.clone());
    result.cancel();

    let error = result.stream().next().await.unwrap().unwrap_err();
    let message = error.to_string();
    assert!(message.contains("query cancelled"));
    assert!(message.contains("injected cancellation cleanup failure"));
}

async fn collect(result: QueryResult) -> Vec<arrow::record_batch::RecordBatch> {
    result.into_stream().try_collect::<Vec<_>>().await.unwrap()
}

fn explain_value(batches: Vec<RecordBatch>) -> String {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_owned()
}

fn scan_line<'a>(explain: &'a str, table: &str) -> &'a str {
    explain
        .lines()
        .find(|line| line.contains(&format!("Scan table={table} ")))
        .unwrap_or_else(|| panic!("missing scan for {table}: {explain}"))
}

async fn query_count(session: &crate::Session, table: &str) -> i64 {
    let batches = collect(
        session
            .execute(&format!("SELECT count(*) FROM {table}"))
            .await
            .unwrap(),
    )
    .await;
    first_i64(&batches)
}

fn first_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

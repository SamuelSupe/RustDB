use std::{collections::HashMap, fs, path::PathBuf};

use arrow::array::{Array, Int64Array, StringArray};
use futures::StreamExt;
use rustdb::{CsvHeader, CsvOptions, Engine, EngineConfig, QueryResult, Result};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

async fn collect(mut result: QueryResult) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = result.stream().next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

async fn session_with_employees() -> Result<rustdb::Session> {
    let engine = Engine::new(EngineConfig::default())?;
    let session = engine.session();
    session
        .register_csv(
            "employees",
            [fixture("employees.csv").to_string_lossy().into_owned()],
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await?;
    Ok(session)
}

async fn session_with_company() -> Result<rustdb::Session> {
    let session = session_with_employees().await?;
    session
        .register_csv(
            "departments",
            [fixture("departments.csv").to_string_lossy().into_owned()],
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn filters_and_projects_csv_batches() -> Result<()> {
    let session = session_with_employees().await?;
    let batches = collect(
        session
            .execute("SELECT name, salary FROM employees WHERE salary >= 120000")
            .await?,
    )
    .await?;

    let names = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(names, ["Ada", "Grace", "Barbara"]);
    Ok(())
}

#[tokio::test]
async fn groups_and_aggregates_csv() -> Result<()> {
    let session = session_with_employees().await?;
    let batches = collect(
        session
            .execute(
                "SELECT department, count(*) AS people, sum(salary) AS payroll \
                 FROM employees GROUP BY department",
            )
            .await?,
    )
    .await?;

    let mut values = HashMap::new();
    for batch in batches {
        let department = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let people = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payroll = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.insert(
                department.value(row).to_owned(),
                (people.value(row), payroll.value(row)),
            );
        }
    }

    assert_eq!(values["engineering"], (3, 355_000));
    assert_eq!(values["research"], (2, 245_000));
    assert_eq!(values["operations"], (1, 105_000));
    Ok(())
}

#[tokio::test]
async fn executes_scalar_query_without_a_table() -> Result<()> {
    let session = Engine::new(EngineConfig::default())?.session();
    let batches = collect(session.execute("SELECT 40 + 2 AS answer").await?).await?;
    let answer = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(answer.value(0), 42);
    Ok(())
}

#[tokio::test]
async fn executes_inner_and_left_hash_joins() -> Result<()> {
    let session = session_with_company().await?;
    let inner = collect(
        session
            .execute(
                "SELECT e.name, d.budget FROM employees e \
                 INNER JOIN departments d ON e.department = d.department",
            )
            .await?,
    )
    .await?;
    assert_eq!(inner.iter().map(|batch| batch.num_rows()).sum::<usize>(), 6);

    let left = collect(
        session
            .execute(
                "SELECT d.department, e.name FROM departments d \
                 LEFT JOIN employees e ON d.department = e.department",
            )
            .await?,
    )
    .await?;
    let mut saw_unmatched_sales = false;
    for batch in left {
        let department = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let employee = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if department.value(row) == "sales" && employee.is_null(row) {
                saw_unmatched_sales = true;
            }
        }
    }
    assert!(saw_unmatched_sales);
    Ok(())
}

#[tokio::test]
async fn queries_csv_through_file_table_function() -> Result<()> {
    let session = Engine::new(EngineConfig::default())?.session();
    let sql = format!(
        "SELECT count(*) AS people FROM read_csv('{}', header = true) WHERE active = true",
        fixture("employees.csv").display(),
    );
    let batches = collect(session.execute(&sql).await?).await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 5);
    Ok(())
}

#[tokio::test]
async fn join_streams_high_fanout_in_configured_batches() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let left = directory.path().join("left.csv");
    let right = directory.path().join("right.csv");
    fs::write(&left, "key\n1\n").unwrap();
    let right_rows = (0..100).map(|_| "1").collect::<Vec<_>>().join("\n");
    fs::write(&right, format!("key\n{right_rows}\n")).unwrap();

    let session = Engine::new(EngineConfig {
        batch_size: 7,
        temp_dir: directory.path().join("spill"),
        ..EngineConfig::default()
    })?
    .session();
    let options = CsvOptions {
        header: CsvHeader::Present,
        ..CsvOptions::default()
    };
    session
        .register_csv("left_rows", [left.to_string_lossy()], options.clone())
        .await?;
    session
        .register_csv("right_rows", [right.to_string_lossy()], options)
        .await?;

    let mut result = session
        .execute("SELECT l.key FROM left_rows l JOIN right_rows r ON l.key = r.key")
        .await?;
    let mut rows = 0;
    let mut batches = 0;
    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        assert!(batch.num_rows() <= 7);
        rows += batch.num_rows();
        batches += 1;
    }
    assert_eq!(rows, 100);
    assert!(batches > 1);
    Ok(())
}

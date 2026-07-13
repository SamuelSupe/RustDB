use std::{collections::HashMap, fs, path::PathBuf};

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::DataType,
};
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
            CsvOptions::builder().header(CsvHeader::Present).build(),
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
            CsvOptions::builder().header(CsvHeader::Present).build(),
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
async fn csv_refresh_preserves_old_column_order_and_maps_the_new_header() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dynamic.csv");
    fs::write(&path, "b,a\n1,old\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )?
    .session();
    session
        .register_csv(
            "dynamic",
            [path.to_string_lossy()],
            CsvOptions::builder().header(CsvHeader::Present).build(),
        )
        .await?;

    fs::write(&path, "a,z,c,b\nnew,last,middle,2\n").unwrap();
    let schema = session.refresh_table("dynamic").await?;
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["b", "a", "c", "z"]);

    let batches = collect(session.execute("SELECT * FROM dynamic").await?).await?;
    assert_eq!(batches[0].schema(), schema);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    for (column, expected) in [(1, "new"), (2, "middle"), (3, "last")] {
        assert_eq!(
            batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn csv_refresh_reports_incompatible_file_uri_and_column() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("parts");
    fs::create_dir(&data).unwrap();
    fs::write(data.join("a.csv"), "id,label\n1,one\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )?
    .session();
    session
        .register_csv(
            "dynamic",
            [format!("{}/*.csv", data.display())],
            CsvOptions::builder().header(CsvHeader::Present).build(),
        )
        .await?;

    fs::write(data.join("b.csv"), "id,label\nnot-an-integer,two\n").unwrap();
    let error = session.refresh_table("dynamic").await.unwrap_err();
    let message = error.to_string();
    assert!(message.contains("b.csv"), "{message}");
    assert!(message.contains("column id"), "{message}");
    assert!(message.contains("Int64"), "{message}");
    Ok(())
}

#[tokio::test]
async fn inferred_csv_schema_rejects_dynamic_type_drift_until_refresh() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("parts");
    fs::create_dir(&data).unwrap();
    let first = data.join("a.csv");
    let second = data.join("b.csv");
    fs::write(&first, "value\n1.5\n2.5\n").unwrap();

    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )?
    .session();
    session
        .register_csv(
            "dynamic_types",
            [format!("{}/*.csv", data.display())],
            CsvOptions::default(),
        )
        .await?;

    fs::write(&second, "value\n9007199254740993\n").unwrap();
    let error = match session.execute("SELECT value FROM dynamic_types").await {
        Err(error) => error,
        Ok(mut result) => result
            .stream()
            .next()
            .await
            .expect("schema drift must produce a terminal result")
            .expect_err("dynamic Int64 file must not use the registered Float64 schema"),
    };
    let message = error.to_string();
    assert!(message.contains("b.csv"), "{message}");
    assert!(message.contains("column value"), "{message}");
    assert!(message.contains("Float64"), "{message}");
    assert!(message.contains("Int64"), "{message}");

    let refresh_error = session.refresh_table("dynamic_types").await.unwrap_err();
    assert!(refresh_error.to_string().contains("b.csv"));

    // A failed refresh leaves the old provider visible atomically.
    fs::remove_file(&second).unwrap();
    let old = session
        .execute("SELECT value FROM dynamic_types ORDER BY value")
        .await?;
    assert_eq!(old.schema().field(0).data_type(), &DataType::Float64);
    assert_eq!(
        collect(old)
            .await?
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        2
    );

    fs::write(&first, "value\n9007199254740993\n").unwrap();
    fs::write(&second, "value\n9007199254740995\n").unwrap();
    let refreshed = session.refresh_table("dynamic_types").await?;
    assert_eq!(refreshed.field(0).data_type(), &DataType::Int64);

    let batches = collect(
        session
            .execute("SELECT value FROM dynamic_types ORDER BY value")
            .await?,
    )
    .await?;
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
    assert_eq!(values, [9_007_199_254_740_993, 9_007_199_254_740_995]);
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

    let session = Engine::new(
        EngineConfig::builder()
            .batch_size(7)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )?
    .session();
    let options = CsvOptions::builder().header(CsvHeader::Present).build();
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

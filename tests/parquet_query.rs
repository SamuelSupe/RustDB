use std::{fs::File, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
        StringDictionaryBuilder,
    },
    datatypes::{DataType, Field, Int8Type, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use rustdb::{Engine, EngineConfig, ParquetOptions, ParquetSchemaMode, Result};

mod support;

use support::parquet_evolution;

fn write_fixture(path: &std::path::Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let file = File::create(path).map_err(|error| rustdb::Error::io(Some(path.into()), error))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a", "b"])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0])),
        ],
    )?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_q6_fixture(path: &std::path::Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_discount", DataType::Decimal128(15, 2), false),
        Field::new("l_quantity", DataType::Int64, false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
    ]));
    let discount = Decimal128Array::from(vec![5_i128, 5, 5]).with_precision_and_scale(15, 2)?;
    let price =
        Decimal128Array::from(vec![10_000_i128, 28_000, 30_000]).with_precision_and_scale(15, 2)?;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(vec![8_766, 8_766, 9_131])),
            Arc::new(discount),
            Arc::new(Int64Array::from(vec![10, 10, 10])),
            Arc::new(price),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

#[tokio::test]
async fn q6_exact_filter_drops_filter_only_scan_columns() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("q6-exact.parquet");
    write_q6_fixture(&path)?;
    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "lineitem_q6_exact",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    let query = "SELECT sum(l_extendedprice * l_discount) AS revenue \
                 FROM lineitem_q6_exact \
                 WHERE l_shipdate >= DATE '1994-01-01' \
                   AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR \
                   AND l_discount BETWEEN 0.04 AND 0.06 \
                   AND l_quantity < 24";
    let mut explain = session.execute(&format!("EXPLAIN {query}")).await?;
    let batch = explain.stream().next().await.unwrap()?;
    let text = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_owned();
    drop(explain);
    assert!(
        !text
            .lines()
            .any(|line| line.trim_start().starts_with("Filter ")),
        "{text}"
    );
    assert!(
        text.contains("projection=Some([1, 3]) filter=exact"),
        "{text}"
    );

    let mut result = session.execute(query).await?;
    let batch = result.stream().next().await.unwrap()?;
    let revenue = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(revenue.data_type(), &DataType::Decimal128(38, 4));
    assert_eq!(revenue.value(0), 190_000);
    drop(result);

    let count = "SELECT count(*) FROM lineitem_q6_exact WHERE l_quantity < 24";
    let mut explain = session.execute(&format!("EXPLAIN {count}")).await?;
    let batch = explain.stream().next().await.unwrap()?;
    let text = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_owned();
    drop(explain);
    assert!(text.contains("projection=Some([]) filter=exact"), "{text}");
    let mut result = session.execute(count).await?;
    let batch = result.stream().next().await.unwrap()?;
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 3);
    Ok(())
}

#[tokio::test]
async fn queries_parquet_with_projection_pruning_and_sort() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("metrics.parquet");
    write_fixture(&path)?;

    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "metrics",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    let mut result = session
        .execute(
            "SELECT category, count(*) AS n, sum(value) AS total \
             FROM metrics WHERE id > 3 GROUP BY category ORDER BY category",
        )
        .await?;
    let metrics = result.metrics();
    let mut batches = Vec::new();
    while let Some(batch) = result.stream().next().await {
        batches.push(batch?);
    }

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    let category = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(category.value(0), "a");
    assert_eq!(category.value(1), "b");
    assert!(metrics.snapshot().row_groups_pruned >= 1);
    Ok(())
}

#[tokio::test]
async fn hash_join_runtime_filter_reaches_parquet_row_group_pruning() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime-filter.parquet");
    write_fixture(&path)?;

    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "runtime_filter_fact",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    let mut result = session
        .execute(
            "SELECT f.id FROM runtime_filter_fact f \
             JOIN (SELECT 1 AS id) d ON f.id = d.id",
        )
        .await?;
    let metrics = result.metrics();
    let mut rows = 0usize;
    while let Some(batch) = result.stream().next().await {
        rows = rows.saturating_add(batch?.num_rows());
    }
    assert_eq!(rows, 1);
    let metrics = metrics.snapshot();
    assert!(metrics.runtime_filter_hits >= 1, "{metrics:?}");
    assert!(metrics.row_groups_pruned >= 1, "{metrics:?}");

    let mut empty = session
        .execute(
            "SELECT f.id FROM runtime_filter_fact f \
             JOIN (SELECT 1 AS id WHERE FALSE) d ON f.id = d.id",
        )
        .await?;
    let empty_metrics = empty.metrics();
    let mut empty_rows = 0usize;
    while let Some(batch) = empty.stream().next().await {
        empty_rows = empty_rows.saturating_add(batch?.num_rows());
    }
    assert_eq!(empty_rows, 0);
    let empty_metrics = empty_metrics.snapshot();
    assert!(empty_metrics.runtime_filter_hits >= 1, "{empty_metrics:?}");
    assert!(empty_metrics.row_groups_pruned >= 2, "{empty_metrics:?}");
    Ok(())
}

#[tokio::test]
async fn queries_parquet_file_function() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("metrics.parquet");
    write_fixture(&path)?;
    let session = Engine::new(EngineConfig::default())?.session();
    let sql = format!("SELECT count(*) FROM read_parquet('{}')", path.display());
    let mut result = session.execute(&sql).await?;
    let metrics = result.metrics();
    let batch = result.stream().next().await.unwrap()?;
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 6);
    drop(result);
    let metrics = metrics.snapshot();
    assert_eq!(metrics.rows_scanned, 6);
    assert_eq!(metrics.bytes_scanned, 0);

    let explain_sql = format!(
        "EXPLAIN SELECT count(*) FROM read_parquet('{}')",
        path.display()
    );
    let mut explain = session.execute(&explain_sql).await?;
    let batch = explain.stream().next().await.unwrap()?;
    let text = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert!(text.contains("projection=Some([])"), "{text}");
    Ok(())
}

#[tokio::test]
async fn registered_table_accepts_file_updates_between_queries() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("changing.parquet");
    write_id_fixture(&path, &[1])?;
    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "changing",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;

    assert_eq!(
        query_count(&session, "SELECT count(*) FROM changing").await?,
        1
    );
    write_id_fixture(&path, &[1, 2, 3])?;
    assert_eq!(
        query_count(&session, "SELECT count(*) FROM changing").await?,
        3
    );
    Ok(())
}

#[tokio::test]
async fn registered_table_rejects_an_incompatible_schema_change() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("schema-change.parquet");
    write_fixture(&path)?;
    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "changing_schema",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    write_id_fixture(&path, &[1, 2, 3])?;

    let error = match session.execute("SELECT value FROM changing_schema").await {
        Err(error) => error,
        Ok(mut result) => result
            .stream()
            .next()
            .await
            .expect("scan should report a schema error")
            .unwrap_err(),
    };
    assert!(error.to_string().contains("schema"));
    assert!(error.to_string().contains("schema-change.parquet"));
    Ok(())
}

#[tokio::test]
async fn registered_parquet_discovers_files_and_refreshes_schema() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("parts");
    std::fs::create_dir(&data).unwrap();
    let first = data.join("a.parquet");
    write_id_fixture(&first, &[1])?;
    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "dynamic_parquet",
            [format!("{}/*.parquet", data.display())],
            ParquetOptions::default(),
        )
        .await?;

    let second = data.join("b.parquet");
    write_id_extra_fixture(&second, &[2, 3], &["two", "three"])?;
    assert_eq!(
        query_count(&session, "SELECT count(*) FROM dynamic_parquet").await?,
        3
    );

    std::fs::remove_file(first).unwrap();
    assert_eq!(
        query_count(&session, "SELECT count(*) FROM dynamic_parquet").await?,
        2
    );
    let schema = session.refresh_table("dynamic_parquet").await?;
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(1).name(), "extra");

    let mut result = session
        .execute("SELECT extra FROM dynamic_parquet ORDER BY extra")
        .await?;
    let batch = result.stream().next().await.unwrap()?;
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "three");
    assert_eq!(values.value(1), "two");
    Ok(())
}

#[tokio::test]
async fn safe_widening_reads_real_files_and_refreshes_missing_and_new_columns() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("evolution");
    std::fs::create_dir(&data).unwrap();
    let first = data.join("a.parquet");
    let second = data.join("b.parquet");
    parquet_evolution::write_i32(&first, &[1, 2], "label", &["one", "two"])?;
    parquet_evolution::write_i64(&second, &[3, 4], "label", &["three", "four"])?;

    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "widening",
            [format!("{}/*.parquet", data.display())],
            ParquetOptions {
                schema_mode: ParquetSchemaMode::SafeWidening,
                ..ParquetOptions::default()
            },
        )
        .await?;
    let mut widened = session
        .execute("SELECT id FROM widening ORDER BY id")
        .await?;
    let mut widened_ids = Vec::new();
    while let Some(batch) = widened.stream().next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("SafeWidening must expose Int64");
        widened_ids.extend(ids.values().iter().copied());
    }
    assert_eq!(widened_ids, [1, 2, 3, 4]);

    std::fs::remove_file(first).unwrap();
    std::fs::remove_file(second).unwrap();
    parquet_evolution::write_i64(&data.join("c.parquet"), &[10], "new_value", &["ten"])?;
    parquet_evolution::write_i64(&data.join("d.parquet"), &[20], "new_value", &["twenty"])?;

    let error = match session.execute("SELECT count(*) FROM widening").await {
        Err(error) => error,
        Ok(_) => panic!("a missing registered column must require REFRESH TABLE"),
    };
    assert!(error.to_string().contains("label"), "{error}");

    let schema = session.refresh_table("widening").await?;
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).name(), "new_value");

    let mut result = session
        .execute("SELECT id, new_value FROM widening ORDER BY id")
        .await?;
    let mut rows = Vec::new();
    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        rows.extend(
            (0..batch.num_rows()).map(|row| (ids.value(row), values.value(row).to_owned())),
        );
    }
    assert_eq!(rows, [(10, "ten".to_owned()), (20, "twenty".to_owned())]);
    Ok(())
}

#[tokio::test]
async fn dictionary_encoded_files_decode_in_every_schema_mode() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("dictionary");
    std::fs::create_dir(&data).unwrap();
    let dictionary_path = data.join("a.parquet");
    write_dictionary_fixture(&dictionary_path, &["alpha", "beta"])?;
    write_string_fixture(&data.join("b.parquet"), &["gamma"])?;
    let dictionary_reader = ParquetRecordBatchReaderBuilder::try_new(
        File::open(&dictionary_path)
            .map_err(|error| rustdb::Error::io(Some(dictionary_path.clone()), error))?,
    )?;
    assert!(matches!(
        dictionary_reader.schema().field(0).data_type(),
        DataType::Dictionary(_, value) if value.as_ref() == &DataType::Utf8
    ));

    let session = Engine::new(EngineConfig::default())?.session();
    let pattern = format!("{}/*.parquet", data.display());
    for (table, mode) in [
        ("dictionary_strict", ParquetSchemaMode::Strict),
        ("dictionary_union", ParquetSchemaMode::UnionByName),
        ("dictionary_widening", ParquetSchemaMode::SafeWidening),
    ] {
        session
            .register_parquet(
                table,
                [pattern.clone()],
                ParquetOptions {
                    schema_mode: mode,
                    ..ParquetOptions::default()
                },
            )
            .await?;
        let mut result = session
            .execute(&format!("SELECT name FROM {table} ORDER BY name"))
            .await?;
        assert_eq!(result.schema().field(0).data_type(), &DataType::Utf8);
        let mut values = Vec::new();
        while let Some(batch) = result.stream().next().await {
            let batch = batch?;
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Dictionary values must be decoded to Utf8");
            values.extend(names.iter().map(|value| value.unwrap().to_owned()));
        }
        assert_eq!(values, ["alpha", "beta", "gamma"]);
    }
    Ok(())
}

#[tokio::test]
async fn reads_all_row_group_morsels_with_bounded_concurrency() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("morsels.parquet");
    // Four physical row groups should expose exactly four schedulable scan
    // lanes when the engine is configured with four compute threads.
    let values = (0_i64..16).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let file = File::create(&path).map_err(|error| rustdb::Error::io(Some(path.clone()), error))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;
    writer.write(&RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(values.clone()))],
    )?)?;
    writer.close()?;

    let config = EngineConfig::builder()
        .memory_limit(128 << 20)
        .compute_threads(4)
        .io_concurrency(3)
        .batch_size(3)
        .build();
    let session = Engine::new(config)?.session();
    session
        .register_parquet(
            "morsels",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    let mut result = session.execute("SELECT id FROM morsels").await?;
    let metrics = result.metrics();
    let mut actual = Vec::new();
    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(ids.values().iter().copied());
    }
    actual.sort_unstable();
    assert_eq!(actual, values);
    let metrics = metrics.snapshot();
    assert_eq!(metrics.peak_active_lanes, 4);
    assert!(metrics.scheduler_wait > std::time::Duration::ZERO);
    Ok(())
}

#[tokio::test]
async fn one_parquet_row_group_uses_one_scan_lane() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("one-morsel.parquet");
    let values = (0_i64..16).collect::<Vec<_>>();
    write_id_fixture(&path, &values)?;

    let config = EngineConfig::builder()
        .memory_limit(128 << 20)
        .compute_threads(4)
        .batch_size(3)
        .build();
    let session = Engine::new(config)?.session();
    session
        .register_parquet(
            "one_morsel",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;

    let mut result = session.execute("SELECT id FROM one_morsel").await?;
    let metrics = result.metrics();
    let mut actual = Vec::new();
    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(ids.values().iter().copied());
    }
    actual.sort_unstable();
    assert_eq!(actual, values);
    assert_eq!(metrics.snapshot().peak_active_lanes, 1);
    Ok(())
}

fn write_id_fixture(path: &std::path::Path, values: &[i64]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let file = File::create(path).map_err(|error| rustdb::Error::io(Some(path.into()), error))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    writer.write(&RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )?)?;
    writer.close()?;
    Ok(())
}

fn write_id_extra_fixture(path: &std::path::Path, ids: &[i64], extra: &[&str]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ]));
    let file = File::create(path).map_err(|error| rustdb::Error::io(Some(path.into()), error))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    writer.write(&RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(extra.to_vec())),
        ],
    )?)?;
    writer.close()?;
    Ok(())
}

fn write_dictionary_fixture(path: &std::path::Path, values: &[&str]) -> Result<()> {
    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
    for value in values {
        builder.append(*value)?;
    }
    write_name_batch(path, Arc::new(builder.finish()) as ArrayRef)
}

fn write_string_fixture(path: &std::path::Path, values: &[&str]) -> Result<()> {
    write_name_batch(path, Arc::new(StringArray::from(values.to_vec())))
}

fn write_name_batch(path: &std::path::Path, names: ArrayRef) -> Result<()> {
    let batch = RecordBatch::try_from_iter([("name", names)])?;
    let file = File::create(path).map_err(|error| rustdb::Error::io(Some(path.into()), error))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

async fn query_count(session: &rustdb::Session, sql: &str) -> Result<i64> {
    let mut result = session.execute(sql).await?;
    let batch = result
        .stream()
        .next()
        .await
        .ok_or_else(|| rustdb::Error::Execution("count returned no batch".to_owned()))??;
    Ok(batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count returns Int64")
        .value(0))
}

use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use rustdb::{Engine, EngineConfig, Result, S3Config};
use uuid::Uuid;

const BUCKET: &str = "rustdb-tests";

#[tokio::test]
async fn copy_and_native_backup_round_trip_through_minio() -> Result<()> {
    let Some(endpoint) = minio_endpoint() else {
        return Ok(());
    };
    let directory = tempfile::tempdir().map_err(|error| rustdb::Error::io(None, error))?;
    let database = directory.path().join("native");
    let restored = directory.path().join("restored");
    let prefix = format!("integration/v08/{}/", Uuid::new_v4());
    let csv = format!("s3://{BUCKET}/{prefix}events.csv");
    let parquet = format!("s3://{BUCKET}/{prefix}events.parquet");
    let late_copy = format!("s3://{BUCKET}/{prefix}late.csv");
    let backup = format!("s3://{BUCKET}/{prefix}backup");
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        .with_endpoint(&endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .s3(S3Config {
            endpoint: Some(endpoint.clone()),
            region: Some("us-east-1".to_owned()),
            force_path_style: true,
            allow_http: true,
            ..S3Config::default()
        })
        .build();
    let engine = Engine::open(&database, config.clone())?;
    let session = engine.session();
    drain(session.execute("CREATE SCHEMA lake").await?).await?;
    drain(
        session
            .execute(
                "CREATE TABLE lake.events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(id, label)",
            )
            .await?,
    )
    .await?;
    drain(
        session
            .execute(&format!(
                "COPY lake.events TO '{csv}' (FORMAT CSV, HEADER TRUE)"
            ))
            .await?,
    )
    .await?;
    drain(
        session
            .execute(&format!("COPY lake.events TO '{parquet}' (FORMAT PARQUET)"))
            .await?,
    )
    .await?;
    assert_eq!(
        scalar_count(
            &session,
            &format!("SELECT count(*) FROM read_csv('{csv}', header = true)")
        )
        .await?,
        3
    );
    assert_eq!(
        scalar_count(
            &session,
            &format!("SELECT count(*) FROM read_parquet('{parquet}')")
        )
        .await?,
        3
    );
    drain(
        session
            .execute("CREATE TABLE lake.csv_import (id BIGINT, label VARCHAR)")
            .await?,
    )
    .await?;
    drain(
        session
            .execute(&format!(
                "COPY lake.csv_import FROM '{csv}' (FORMAT CSV, HEADER TRUE)"
            ))
            .await?,
    )
    .await?;
    assert_eq!(query_count(&session, "lake.csv_import").await?, 3);
    drain(
        session
            .execute("CREATE TABLE lake.parquet_import (id BIGINT, label VARCHAR)")
            .await?,
    )
    .await?;
    drain(
        session
            .execute(&format!(
                "COPY lake.parquet_import FROM '{parquet}' (FORMAT PARQUET)"
            ))
            .await?,
    )
    .await?;
    assert_eq!(query_count(&session, "lake.parquet_import").await?, 3);

    let mut late_result = session
        .execute(&format!(
            "COPY lake.events TO '{late_copy}' (FORMAT CSV, HEADER TRUE)"
        ))
        .await?;
    let late_manifest = Path::from(format!("{prefix}late.csv/_rustdb_manifest.json"));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match store.head(&late_manifest).await {
                Ok(_) => return,
                Err(object_store::Error::NotFound { .. }) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => panic!("failed to inspect late COPY manifest: {error}"),
            }
        }
    })
    .await
    .expect("remote COPY manifest was not published");
    late_result.cancel();
    match late_result
        .stream()
        .next()
        .await
        .expect("late COPY outcome")
    {
        Ok(_) | Err(rustdb::Error::CopyPostCommitFailure { .. }) => {}
        Err(error) => return Err(error),
    }
    // The streaming result owns the single-query admission permit until it is
    // exhausted or dropped. Release it before starting the verification query.
    drop(late_result);
    assert_eq!(
        scalar_count(
            &session,
            &format!("SELECT count(*) FROM read_csv('{late_copy}', header = true)")
        )
        .await?,
        3
    );
    engine.backup_to_location(&backup).await?;
    drop(session);
    drop(engine);

    let restored_engine = Engine::restore_from_location(&backup, &restored, config.clone()).await?;
    let restored_session = restored_engine.session();
    assert_eq!(query_count(&restored_session, "lake.events").await?, 3);
    assert_eq!(query_count(&restored_session, "lake.csv_import").await?, 3);
    assert_eq!(
        query_count(&restored_session, "lake.parquet_import").await?,
        3
    );
    assert_eq!(
        scalar_count(
            &restored_session,
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'lake'",
        )
        .await?,
        1
    );
    drop(restored_session);
    drop(restored_engine);

    let root = Path::from(prefix.trim_end_matches('/'));
    let objects = store
        .list(Some(&root))
        .map_ok(|object| object.location)
        .try_collect::<Vec<_>>()
        .await?;
    for object in objects {
        store.delete(&object).await?;
    }
    Ok(())
}

async fn drain(mut result: rustdb::QueryResult) -> Result<()> {
    while let Some(batch) = result.stream().next().await {
        batch?;
    }
    Ok(())
}

async fn query_count(session: &rustdb::Session, table: &str) -> Result<i64> {
    scalar_count(session, &format!("SELECT count(*) FROM {table}")).await
}

async fn scalar_count(session: &rustdb::Session, sql: &str) -> Result<i64> {
    let mut result = session.execute(sql).await?;
    let batch = result.stream().next().await.expect("count batch")?;
    Ok(batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0))
}

fn minio_endpoint() -> Option<String> {
    let required = matches!(std::env::var("RUSTDB_REQUIRE_MINIO").as_deref(), Ok("1"));
    match std::env::var("RUSTDB_MINIO_ENDPOINT") {
        Ok(endpoint) => Some(endpoint),
        Err(error) if required => {
            panic!("RUSTDB_REQUIRE_MINIO=1 but RUSTDB_MINIO_ENDPOINT is unavailable: {error}")
        }
        Err(_) => None,
    }
}

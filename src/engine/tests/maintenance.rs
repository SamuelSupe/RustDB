use arrow::array::{Int64Array, UInt64Array};

use super::{collect, query_count};
use crate::{Engine, EngineConfig};

#[tokio::test]
async fn maintenance_and_system_views_track_native_state() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .max_concurrent_queries(2)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    collect(
        session
            .execute(
                "CREATE TABLE events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(id, label)",
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        scalar_u64(
            &session,
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'events'"
        )
        .await,
        2
    );

    let held_snapshot = session.execute("SELECT * FROM events").await.unwrap();
    collect(
        session
            .execute("DELETE FROM events WHERE id = 1")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        scalar_u64(
            &session,
            "SELECT deleted_rows FROM rustdb_system.tables WHERE table_name = 'events'"
        )
        .await,
        1
    );
    let retained = collect(session.execute("VACUUM events").await.unwrap()).await;
    assert!(status(&retained).contains("0 snapshots removed"));
    drop(held_snapshot);

    collect(session.execute("COMPACT TABLE events").await.unwrap()).await;
    assert_eq!(query_count(&session, "events").await, 2);
    assert_eq!(
        scalar_u64(
            &session,
            "SELECT deleted_rows FROM rustdb_system.tables WHERE table_name = 'events'"
        )
        .await,
        0
    );
    let vacuumed = collect(session.execute("VACUUM").await.unwrap()).await;
    assert!(status(&vacuumed).contains("snapshot"));
    let checkpoint = collect(session.execute("CHECKPOINT").await.unwrap()).await;
    assert!(status(&checkpoint).contains("WAL records removed"));
    assert_eq!(
        scalar_u64(
            &session,
            "SELECT tracked_transactions FROM rustdb_system.wal"
        )
        .await,
        0
    );
    collect(session.execute("ANALYZE events").await.unwrap()).await;
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "events").await, 2);
}

async fn scalar_u64(session: &crate::Session, sql: &str) -> u64 {
    let batches = collect(session.execute(sql).await.unwrap()).await;
    let array = batches[0].column(0);
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        array.value(0)
    } else {
        u64::try_from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
        )
        .unwrap()
    }
}

fn status(batches: &[arrow::record_batch::RecordBatch]) -> &str {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0)
}

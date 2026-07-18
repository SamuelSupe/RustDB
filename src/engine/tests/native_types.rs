use arrow::array::BooleanArray;

use super::collect;
use crate::{Engine, EngineConfig};

#[tokio::test]
async fn native_time_uuid_and_interval_values_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    collect(
        engine
            .session()
            .execute(
                "CREATE TABLE typed AS SELECT \
                 TIME(9) '12:34:56.123456789' AS t, \
                 TIMESTAMP(9) '2024-02-29 12:34:56.123456789' AS ts, \
                 TIMESTAMPTZ '2024-03-10 01:30:00-05:00' AS z, \
                 UUID '550e8400-e29b-41d4-a716-446655440000' AS id, \
                 INTERVAL '1-2' YEAR TO MONTH AS ym, \
                 INTERVAL '3' DAY AS dt, \
                 INTERVAL '1 02:03:04.5' DAY TO SECOND AS span",
            )
            .await
            .unwrap(),
    )
    .await;
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    let batches = collect(
        reopened
            .session()
            .execute(
                "SELECT \
                 t = TIME(9) '12:34:56.123456789', \
                 ts = TIMESTAMP(9) '2024-02-29 12:34:56.123456789', \
                 z = TIMESTAMPTZ '2024-03-10 01:30:00-05:00', \
                 id = UUID '550e8400-e29b-41d4-a716-446655440000', \
                 ym = INTERVAL '1-2' YEAR TO MONTH, \
                 dt = INTERVAL '3' DAY, \
                 span = INTERVAL '1 02:03:04.5' DAY TO SECOND \
                 FROM typed",
            )
            .await
            .unwrap(),
    )
    .await;
    for column in batches[0].columns() {
        let values = column.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(values.value(0));
    }
}

#[tokio::test]
async fn native_interval_columns_work_in_update_delete_and_returning() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        directory.path().join("native"),
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    collect(
        session
            .execute(
                "CREATE TABLE typed AS SELECT 1 AS id, \
                 INTERVAL '1-2' YEAR TO MONTH AS ym, \
                 INTERVAL '1 02:03:04.5' DAY TO SECOND AS span",
            )
            .await
            .unwrap(),
    )
    .await;

    let updated = collect(
        session
            .execute(
                "UPDATE typed SET id = 2 \
                 WHERE ym = INTERVAL '1-2' YEAR TO MONTH \
                 RETURNING ym = INTERVAL '1-2' YEAR TO MONTH",
            )
            .await
            .unwrap(),
    )
    .await;
    let values = updated[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(values.value(0));

    let deleted = collect(
        session
            .execute(
                "DELETE FROM typed WHERE span = INTERVAL '1 02:03:04.5' DAY TO SECOND \
                 RETURNING ym = INTERVAL '1-2' YEAR TO MONTH",
            )
            .await
            .unwrap(),
    )
    .await;
    let values = deleted[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(values.value(0));
}

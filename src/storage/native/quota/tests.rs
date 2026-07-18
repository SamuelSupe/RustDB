use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::Arc,
};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::Error;

use super::super::{
    NativeDatabase, NativePublishedSnapshot, NativeTransactionChanges, NativeView, NativeWriteMode,
    PreparedSnapshot, TableSnapshot,
};

#[test]
fn final_check_aggregates_rebase_and_large_view_payload() {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let view_sql = format!("SELECT '{}' AS value", "v".repeat(512 * 1024));
    let empty_set = BTreeSet::new();
    let empty_renames = BTreeMap::new();
    let empty_schema_updates = BTreeMap::new();
    let empty_views = BTreeMap::new();

    let reference = NativeDatabase::open(directory.path().join("quota-reference")).unwrap();
    let reference_scenario = stage_scenario(&reference, Arc::clone(&schema), &view_sql);
    let initial_peak = super::projected_transaction_engine_peak(
        &reference,
        std::slice::from_ref(&reference_scenario.write),
        &empty_set,
        &empty_renames,
        &empty_schema_updates,
        &reference_scenario.view_updates,
    )
    .unwrap();
    let calibration = super::super::rebase::write(
        &reference,
        "events",
        &reference_scenario.base,
        &reference_scenario.current,
        &reference_scenario.write.snapshot(),
    )
    .unwrap()
    .expect("disjoint appends are rebaseable");
    let calibration_writes = vec![reference_scenario.write, calibration];
    let rebase_peak = super::projected_transaction_engine_peak(
        &reference,
        std::slice::from_ref(calibration_writes.last().unwrap()),
        &empty_set,
        &empty_renames,
        &empty_schema_updates,
        &empty_views,
    )
    .unwrap();
    let final_peak = super::projected_transaction_engine_peak(
        &reference,
        &calibration_writes,
        &empty_set,
        &empty_renames,
        &empty_schema_updates,
        &reference_scenario.view_updates,
    )
    .unwrap();
    let individually_admitted = initial_peak.max(rebase_peak);
    assert!(final_peak > individually_admitted);
    let limit = individually_admitted + (final_peak - individually_admitted) / 2;
    super::super::transaction_commit::abort(&reference, calibration_writes).unwrap();
    super::prune_published(&reference);
    drop(reference);

    let target_path = directory.path().join("quota-target");
    let mut database = NativeDatabase::open(&target_path).unwrap();
    let scenario = stage_scenario(&database, schema, &view_sql);
    database.quota.engine_limit_bytes = Some(limit);
    let lsn_before_commit = database.wal_info().unwrap().next_lsn;
    let error = database
        .commit_transaction_writes(NativeTransactionChanges {
            snapshot_generation: scenario.snapshot_generation,
            expected_schemas: BTreeMap::from([("main".to_owned(), true)]),
            schema_updates: empty_schema_updates,
            expected: BTreeMap::from([("events".to_owned(), Some(scenario.base))]),
            writes: vec![scenario.write],
            drops: empty_set,
            renames: empty_renames,
            expected_views: BTreeMap::from([("huge".to_owned(), None)]),
            view_updates: scenario.view_updates,
        })
        .unwrap_err();
    assert!(matches!(
        error,
        Error::NativeDiskQuotaExceeded {
            table: None,
            peak_bytes,
            limit_bytes,
            ..
        } if peak_bytes > limit_bytes && limit_bytes == limit
    ));
    let wal = database.wal_info().unwrap();
    assert!(
        wal.next_lsn >= lsn_before_commit + 3,
        "the final check must run after the rebase WAL was created"
    );
    assert_eq!(wal.active_writes, 0);
    assert_eq!(database.catalog_generation(), 2);
    assert!(database.view_definitions().is_empty());
    let current = database.table_snapshot("events").unwrap();
    assert_eq!(current.row_count(), 2);
    assert_eq!(
        super::super::disk_budget::directories_storage_bytes([database.path().join("tables")])
            .unwrap(),
        super::super::disk_budget::directories_storage_bytes(
            current.reachable_directories(database.path())
        )
        .unwrap()
    );
    assert_eq!(
        fs::read_dir(database.path().join("staging"))
            .unwrap()
            .count(),
        0
    );
    drop(current);
    drop(database);

    let reopened = NativeDatabase::open(&target_path).unwrap();
    assert_eq!(reopened.catalog_generation(), 2);
    assert_eq!(reopened.table_snapshot("events").unwrap().row_count(), 2);
    assert!(reopened.view_definitions().is_empty());
    assert_eq!(reopened.wal_info().unwrap().active_writes, 0);
}

struct Scenario {
    snapshot_generation: u64,
    base: Arc<TableSnapshot>,
    current: Arc<TableSnapshot>,
    write: NativePublishedSnapshot,
    view_updates: BTreeMap<String, Option<Arc<NativeView>>>,
}

fn stage_scenario(database: &NativeDatabase, schema: Arc<Schema>, view_sql: &str) -> Scenario {
    let initial = prepared_create(
        database,
        Arc::clone(&schema),
        &int_batch(Arc::clone(&schema), 1),
    );
    database.commit_write(initial).unwrap();
    let (snapshot_generation, schemas, tables, _) = database.transaction_snapshot();
    let base = Arc::clone(tables.get("events").unwrap());
    let transaction = prepared_transaction_append(
        database,
        snapshot_generation,
        Arc::clone(&base),
        &schemas,
        Arc::clone(&schema),
        2,
    );
    let write = database.publish_transaction_write(transaction).unwrap();

    let concurrent = database
        .plan_write(
            "events",
            NativeWriteMode::Append,
            database.catalog_generation(),
            Arc::clone(&schema),
            0,
        )
        .unwrap();
    let mut concurrent = database.start_write(concurrent).unwrap();
    concurrent
        .write_batch(&int_batch(Arc::clone(&schema), 3))
        .unwrap();
    database.commit_write(concurrent.finish().unwrap()).unwrap();
    let current = database.table_snapshot("events").unwrap();
    let view = Arc::new(NativeView::new(1, view_sql.to_owned(), schema).unwrap());
    Scenario {
        snapshot_generation,
        base,
        current,
        write,
        view_updates: BTreeMap::from([("huge".to_owned(), Some(view))]),
    }
}

fn prepared_create(
    database: &NativeDatabase,
    schema: Arc<Schema>,
    batch: &RecordBatch,
) -> PreparedSnapshot {
    let plan = database
        .plan_write(
            "events",
            NativeWriteMode::Create,
            database.catalog_generation(),
            schema,
            1024 * 1024,
        )
        .unwrap();
    let mut writer = database.start_write(plan).unwrap();
    writer.write_batch(batch).unwrap();
    writer.finish().unwrap()
}

fn prepared_transaction_append(
    database: &NativeDatabase,
    snapshot_generation: u64,
    base: Arc<TableSnapshot>,
    schemas: &BTreeSet<String>,
    schema: Arc<Schema>,
    value: i64,
) -> PreparedSnapshot {
    let plan = database
        .plan_transaction_write(
            "events",
            NativeWriteMode::Append,
            snapshot_generation,
            Some(base),
            schemas,
            Arc::clone(&schema),
            0,
        )
        .unwrap();
    let mut writer = database.start_write(plan).unwrap();
    writer.write_batch(&int_batch(schema, value)).unwrap();
    writer.finish().unwrap()
}

fn int_batch(schema: Arc<Schema>, value: i64) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
}

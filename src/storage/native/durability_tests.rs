use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::Error;

use super::{NativeDatabase, NativeTransactionChanges, NativeWriteMode, commit::test_failpoint};

#[test]
fn reopen_resolves_every_native_commit_durability_boundary() {
    for (boundary, committed) in [
        (test_failpoint::Boundary::SnapshotPublished, false),
        (test_failpoint::Boundary::CatalogPrepared, false),
        (test_failpoint::Boundary::WalCommitted, true),
        (test_failpoint::Boundary::CatalogPublished, true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database");
        let database = NativeDatabase::open(&path).unwrap();
        let prepared = prepared_table(&database);

        test_failpoint::arm(boundary);
        let error = database.commit_write(prepared).unwrap_err();
        assert_boundary_error(boundary, &error);
        drop(database);

        let reopened = NativeDatabase::open(&path).unwrap();
        assert_eq!(
            reopened.catalog_generation(),
            u64::from(committed),
            "wrong generation after restart at {boundary:?}"
        );
        assert_eq!(
            reopened.table_snapshot("events").is_ok(),
            committed,
            "wrong table visibility after restart at {boundary:?}"
        );
        let (_, _, active_writes) = reopened.wal().unwrap().stats();
        assert_eq!(active_writes, 0, "active WAL remained at {boundary:?}");
    }
}

#[test]
fn reopen_resolves_every_transaction_commit_durability_boundary() {
    for (boundary, committed) in [
        (test_failpoint::Boundary::SnapshotPublished, false),
        (test_failpoint::Boundary::CatalogPrepared, false),
        (test_failpoint::Boundary::WalCommitted, true),
        (test_failpoint::Boundary::CatalogPublished, true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database");
        let database = NativeDatabase::open(&path).unwrap();
        let prepared = prepared_table(&database);

        if boundary == test_failpoint::Boundary::SnapshotPublished {
            test_failpoint::arm(boundary);
            let error = match database.publish_transaction_write(prepared) {
                Ok(_) => panic!("expected injected {boundary:?} failure"),
                Err(error) => error,
            };
            assert_boundary_error(boundary, &error);
        } else {
            let published = database.publish_transaction_write(prepared).unwrap();
            test_failpoint::arm(boundary);
            let error = database
                .commit_transaction_writes(NativeTransactionChanges {
                    snapshot_generation: 0,
                    expected_schemas: Default::default(),
                    schema_updates: Default::default(),
                    expected: std::collections::BTreeMap::from([("events".to_owned(), None)]),
                    writes: vec![published],
                    drops: Default::default(),
                    renames: Default::default(),
                    expected_views: Default::default(),
                    view_updates: Default::default(),
                })
                .unwrap_err();
            assert_boundary_error(boundary, &error);
        }
        drop(database);

        let reopened = NativeDatabase::open(&path).unwrap();
        assert_eq!(
            reopened.catalog_generation(),
            u64::from(committed),
            "wrong transaction generation after restart at {boundary:?}"
        );
        assert_eq!(
            reopened.table_snapshot("events").is_ok(),
            committed,
            "wrong transaction table visibility after restart at {boundary:?}"
        );
        let (_, _, active_writes) = reopened.wal().unwrap().stats();
        assert_eq!(
            active_writes, 0,
            "active transaction WAL remained at {boundary:?}"
        );
    }
}

fn assert_boundary_error(boundary: test_failpoint::Boundary, error: &Error) {
    match boundary {
        test_failpoint::Boundary::SnapshotPublished | test_failpoint::Boundary::CatalogPrepared => {
            assert!(matches!(error, Error::NativeStorage { .. }), "{error:?}");
        }
        test_failpoint::Boundary::WalCommitted => {
            assert!(
                matches!(error, Error::CommitOutcomeUnknown { .. }),
                "{error:?}"
            );
        }
        test_failpoint::Boundary::CatalogPublished => {
            assert!(
                matches!(error, Error::NativeCommitPostCommitFailure { .. }),
                "{error:?}"
            );
        }
    }
}

fn prepared_table(database: &NativeDatabase) -> super::PreparedSnapshot {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let plan = database
        .plan_write(
            "events",
            NativeWriteMode::Create,
            database.catalog_generation(),
            Arc::clone(&schema),
            1024 * 1024,
        )
        .unwrap();
    let mut writer = database.start_write(plan).unwrap();
    writer
        .write_batch(
            &RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap(),
        )
        .unwrap();
    writer.finish().unwrap()
}

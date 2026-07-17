use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use super::{NativeDatabase, NativeWriteMode};

const SOURCE_BYTES: u64 = 1 << 20;

#[test]
fn a_new_write_counts_retired_storage_pinned_by_an_older_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let database = NativeDatabase::open(directory.path().join("database")).unwrap();
    let schema = schema();
    commit(&database, NativeWriteMode::Create, Arc::clone(&schema));
    let pinned = database.table_snapshots().remove(0).1;

    commit(&database, NativeWriteMode::Replace, Arc::clone(&schema));
    let current = database.table_snapshots().remove(0).1;
    let next = database
        .plan_write(
            "events",
            NativeWriteMode::Replace,
            database.catalog_generation(),
            schema,
            SOURCE_BYTES,
        )
        .unwrap();

    assert!(
        next.retained_old_storage_bytes > current.storage_bytes(),
        "retired storage was omitted from the peak budget"
    );
    assert!(
        next.retained_old_source_bytes > current.source_bytes(),
        "retired source bytes were omitted from the peak budget"
    );
    drop(pinned);
    database.drain_retired().unwrap();
}

fn commit(database: &NativeDatabase, mode: NativeWriteMode, schema: SchemaRef) {
    let plan = database
        .plan_write(
            "events",
            mode,
            database.catalog_generation(),
            Arc::clone(&schema),
            SOURCE_BYTES,
        )
        .unwrap();
    let mut writer = database.start_write(plan).unwrap();
    writer
        .write_batch(
            &RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from_iter_values(0..1_000_i64))],
            )
            .unwrap(),
        )
        .unwrap();
    let prepared = writer.finish().unwrap();
    database.commit_write(prepared).unwrap();
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

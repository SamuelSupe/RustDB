use std::{fs, fs::OpenOptions, sync::Arc};

use arrow::{
    array::{BinaryArray, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use uuid::Uuid;

use super::{
    DeleteVector, NativeSegment, SnapshotOperation, TableSnapshot, layout, load, write_staged,
};
use crate::{Error, storage::native::segment::writer::SegmentWriter};

#[test]
fn persists_and_reloads_a_verified_table_snapshot() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("tables")).unwrap();
    let database_id = Uuid::new_v4().to_string();
    let table_id = Uuid::new_v4().to_string();
    let snapshot_id = Uuid::new_v4().to_string();
    let segment_id = Uuid::new_v4().to_string();
    let directory = layout::snapshot_directory(root.path(), &table_id, 1, &snapshot_id);
    let segments = directory.join("segments");
    fs::create_dir_all(&segments).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let path = segments.join(format!("{segment_id}.rdbseg"));
    let mut writer = SegmentWriter::create_new(&path, Arc::clone(&schema)).unwrap();
    writer.write_batch(&batch).unwrap();
    let metadata = writer.finish().unwrap();
    let source_bytes = metadata.bytes();
    let segment = NativeSegment::new(&segment_id, 1, &snapshot_id, metadata);
    let mut snapshot = TableSnapshot::new(
        &database_id,
        &table_id,
        1,
        &snapshot_id,
        None,
        SnapshotOperation::Import,
        schema,
        source_bytes,
        vec![segment],
    )
    .unwrap();
    write_staged(&directory, &mut snapshot).unwrap();

    let reference = snapshot.table_reference();
    let loaded = load(root.path(), &database_id, &reference).unwrap();
    assert_eq!(loaded.table_id(), table_id);
    assert_eq!(loaded.row_count(), 3);
    assert_eq!(loaded.segment_bytes(), source_bytes);
    assert_eq!(loaded.schema().field(0).name(), "id");
    assert_eq!(loaded.manifest_sha256(), snapshot.manifest_sha256());

    OpenOptions::new()
        .write(true)
        .open(layout::manifest_path(&directory))
        .unwrap()
        .set_len((super::persistence::MAX_TABLE_MANIFEST_BYTES + 1) as u64)
        .unwrap();
    assert!(matches!(
        load(root.path(), &database_id, &reference).unwrap_err(),
        Error::NativeStorage { message, .. } if message.contains("table manifest exceeds")
    ));
}

#[test]
fn rejects_a_corrupt_segment_before_exposing_the_snapshot() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("tables")).unwrap();
    let database_id = Uuid::new_v4().to_string();
    let table_id = Uuid::new_v4().to_string();
    let snapshot_id = Uuid::new_v4().to_string();
    let segment_id = Uuid::new_v4().to_string();
    let directory = layout::snapshot_directory(root.path(), &table_id, 1, &snapshot_id);
    let segments = directory.join("segments");
    fs::create_dir_all(&segments).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let path = segments.join(format!("{segment_id}.rdbseg"));
    let metadata = SegmentWriter::create_new(&path, Arc::clone(&schema))
        .unwrap()
        .finish()
        .unwrap();
    let source_bytes = metadata.bytes();
    let mut snapshot = TableSnapshot::new(
        &database_id,
        &table_id,
        1,
        &snapshot_id,
        None,
        SnapshotOperation::Import,
        schema,
        source_bytes,
        vec![NativeSegment::new(&segment_id, 1, &snapshot_id, metadata)],
    )
    .unwrap();
    write_staged(&directory, &mut snapshot).unwrap();
    fs::write(&path, b"corrupt").unwrap();

    assert!(matches!(
        load(root.path(), &database_id, &snapshot.table_reference()).unwrap_err(),
        Error::NativeStorage { .. }
    ));
}

#[test]
fn v3_snapshot_persists_visible_rows_and_a_versioned_delete_vector() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("tables")).unwrap();
    let database_id = Uuid::new_v4().to_string();
    let table_id = Uuid::new_v4().to_string();
    let snapshot_id = Uuid::new_v4().to_string();
    let segment_id = Uuid::new_v4().to_string();
    let directory = layout::snapshot_directory(root.path(), &table_id, 1, &snapshot_id);
    fs::create_dir_all(directory.join("segments")).unwrap();
    fs::create_dir_all(directory.join("delete-vectors")).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let path = directory
        .join("segments")
        .join(format!("{segment_id}.rdbseg"));
    let mut writer = SegmentWriter::create_new(&path, Arc::clone(&schema)).unwrap();
    writer.write_batch(&batch).unwrap();
    let metadata = writer.finish().unwrap();
    let source_bytes = metadata.bytes();
    let vector = DeleteVector::from_offsets(3, [1]).unwrap();
    let descriptor = vector
        .write(
            &layout::delete_vector_path(root.path(), &table_id, 1, &snapshot_id, &segment_id),
            1,
            &snapshot_id,
            &super::super::disk_budget::DiskBudget::unlimited(),
        )
        .unwrap();
    let segment =
        NativeSegment::new(&segment_id, 1, &snapshot_id, metadata).with_delete_vector(descriptor);
    let mut snapshot = TableSnapshot::new(
        &database_id,
        &table_id,
        1,
        &snapshot_id,
        None,
        SnapshotOperation::Import,
        schema,
        source_bytes,
        vec![segment],
    )
    .unwrap();
    write_staged(&directory, &mut snapshot).unwrap();

    let loaded = load(root.path(), &database_id, &snapshot.table_reference()).unwrap();
    assert_eq!(loaded.format_version(), 3);
    assert_eq!(loaded.physical_row_count(), 3);
    assert_eq!(loaded.deleted_row_count(), 1);
    assert_eq!(loaded.row_count(), 2);
    assert!(loaded.delete_vector_bytes() > 0);
}

#[test]
fn rejects_a_snapshot_above_the_two_times_source_limit() {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Binary,
        false,
    )]));
    let mut state = 0x1234_5678_9abc_def0_u64;
    let values = (0..4_096)
        .map(|_| {
            (0..64)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BinaryArray::from_iter_values(
            values.iter().map(Vec::as_slice),
        ))],
    )
    .unwrap();
    let mut writer =
        SegmentWriter::create_new(directory.path().join("segment.rdbseg"), Arc::clone(&schema))
            .unwrap();
    writer.write_batch(&batch).unwrap();
    let metadata = writer.finish().unwrap();
    assert!(metadata.bytes() > super::super::write_plan::FIXED_METADATA_ALLOWANCE_BYTES);
    let snapshot_id = Uuid::new_v4().to_string();
    let error = TableSnapshot::new(
        Uuid::new_v4().to_string(),
        Uuid::new_v4().to_string(),
        1,
        &snapshot_id,
        None,
        SnapshotOperation::Import,
        schema,
        0,
        vec![NativeSegment::new(
            Uuid::new_v4().to_string(),
            1,
            &snapshot_id,
            metadata,
        )],
    )
    .unwrap_err();
    assert!(matches!(error, Error::ResourceExhausted(_)));
}

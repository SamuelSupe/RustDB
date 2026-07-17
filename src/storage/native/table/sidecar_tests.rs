use std::{fs, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    NativeSegment, PredicateSidecarDescriptor, SnapshotOperation, TableSnapshot,
    format::ManifestEnvelope, layout, load, write_staged,
};
use crate::{
    Error,
    storage::native::{io as native_io, manifest::TableReference, segment::writer::SegmentWriter},
};

struct Fixture {
    root: tempfile::TempDir,
    database_id: String,
    table_id: String,
    snapshot_id: String,
    directory: std::path::PathBuf,
    sidecar_path: Option<std::path::PathBuf>,
    snapshot: TableSnapshot,
}

fn fixture(sidecar_row_groups: Option<u64>) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("tables")).unwrap();
    let database_id = Uuid::new_v4().to_string();
    let table_id = Uuid::new_v4().to_string();
    let snapshot_id = Uuid::new_v4().to_string();
    let segment_id = Uuid::new_v4().to_string();
    let directory = layout::snapshot_directory(root.path(), &table_id, 1, &snapshot_id);
    fs::create_dir_all(directory.join("segments")).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let segment_path = layout::segment_path(root.path(), &table_id, 1, &snapshot_id, &segment_id);
    let mut writer = SegmentWriter::create_new(&segment_path, Arc::clone(&schema)).unwrap();
    writer.write_batch(&batch).unwrap();
    let metadata = writer.finish().unwrap();
    let source_bytes = metadata.bytes();
    let mut segment = NativeSegment::new(&segment_id, 1, &snapshot_id, metadata);
    let sidecar_path = sidecar_row_groups.map(|row_group_count| {
        let path =
            layout::predicate_sidecar_path(root.path(), &table_id, 1, &snapshot_id, &segment_id);
        let contents = b"predicate-sidecar-v1";
        fs::write(&path, contents).unwrap();
        segment = segment
            .clone()
            .with_predicate_sidecar(PredicateSidecarDescriptor::new(
                super::sidecar::FORMAT_VERSION,
                contents.len() as u64,
                format!("{:x}", Sha256::digest(contents)),
                3,
                row_group_count,
                vec![0],
            ));
        path
    });
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
    Fixture {
        root,
        database_id,
        table_id,
        snapshot_id,
        directory,
        sidecar_path,
        snapshot,
    }
}

#[test]
fn verifies_and_accounts_for_a_declared_predicate_sidecar() {
    let fixture = fixture(Some(1));
    let loaded = load(
        fixture.root.path(),
        &fixture.database_id,
        &fixture.snapshot.table_reference(),
    )
    .unwrap();
    let sidecar = fixture.sidecar_path.unwrap();
    assert_eq!(
        loaded.predicate_sidecar_paths(fixture.root.path()),
        [sidecar]
    );
    assert!(loaded.storage_bytes() > loaded.segment_bytes());
}

#[test]
fn rejects_missing_resized_and_corrupt_predicate_sidecars() {
    let missing = fixture(Some(1));
    fs::remove_file(missing.sidecar_path.as_ref().unwrap()).unwrap();
    assert!(
        load(
            missing.root.path(),
            &missing.database_id,
            &missing.snapshot.table_reference()
        )
        .is_err()
    );

    let resized = fixture(Some(1));
    fs::write(resized.sidecar_path.as_ref().unwrap(), b"too-short").unwrap();
    assert!(matches!(
        load(
            resized.root.path(),
            &resized.database_id,
            &resized.snapshot.table_reference()
        )
        .unwrap_err(),
        Error::NativeStorage { message, .. } if message.contains("byte size mismatch")
    ));

    let corrupt = fixture(Some(1));
    let path = corrupt.sidecar_path.as_ref().unwrap();
    let length = fs::metadata(path).unwrap().len() as usize;
    fs::write(path, vec![b'x'; length]).unwrap();
    assert!(matches!(
        load(
            corrupt.root.path(),
            &corrupt.database_id,
            &corrupt.snapshot.table_reference()
        )
        .unwrap_err(),
        Error::NativeStorage { message, .. } if message.contains("sidecar checksum mismatch")
    ));
}

#[test]
fn rejects_a_predicate_sidecar_bound_to_the_wrong_row_group_count() {
    let fixture = fixture(Some(2));
    assert!(matches!(
        load(
            fixture.root.path(),
            &fixture.database_id,
            &fixture.snapshot.table_reference()
        )
        .unwrap_err(),
        Error::NativeStorage { message, .. } if message.contains("row-group binding mismatch")
    ));
}

#[test]
fn reads_a_legacy_v1_manifest_without_a_sidecar() {
    let fixture = fixture(None);
    let path = layout::manifest_path(&fixture.directory);
    let bytes = fs::read(&path).unwrap();
    let mut envelope: ManifestEnvelope = serde_json::from_slice(&bytes).unwrap();
    envelope.manifest.format_version = 1;
    let checksum = native_io::json_sha256(
        &path,
        &envelope.manifest,
        super::persistence::MAX_TABLE_MANIFEST_BYTES,
        "table manifest",
    )
    .unwrap();
    envelope.sha256 = checksum.clone();
    let encoded = native_io::encode_json_bounded(
        &path,
        &envelope,
        super::persistence::MAX_TABLE_MANIFEST_BYTES,
        "table manifest",
        true,
        true,
    )
    .unwrap();
    fs::write(&path, encoded).unwrap();
    let reference = TableReference::new(&fixture.table_id, 1, &fixture.snapshot_id, checksum);

    assert!(load(fixture.root.path(), &fixture.database_id, &reference).is_ok());
}

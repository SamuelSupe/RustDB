use super::*;
use crate::storage::native::segment::predicate_sidecar::PredicateType;

fn file() -> PredicateSidecarFile {
    let block = EncodedPredicateBlock::encode(PredicateType::Int64, &vec![Some(7); 1_024])
        .unwrap()
        .unwrap();
    PredicateSidecarFile::new(
        &"11".repeat(32),
        &"22".repeat(32),
        1_024,
        vec![1_024],
        vec![PredicateSidecarBlock::new(0, 3, block).unwrap()],
    )
    .unwrap()
}

#[test]
fn round_trips_file_bindings_and_range_index() {
    let expected = file();
    let bytes = expected.to_bytes().unwrap();
    let prefix_len = PredicateSidecarIndex::metadata_prefix_len(&bytes[..128]).unwrap();
    let index =
        PredicateSidecarIndex::from_metadata(&bytes[..prefix_len], bytes.len() as u64).unwrap();
    let entry = index.entry(0, 3).unwrap();
    assert_eq!(entry.row_count(), 1_024);
    assert_eq!(entry.sha256().len(), 64);
    assert!(
        index
            .decode_block(entry, &bytes[entry.byte_range().unwrap()])
            .is_ok()
    );

    let decoded = PredicateSidecarFile::from_bytes(&bytes).unwrap();
    assert_eq!(decoded, expected);
    assert_eq!(decoded.schema_fingerprint(), "11".repeat(32));
    assert_eq!(decoded.segment_sha256(), "22".repeat(32));
    assert_eq!(decoded.segment_rows(), 1_024);
    assert_eq!(decoded.row_group_rows(), &[1_024]);
    assert_eq!(decoded.indexed_column_ordinals(), &[3]);
    assert!(decoded.block(0, 3).is_some());
}

#[test]
fn rejects_corrupt_offsets_and_block_checksums() {
    let mut bad_offset = file().to_bytes().unwrap();
    bad_offset[104] = 1;
    assert!(PredicateSidecarFile::from_bytes(&bad_offset).is_err());

    let mut bad_payload = file().to_bytes().unwrap();
    *bad_payload.last_mut().unwrap() ^= 1;
    assert!(matches!(
        PredicateSidecarFile::from_bytes(&bad_payload),
        Err(PredicateSidecarError::Corrupt(message)) if message.contains("checksum")
    ));
}

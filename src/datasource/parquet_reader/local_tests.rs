use std::{fs, fs::File, io, path::Path};

use tempfile::tempdir;

use super::{
    MAX_COALESCED_SPAN_BYTES, local_etag, map_read_error, plan_ranges, read_ranges_blocking,
};
use crate::storage::ObjectSnapshot;

#[test]
fn local_coalescing_preserves_order_and_shares_merged_backing() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("coalesced.bin");
    fs::write(&path, b"0123456789abcdefghijklmnop").unwrap();
    let snapshot = snapshot(&path);
    let ranges = vec![10..12, 0..0, 4..6, 2..5, 12..14, 3..4, 5..7];

    let plan = plan_ranges("file:///coalesced.bin", &ranges, snapshot.size).unwrap();
    assert_eq!(
        plan.spans
            .iter()
            .map(|span| span.range.clone())
            .collect::<Vec<_>>(),
        vec![2..7, 10..14]
    );
    assert_eq!(plan.physical_bytes, 9);

    let output =
        read_ranges_blocking(&path, "file:///coalesced.bin", &snapshot, None, &ranges).unwrap();
    assert_eq!(
        output
            .iter()
            .map(|bytes| bytes.as_ref())
            .collect::<Vec<_>>(),
        vec![
            b"ab".as_slice(),
            b"".as_slice(),
            b"45",
            b"234",
            b"cd",
            b"3",
            b"56"
        ]
    );

    // Overlapping, contained, and adjacent logical ranges are zero-copy slices
    // of their shared physical span.
    assert_eq!(output[2].as_ptr() as usize, output[3].as_ptr() as usize + 2);
    assert_eq!(output[5].as_ptr() as usize, output[3].as_ptr() as usize + 1);
    assert_eq!(output[6].as_ptr() as usize, output[3].as_ptr() as usize + 3);
    assert_eq!(output[4].as_ptr() as usize, output[0].as_ptr() as usize + 2);
}

#[test]
fn local_coalescing_never_reads_across_a_gap() {
    let ranges = vec![9..11, 1..3, 5..8, 3..5, 12..13];
    let plan = plan_ranges("file:///gaps.bin", &ranges, 13).unwrap();

    assert_eq!(
        plan.spans
            .iter()
            .map(|span| span.range.clone())
            .collect::<Vec<_>>(),
        vec![1..8, 9..11, 12..13]
    );
    assert_eq!(plan.physical_bytes, 10);
}

#[test]
fn local_coalescing_bounds_adjacent_backing_allocations() {
    let half = MAX_COALESCED_SPAN_BYTES / 2;
    let ranges = vec![
        0..half,
        half..MAX_COALESCED_SPAN_BYTES,
        MAX_COALESCED_SPAN_BYTES..MAX_COALESCED_SPAN_BYTES + 1,
    ];
    let plan = plan_ranges("file:///bounded.bin", &ranges, MAX_COALESCED_SPAN_BYTES + 1).unwrap();

    assert_eq!(
        plan.spans
            .iter()
            .map(|span| span.range.clone())
            .collect::<Vec<_>>(),
        vec![
            0..MAX_COALESCED_SPAN_BYTES,
            MAX_COALESCED_SPAN_BYTES..MAX_COALESCED_SPAN_BYTES + 1
        ]
    );
    assert_eq!(plan.physical_bytes, MAX_COALESCED_SPAN_BYTES + 1);
}

#[test]
fn local_coalescing_validates_every_range_before_opening_or_reading() {
    let directory = tempdir().unwrap();
    let missing = directory.path().join("missing.bin");
    let snapshot = ObjectSnapshot {
        size: 10,
        e_tag: None,
        version: None,
        local_identity: None,
    };

    let error = read_ranges_blocking(
        &missing,
        "file:///missing.bin",
        &snapshot,
        None,
        &[0..4, std::ops::Range { start: 8, end: 3 }],
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("invalid byte range"), "{error}");
    assert!(!error.contains("No such file"), "{error}");
}

#[test]
fn short_read_rechecks_identity_and_reports_concurrent_truncation() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("truncated.bin");
    fs::write(&path, b"0123456789").unwrap();
    let expected = snapshot(&path);
    let file = File::open(&path).unwrap();
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(2)
        .unwrap();

    let error = map_read_error(
        &file,
        &path,
        "file:///truncated.bin",
        &expected,
        io::Error::new(io::ErrorKind::UnexpectedEof, "short positional read"),
    )
    .to_string();

    assert!(error.contains("object changed during query"), "{error}");
    assert!(error.contains("truncated.bin"), "{error}");
}

fn snapshot(path: &Path) -> ObjectSnapshot {
    let metadata = fs::metadata(path).unwrap();
    ObjectSnapshot {
        size: metadata.len(),
        e_tag: Some(local_etag(&metadata)),
        version: None,
        local_identity: crate::storage::LocalFileIdentity::from_metadata(&metadata),
    }
}

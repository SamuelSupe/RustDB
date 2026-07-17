use std::{fs, fs::File};

use crate::Error;

use super::{encode_json_bounded, json_sha256, read_bounded, read_contents_bounded};

#[test]
fn rejects_an_oversized_file_from_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata");
    fs::write(&path, b"123456789").unwrap();

    assert!(matches!(
        read_bounded(&path, 8, "test metadata"),
        Err(Error::NativeStorage { message, .. }) if message.contains("8-byte limit")
    ));
}

#[test]
fn detects_content_beyond_the_limit_during_reading() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata");
    fs::write(&path, b"123456789").unwrap();
    let mut file = File::open(&path).unwrap();

    assert!(matches!(
        read_contents_bounded(&mut file, &path, 8, "test metadata", 8),
        Err(Error::NativeStorage { message, .. }) if message.contains("grew beyond")
    ));
}

#[test]
fn bounds_json_during_encoding_and_hashing() {
    let path = std::path::Path::new("bounded.json");
    let value = vec!["abcdefgh"; 8];

    assert!(matches!(
        encode_json_bounded(path, &value, 16, "test JSON", false, true),
        Err(Error::NativeStorage { message, .. }) if message.contains("16-byte limit")
    ));
    assert!(matches!(
        json_sha256(path, &value, 16, "test JSON"),
        Err(Error::NativeStorage { message, .. }) if message.contains("16-byte limit")
    ));
}

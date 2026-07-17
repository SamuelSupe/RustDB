use std::{
    fs::{self, FileTimes},
    io::{Seek, SeekFrom, Write},
    time::Duration,
};

use tokio::io::{AsyncReadExt, BufReader};

use super::open_csv_input;
use crate::{
    CsvCompression, S3Config,
    runtime::{MemoryPool, QueryContext},
    storage::LocationResolver,
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn rejects_same_size_mutation_with_restored_mtime_before_read() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("restored-mtime.csv");
    fs::write(&path, b"id,name\n1,old\n").unwrap();
    let source = resolve(&path).await;
    let modified = fs::metadata(&path).unwrap().modified().unwrap();

    std::thread::sleep(Duration::from_millis(2));
    fs::write(&path, b"id,name\n1,new\n").unwrap();
    restore_mtime(&path, modified);

    let error = open_csv_input(&source, source.snapshot(), CsvCompression::None, None)
        .await
        .err()
        .expect("same-size mutation must be rejected");
    assert_object_changed(&error.to_string(), &path);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn rejects_same_size_atomic_replacement_before_read() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("atomic.csv");
    let replacement = directory.path().join("replacement.csv");
    fs::write(&path, b"id,name\n1,old\n").unwrap();
    let source = resolve(&path).await;
    let modified = fs::metadata(&path).unwrap().modified().unwrap();

    fs::write(&replacement, b"id,name\n1,new\n").unwrap();
    restore_mtime(&replacement, modified);
    fs::rename(&replacement, &path).unwrap();

    let error = open_csv_input(&source, source.snapshot(), CsvCompression::None, None)
        .await
        .err()
        .expect("same-size replacement must be rejected");
    assert_object_changed(&error.to_string(), &path);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn rejects_in_place_mutation_before_returning_eof() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mutated-during-read.csv");
    let input = [b"id,value\n".as_slice(), &vec![b'x'; 32 * 1024]].concat();
    fs::write(&path, &input).unwrap();
    let source = resolve(&path).await;
    let modified = fs::metadata(&path).unwrap().modified().unwrap();
    let mut reader = BufReader::with_capacity(
        32,
        open_csv_input(&source, source.snapshot(), CsvCompression::None, None)
            .await
            .unwrap(),
    );
    let mut prefix = [0_u8; 1];
    reader.read_exact(&mut prefix).await.unwrap();

    std::thread::sleep(Duration::from_millis(2));
    let mut writer = fs::OpenOptions::new().write(true).open(&path).unwrap();
    writer.seek(SeekFrom::Start(16 * 1024)).unwrap();
    writer.write_all(b"changed!").unwrap();
    writer.sync_all().unwrap();
    restore_mtime(&path, modified);

    let mut remainder = Vec::new();
    let error = reader
        .read_to_end(&mut remainder)
        .await
        .expect_err("EOF validation must reject an in-flight mutation");
    assert_object_changed(&error.to_string(), &path);
}

#[tokio::test]
async fn reads_unchanged_local_input_and_preserves_metrics() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unchanged.csv");
    let input = b"id,name\n1,alice\n2,bob\n";
    fs::write(&path, input).unwrap();
    let source = resolve(&path).await;
    let context = QueryContext::new(MemoryPool::new(1024 * 1024), directory.path()).unwrap();
    let mut reader = open_csv_input(
        &source,
        source.snapshot(),
        CsvCompression::None,
        Some((&context.control, &context.metrics)),
    )
    .await
    .unwrap();

    let mut output = Vec::new();
    reader.read_to_end(&mut output).await.unwrap();

    assert_eq!(output, input);
    assert_eq!(
        context.metrics.snapshot().csv_source_bytes,
        u64::try_from(input.len()).unwrap()
    );
}

async fn resolve(path: &std::path::Path) -> crate::storage::ObjectSource {
    LocationResolver::new(S3Config::default())
        .resolve(&[path.to_string_lossy().into_owned()])
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

fn restore_mtime(path: &std::path::Path, modified: std::time::SystemTime) {
    fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
}

fn assert_object_changed(error: &str, path: &std::path::Path) {
    assert!(error.contains("object changed during query"), "{error}");
    assert!(
        error.contains(&path.to_string_lossy().into_owned()),
        "{error}"
    );
}

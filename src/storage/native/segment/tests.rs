use std::{
    fs,
    io::{self, Write},
    os::unix::fs::PermissionsExt,
    sync::Arc,
};

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder, basic::Compression,
    file::properties::EnabledStatistics, schema::types::ColumnPath,
};
use sha2::{Digest, Sha256};

use crate::{Error, Result, runtime::MemoryPool};

use super::{
    FORMAT_VERSION, SegmentMetadata, predicate_file_writer,
    predicate_sidecar::PredicateSidecarFile,
    staging_file::Sha256Writer,
    writer::{SegmentWriter, properties_for_test},
};
use crate::storage::native::disk_budget::DiskBudget;

#[test]
fn round_trips_batches_with_fixed_properties() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::io(None, error))?;
    let path = directory.path().join("part-000.rdbseg");
    let schema = schema();
    let mut writer = SegmentWriter::create_new(&path, Arc::clone(&schema))?;
    writer.write_batch(&batch(Arc::clone(&schema), &[1, 2], &["a", "b"]))?;
    writer.write_batch(&batch(Arc::clone(&schema), &[3], &["c"]))?;
    let metadata = writer.finish()?;

    assert_eq!(metadata.format_version(), FORMAT_VERSION);
    assert_eq!(metadata.rows(), 3);
    assert_eq!(metadata.bytes(), fs::metadata(&path).unwrap().len());
    assert_eq!(metadata.schema_fingerprint().len(), 64);

    let file = fs::File::open(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    assert_eq!(builder.metadata().file_metadata().num_rows(), 3);
    let batches = builder
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let ids = batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2, 3]);

    let properties = properties_for_test();
    let column = ColumnPath::from("name");
    assert_eq!(
        properties.compression(&column),
        Compression::ZSTD(Default::default())
    );
    assert!(properties.dictionary_enabled(&column));
    assert_eq!(
        properties.statistics_enabled(&column),
        EnabledStatistics::Page
    );
    assert!(!properties.offset_index_disabled());
    Ok(())
}

#[test]
fn writes_a_valid_zero_row_footer() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::io(None, error))?;
    let path = directory.path().join("empty.rdbseg");
    let metadata = SegmentWriter::create_new(&path, schema())?.finish()?;

    assert_eq!(metadata.rows(), 0);
    assert!(metadata.bytes() > 0);
    let file = fs::File::open(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    assert_eq!(builder.metadata().file_metadata().num_rows(), 0);
    assert_eq!(builder.metadata().num_row_groups(), 0);
    assert!(builder.build()?.next().is_none());
    Ok(())
}

#[test]
fn reports_the_checksum_of_the_final_file() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::io(None, error))?;
    let path = directory.path().join("checksum.rdbseg");
    let schema = schema();
    let mut writer = SegmentWriter::create_new(&path, Arc::clone(&schema))?;
    writer.write_batch(&batch(schema, &[42], &["answer"]))?;
    let metadata = writer.finish()?;

    let bytes = fs::read(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    assert_eq!(metadata.sha256(), format!("{:x}", Sha256::digest(bytes)));
    let json = serde_json::to_vec(&metadata)
        .map_err(|error| Error::Internal(format!("could not encode test metadata: {error}")))?;
    let decoded: SegmentMetadata = serde_json::from_slice(&json)
        .map_err(|error| Error::Internal(format!("could not decode test metadata: {error}")))?;
    assert_eq!(decoded, metadata);
    Ok(())
}

#[test]
fn incremental_checksum_tracks_partial_writes() {
    let bytes = b"partial writes must hash only bytes accepted by the inner writer";
    let mut writer = Sha256Writer::new(PartialWriter::new(3));

    writer.write_all(bytes).unwrap();

    assert_eq!(writer.inner().bytes, bytes);
    assert!(writer.inner().writes > 1);
    assert_eq!(writer.sha256(), format!("{:x}", Sha256::digest(bytes)));
}

#[test]
fn rejects_a_mismatched_batch_schema_without_writing_rows() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::io(None, error))?;
    let path = directory.path().join("mismatch.rdbseg");
    let mut writer = SegmentWriter::create_new(&path, schema())?;
    let other_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    let other = RecordBatch::try_new(
        other_schema,
        vec![Arc::new(arrow::array::UInt64Array::from(vec![1]))],
    )?;

    let error = writer.write_batch(&other).unwrap_err();
    assert!(matches!(
        error,
        Error::NativeStorage { message, .. } if message.contains("schema mismatch")
    ));
    assert_eq!(writer.finish()?.rows(), 0);
    Ok(())
}

#[test]
fn creates_private_files_without_overwriting() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::io(None, error))?;
    let path = directory.path().join("private.rdbseg");
    SegmentWriter::create_new(&path, schema())?.finish()?;

    let mode = fs::metadata(&path)
        .map_err(|error| Error::io(Some(path.clone()), error))?
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
    let error = match SegmentWriter::create_new(&path, schema()) {
        Ok(_) => panic!("create_new unexpectedly overwrote an existing segment"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::Io { source, .. } if source.kind() == std::io::ErrorKind::AlreadyExists
    ));
    Ok(())
}

#[test]
fn writes_a_private_range_indexed_predicate_sidecar() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::io(None, error))?;
    let segment_path = directory.path().join("indexed.rdbseg");
    let sidecar_path = directory.path().join("indexed.rdbpred");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let memory = MemoryPool::new(128 * 1024 * 1024);
    let budget = DiskBudget::unlimited();
    let mut writer = SegmentWriter::create_new_with_budget_and_memory(
        &segment_path,
        Arc::clone(&schema),
        budget.clone(),
        Some(&memory),
    )?;
    writer.write_batch(&RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![7; 128 * 1024]))],
    )?)?;
    let (segment, sidecar) = writer.finish_with_predicate_sidecar()?;
    let sidecar = sidecar.expect("repetitive values retain a predicate sidecar");
    let sha256 = format!("{:x}", Sha256::digest(&sidecar.bytes));
    predicate_file_writer::write(&sidecar_path, &sidecar, &sha256, &budget)?;

    assert_eq!(sidecar.row_group_count, 1);
    assert_eq!(sidecar.indexed_column_ordinals, vec![0]);
    assert_eq!(
        fs::metadata(&sidecar_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let parsed = PredicateSidecarFile::from_bytes(&fs::read(&sidecar_path).unwrap()).unwrap();
    assert_eq!(parsed.segment_sha256(), segment.sha256());
    assert_eq!(parsed.segment_rows(), 128 * 1024);
    assert!(parsed.block(0, 0).is_some());
    drop(sidecar);
    assert_eq!(memory.used(), 0);
    Ok(())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn batch(schema: Arc<Schema>, ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

struct PartialWriter {
    bytes: Vec<u8>,
    max_write: usize,
    writes: usize,
}

impl PartialWriter {
    fn new(max_write: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max_write,
            writes: 0,
        }
    }
}

impl Write for PartialWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len().min(self.max_write);
        self.bytes.extend_from_slice(&bytes[..written]);
        self.writes += 1;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

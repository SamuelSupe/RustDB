use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{Error, Result};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::{
        metadata::KeyValue,
        properties::{EnabledStatistics, WriterProperties, WriterVersion},
    },
};

use super::{
    FORMAT_VERSION, SegmentMetadata,
    fingerprint::schema_fingerprint,
    staging_file::{Sha256Writer, cleanup_error, open_private, require_segment_path, sync_parent},
};
use crate::storage::native::disk_budget::{DiskBudget, QuotaFile};

#[cfg(test)]
use super::predicate_sidecar::{PredicateSidecarArtifact, PredicateSidecarCollector};
#[cfg(test)]
use crate::runtime::MemoryPool;

const CREATED_BY: &str = "rustdb-native-segment-v1";
const ROW_GROUP_ROWS: usize = 128 * 1024;
const PAGE_ROWS: usize = 8 * 1024;
const WRITE_BATCH_ROWS: usize = 8 * 1024;

pub(crate) struct SegmentWriter {
    path: PathBuf,
    schema: SchemaRef,
    schema_fingerprint: String,
    rows: u64,
    writer: ArrowWriter<Sha256Writer<QuotaFile>>,
    #[cfg(test)]
    predicate_sidecar: Option<PredicateSidecarCollector>,
}

impl SegmentWriter {
    #[cfg(test)]
    pub(crate) fn create_new(path: impl AsRef<Path>, schema: SchemaRef) -> Result<Self> {
        Self::create_new_with_budget_and_memory(path, schema, DiskBudget::unlimited(), None)
    }

    #[cfg(test)]
    pub(in crate::storage::native) fn create_new_with_budget_and_memory(
        path: impl AsRef<Path>,
        schema: SchemaRef,
        budget: DiskBudget,
        memory: Option<&MemoryPool>,
    ) -> Result<Self> {
        let predicate_sidecar = PredicateSidecarCollector::new(&schema, memory);
        let mut writer = Self::create_new_inner(path, schema, budget)?;
        writer.predicate_sidecar = Some(predicate_sidecar);
        Ok(writer)
    }

    pub(in crate::storage::native) fn create_new_without_predicate_sidecar(
        path: impl AsRef<Path>,
        schema: SchemaRef,
        budget: DiskBudget,
    ) -> Result<Self> {
        Self::create_new_inner(path, schema, budget)
    }

    fn create_new_inner(
        path: impl AsRef<Path>,
        schema: SchemaRef,
        budget: DiskBudget,
    ) -> Result<Self> {
        let path = path.as_ref();
        require_segment_path(path)?;

        let fingerprint = schema_fingerprint(&schema);
        let file = Sha256Writer::new(QuotaFile::new(open_private(path)?, budget.clone()));
        let writer = ArrowWriter::try_new(
            file,
            Arc::clone(&schema),
            Some(writer_properties(&fingerprint)),
        );
        let writer = match writer {
            Ok(writer) => writer,
            Err(error) => {
                return Err(cleanup_error(
                    path,
                    Error::native_storage(
                        path,
                        format!("could not create native segment writer: {error}"),
                    ),
                ));
            }
        };

        Ok(Self {
            path: path.to_path_buf(),
            schema,
            schema_fingerprint: fingerprint,
            rows: 0,
            writer,
            #[cfg(test)]
            predicate_sidecar: None,
        })
    }

    pub(crate) fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(Error::native_storage(
                &self.path,
                format!(
                    "record batch schema mismatch: expected {}, found {}",
                    self.schema_fingerprint,
                    schema_fingerprint(batch.schema().as_ref())
                ),
            ));
        }

        let rows = u64::try_from(batch.num_rows()).map_err(|_| {
            Error::native_storage(&self.path, "record batch row count does not fit in u64")
        })?;
        let next_rows = self.rows.checked_add(rows).ok_or_else(|| {
            Error::native_storage(&self.path, "native segment row count overflow")
        })?;
        self.writer.write(batch).map_err(|error| {
            Error::native_storage(
                &self.path,
                format!("could not write native segment: {error}"),
            )
        })?;
        #[cfg(test)]
        self.predicate_sidecar
            .as_mut()
            .into_iter()
            .for_each(|sidecar| sidecar.write_batch(batch));
        self.rows = next_rows;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::storage::native) fn finish_with_predicate_sidecar(
        mut self,
    ) -> Result<(SegmentMetadata, Option<PredicateSidecarArtifact>)> {
        let predicate_sidecar = self.predicate_sidecar.take();
        let schema_fingerprint = self.schema_fingerprint.clone();
        let rows = self.rows;
        let metadata = self.finish()?;
        let sidecar = predicate_sidecar
            .and_then(|sidecar| sidecar.finish(&schema_fingerprint, metadata.sha256(), rows));
        Ok((metadata, sidecar))
    }

    pub(crate) fn finish(mut self) -> Result<SegmentMetadata> {
        let completion = (|| {
            self.writer.finish().map_err(|error| {
                Error::native_storage(
                    &self.path,
                    format!("could not finalize native segment: {error}"),
                )
            })?;
            self.writer
                .inner_mut()
                .inner()
                .sync_all()
                .map_err(|error| Error::io(Some(self.path.clone()), error))?;
            let bytes = self
                .writer
                .inner()
                .inner()
                .metadata()
                .map_err(|error| Error::io(Some(self.path.clone()), error))?
                .len();
            Ok((bytes, self.writer.inner().sha256()))
        })();
        drop(self.writer);
        let (bytes, sha256) = match completion {
            Ok(metadata) => metadata,
            Err(error) => return Err(cleanup_error(&self.path, error)),
        };
        if let Err(error) = sync_parent(&self.path) {
            return Err(cleanup_error(&self.path, error));
        }

        Ok(SegmentMetadata::new(
            FORMAT_VERSION,
            self.schema_fingerprint,
            self.rows,
            bytes,
            sha256,
        ))
    }
}

fn writer_properties(fingerprint: &str) -> WriterProperties {
    // Parquet 59.1 can verify page CRC values when present, but its writer has
    // no page-checksum setting. The returned whole-file SHA-256 protects the
    // staged segment until a later format upgrade can enable page CRC writes.
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_created_by(CREATED_BY.to_owned())
        .set_compression(Compression::ZSTD(Default::default()))
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(false)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_data_page_row_count_limit(PAGE_ROWS)
        .set_write_batch_size(WRITE_BATCH_ROWS)
        .set_key_value_metadata(Some(vec![
            KeyValue::new(
                "rustdb.segment.format_version".to_owned(),
                FORMAT_VERSION.to_string(),
            ),
            KeyValue::new(
                "rustdb.segment.schema_sha256".to_owned(),
                fingerprint.to_owned(),
            ),
        ]))
        .build()
}

#[cfg(test)]
pub(super) fn properties_for_test() -> WriterProperties {
    writer_properties("test-fingerprint")
}

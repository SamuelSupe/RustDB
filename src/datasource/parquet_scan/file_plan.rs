use std::{mem::size_of, sync::Arc};

use parquet::arrow::{ProjectionMask, arrow_reader::ArrowReaderMetadata};

use crate::{
    Result,
    runtime::{MemoryReservation, QueryContext},
    storage::{ObjectSnapshot, ObjectSource},
};

use super::super::{
    parquet_decimal::NarrowDecimalDecode,
    parquet_dictionary::DictionaryDecode,
    parquet_metadata::ParquetMetadata,
    parquet_reader::{QueryIo, SnapshotParquetReader},
    parquet_row_filter::ParquetRowFilter,
};

const FILE_PLAN_BASE_BYTES: usize = 512;

/// Immutable, query-local decode state shared by all readers for one file.
/// Arrow's mutable RowFilter and stream are still built independently per
/// reader; this plan only shares the safe compiled description.
pub(in crate::datasource) struct ParquetFilePlan {
    files: Arc<[ObjectSource]>,
    file_index: usize,
    reader: SnapshotParquetReader,
    metadata: ParquetMetadata,
    projection: Vec<usize>,
    projection_mask: ProjectionMask,
    row_filter: Option<ParquetRowFilter>,
    dictionary: Option<DictionaryDecode>,
    narrow_decimal: Option<NarrowDecimalDecode>,
    sparse_decimal_payload: bool,
    _descriptor_reservation: MemoryReservation,
}

impl ParquetFilePlan {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::datasource) fn try_new(
        files: Arc<[ObjectSource]>,
        file_index: usize,
        snapshot: ObjectSnapshot,
        metadata: ParquetMetadata,
        projection: Vec<usize>,
        row_filter: Option<ParquetRowFilter>,
        dictionary: Option<DictionaryDecode>,
        context: &QueryContext,
    ) -> Result<Self> {
        let base_reader_metadata = dictionary
            .as_ref()
            .map_or(metadata.reader_metadata(), |dictionary| {
                dictionary.reader_metadata(metadata.reader_metadata())
            });
        let sparse_decimal_payload = row_filter.as_ref().is_some_and(|filter| {
            filter.has_unfiltered_decimal_payload(&projection, metadata.reader_metadata().schema())
        });
        let narrow_decimal = if sparse_decimal_payload {
            NarrowDecimalDecode::try_new(base_reader_metadata, context)?
        } else {
            None
        };
        let reader_metadata = narrow_decimal
            .as_ref()
            .map_or(base_reader_metadata, NarrowDecimalDecode::reader_metadata);
        let projection_mask =
            ProjectionMask::roots(reader_metadata.parquet_schema(), projection.iter().copied());
        let descriptor_bytes = FILE_PLAN_BASE_BYTES
            .saturating_add(size_of::<Self>())
            .saturating_add(projection.capacity().saturating_mul(size_of::<usize>()))
            .saturating_add(reader_metadata.schema().fields().len().saturating_mul(2));
        let descriptor_reservation = context.memory.try_reserve(descriptor_bytes)?;
        context.metrics.observe_memory(context.memory.used());
        let reader = SnapshotParquetReader::new(
            &files[file_index],
            snapshot,
            Some(QueryIo::new(
                context.control.clone(),
                context.metrics.clone(),
            )),
        );

        Ok(Self {
            files,
            file_index,
            reader,
            metadata,
            projection,
            projection_mask,
            row_filter,
            dictionary,
            narrow_decimal,
            sparse_decimal_payload,
            _descriptor_reservation: descriptor_reservation,
        })
    }

    pub(in crate::datasource) fn file_index(&self) -> usize {
        self.file_index
    }

    pub(in crate::datasource) fn file(&self) -> &ObjectSource {
        &self.files[self.file_index]
    }

    pub(in crate::datasource) fn reader(&self) -> SnapshotParquetReader {
        self.reader.clone()
    }

    pub(in crate::datasource) fn metadata(&self) -> &ParquetMetadata {
        &self.metadata
    }

    pub(in crate::datasource) fn reader_metadata(&self) -> &ArrowReaderMetadata {
        if let Some(decimal) = &self.narrow_decimal {
            decimal.reader_metadata()
        } else {
            self.dictionary
                .as_ref()
                .map_or(self.metadata.reader_metadata(), |dictionary| {
                    dictionary.reader_metadata(self.metadata.reader_metadata())
                })
        }
    }

    pub(in crate::datasource) fn projection(&self) -> &[usize] {
        &self.projection
    }

    pub(in crate::datasource) fn projection_mask(&self) -> ProjectionMask {
        self.projection_mask.clone()
    }

    pub(in crate::datasource) fn row_filter(&self) -> Option<&ParquetRowFilter> {
        self.row_filter.as_ref()
    }

    pub(in crate::datasource) fn dictionary_columns(&self) -> &[usize] {
        self.dictionary
            .as_ref()
            .map_or(&[], DictionaryDecode::output_columns)
    }

    pub(in crate::datasource) fn sparse_decimal_payload(&self) -> bool {
        self.sparse_decimal_payload
    }
}

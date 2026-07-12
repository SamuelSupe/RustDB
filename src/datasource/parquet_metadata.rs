use std::{mem::size_of, sync::Arc};

use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

use super::{
    MetadataCache,
    metadata_cache::metadata_weight,
    parquet_pruning_budget::PruningLease,
    parquet_reader::{QueryIo, SnapshotParquetReader},
};
use crate::{
    EngineConfig, Error, Result,
    runtime::{MemoryReservation, QueryContext},
    storage::{ObjectSnapshot, ObjectSource},
};

const FOOTER_EXPANSION_FACTOR: usize = 16;
const METADATA_FIXED_OVERHEAD: usize = 64 * 1024;
const MAX_REGISTRATION_METADATA_BYTES: usize = 64 * 1024 * 1024;

/// A query-scoped view of decoded Parquet metadata. All morsels for one file
/// share the same inner reservation, so it is released only after the last
/// relevant scan stream has finished or has been cancelled.
#[derive(Clone)]
pub(super) struct ParquetMetadata {
    inner: Arc<MetadataInner>,
}

struct MetadataInner {
    metadata: ArrowReaderMetadata,
    _reservation: Option<MemoryReservation>,
    _pruning_lease: Option<PruningLease>,
}

impl ParquetMetadata {
    pub(super) fn new(
        metadata: ArrowReaderMetadata,
        reservation: Option<MemoryReservation>,
        pruning_lease: Option<PruningLease>,
    ) -> Self {
        Self {
            inner: Arc::new(MetadataInner {
                metadata,
                _reservation: reservation,
                _pruning_lease: pruning_lease,
            }),
        }
    }

    pub(super) fn reader_metadata(&self) -> &ArrowReaderMetadata {
        &self.inner.metadata
    }

    pub(super) fn into_parts(
        self,
    ) -> Result<(
        ArrowReaderMetadata,
        Option<MemoryReservation>,
        Option<PruningLease>,
    )> {
        Arc::try_unwrap(self.inner)
            .map(|inner| (inner.metadata, inner._reservation, inner._pruning_lease))
            .map_err(|_| {
                Error::Internal(
                    "cannot upgrade shared Parquet metadata to page-index metadata".to_owned(),
                )
            })
    }

    #[cfg(test)]
    fn reserved_bytes(&self) -> usize {
        self.inner
            ._reservation
            .as_ref()
            .map_or(0, MemoryReservation::size)
    }
}

pub(super) fn registration_metadata_limit(config: &EngineConfig) -> usize {
    (config.memory_limit / 8)
        .max(1)
        .min(config.memory_limit)
        .min(MAX_REGISTRATION_METADATA_BYTES)
}

pub(super) fn schema_memory_size(schema: &Schema) -> usize {
    let metadata_bytes = schema
        .metadata()
        .iter()
        .fold(0_usize, |bytes, (key, value)| {
            bytes
                .saturating_add(size_of::<(String, String)>())
                .saturating_add(key.len())
                .saturating_add(value.len())
        });
    size_of::<Schema>()
        .saturating_add(schema.fields().size().saturating_mul(2))
        .saturating_add(metadata_bytes.saturating_mul(2))
        .saturating_add(1024)
}

pub(super) fn resize_schema_budget(
    required: usize,
    context: Option<&QueryContext>,
    registration_limit: usize,
    reservation: Option<&mut MemoryReservation>,
) -> Result<()> {
    let Some(context) = context else {
        if required <= registration_limit {
            return Ok(());
        }
        return Err(Error::ResourceExhausted(format!(
            "Parquet table schema requires {required} bytes, but the registration budget has \
             {registration_limit} bytes available"
        )));
    };
    let reservation = reservation.ok_or_else(|| {
        Error::Internal("query Parquet schema is missing its reservation".to_owned())
    })?;
    reservation.try_resize(required).map_err(|_| {
        Error::ResourceExhausted(format!(
            "Parquet table schema requires {required} bytes, but the query memory pool has {} \
             bytes available (limit {} bytes)",
            context.memory.available(),
            context.memory.limit(),
        ))
    })
}

pub(super) async fn load_parquet_metadata(
    file: &ObjectSource,
    snapshot: ObjectSnapshot,
    context: Option<&QueryContext>,
    cache: &MetadataCache,
    registration_limit: usize,
) -> Result<ParquetMetadata> {
    if let Some(metadata) = cache.get_footer(file, &snapshot) {
        return lease_metadata(file, &snapshot, metadata, context, registration_limit);
    }

    let query =
        context.map(|context| QueryIo::new(context.control.clone(), context.metrics.clone()));
    let mut reader = SnapshotParquetReader::new(file, snapshot.clone(), query);
    let footer_len = reader.footer_metadata_len().await?;
    let estimated = estimated_metadata_bytes(footer_len);
    let mut reservation = reserve_before_load(file.uri(), estimated, context, registration_limit)?;

    let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
    let actual = metadata_weight(file, &snapshot, &metadata);
    resize_after_load(
        file.uri(),
        actual,
        context,
        registration_limit,
        reservation.as_mut(),
    )?;
    cache.insert_footer(file, &snapshot, metadata.clone());
    Ok(ParquetMetadata::new(metadata, reservation, None))
}

fn lease_metadata(
    file: &ObjectSource,
    snapshot: &ObjectSnapshot,
    metadata: ArrowReaderMetadata,
    context: Option<&QueryContext>,
    registration_limit: usize,
) -> Result<ParquetMetadata> {
    let actual = metadata_weight(file, snapshot, &metadata);
    let reservation = reserve_before_load(file.uri(), actual, context, registration_limit)?;
    Ok(ParquetMetadata::new(metadata, reservation, None))
}

fn estimated_metadata_bytes(footer_len: usize) -> usize {
    footer_len
        .saturating_mul(FOOTER_EXPANSION_FACTOR)
        .saturating_add(METADATA_FIXED_OVERHEAD)
}

fn reserve_before_load(
    uri: &str,
    required: usize,
    context: Option<&QueryContext>,
    registration_limit: usize,
) -> Result<Option<MemoryReservation>> {
    let Some(context) = context else {
        ensure_registration_limit(uri, required, registration_limit)?;
        return Ok(None);
    };
    context
        .memory
        .try_reserve(required)
        .map(Some)
        .map_err(|_| query_budget_error(uri, required, context))
}

fn resize_after_load(
    uri: &str,
    actual: usize,
    context: Option<&QueryContext>,
    registration_limit: usize,
    reservation: Option<&mut MemoryReservation>,
) -> Result<()> {
    let Some(context) = context else {
        return ensure_registration_limit(uri, actual, registration_limit);
    };
    let reservation = reservation.ok_or_else(|| {
        Error::Internal("query Parquet metadata load is missing its reservation".to_owned())
    })?;
    reservation
        .try_resize(actual)
        .map_err(|_| query_budget_error(uri, actual, context))
}

fn ensure_registration_limit(uri: &str, required: usize, available: usize) -> Result<()> {
    if required <= available {
        return Ok(());
    }
    Err(Error::ResourceExhausted(format!(
        "Parquet metadata for '{uri}' requires {required} bytes, but the per-file registration \
         limit has {available} bytes available"
    )))
}

fn query_budget_error(uri: &str, required: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "Parquet metadata for '{uri}' requires {required} bytes, but the query memory pool has {} \
         bytes available (limit {} bytes)",
        context.memory.available(),
        context.memory.limit(),
    ))
}

#[cfg(test)]
#[path = "parquet_metadata_tests.rs"]
mod tests;

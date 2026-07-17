use std::{mem::size_of, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream};

use super::ParquetMetadata;
use crate::{
    Error, ParquetPruningMode, ParquetScanConfig, Result,
    datasource::{
        MetadataCache, ScanRequest, parquet_bloom::supports_bloom,
        parquet_index_metadata::load_page_index_metadata, parquet_metadata::load_parquet_metadata,
        parquet_page_pruning::supports_page_index, parquet_pruning_budget::PruningBudget,
    },
    runtime::{MemoryReservation, QueryContext},
    storage::ObjectSource,
};

// Futures produced by `buffered` contain several cloned handles in addition
// to the retained entry. Reserve a conservative fixed allowance before the
// stream allocates any descriptors; decoded metadata keeps its own exact
// reservation through `ParquetMetadata`.
const FUTURE_DESCRIPTOR_BYTES: usize = 512;

pub(super) struct MetadataPreplan {
    entries: Box<[Option<PreplannedFile>]>,
    _descriptor_reservation: MemoryReservation,
}

pub(super) struct PreplannedFile {
    metadata: ParquetMetadata,
    page_index_attempted: bool,
}

impl MetadataPreplan {
    pub(super) fn take(&mut self, index: usize) -> Option<PreplannedFile> {
        self.entries.get_mut(index)?.take()
    }
}

impl PreplannedFile {
    pub(super) fn into_parts(self) -> (ParquetMetadata, bool) {
        (self.metadata, self.page_index_attempted)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn try_preload(
    fixed_files: bool,
    files: &Arc<[ObjectSource]>,
    request: &ScanRequest,
    context: &Arc<QueryContext>,
    cache: &MetadataCache,
    config: &ParquetScanConfig,
    io_concurrency: usize,
    target_tasks: usize,
    pruning_budget: &PruningBudget,
) -> Result<Option<MetadataPreplan>> {
    let prefix = preplan_prefix(
        fixed_files,
        files.len(),
        io_concurrency,
        target_tasks,
        request.limit,
    );
    if prefix == 0 {
        return Ok(None);
    }
    context.check_cancelled()?;

    let Some((retained_bytes, total_bytes)) = descriptor_bytes(prefix) else {
        context.check_cancelled()?;
        return Ok(None);
    };
    let Ok(mut descriptor_reservation) = context.memory.try_reserve(total_bytes) else {
        context.check_cancelled()?;
        return Ok(None);
    };

    let attempt_page_index = config.page_index == ParquetPruningMode::Auto
        && supports_page_index(request.predicate.as_ref())
        && !(config.bloom_filter == ParquetPruningMode::Auto
            && supports_bloom(request.predicate.as_ref()));
    let concurrency = prefix.min(io_concurrency).max(1);
    let loaded = stream::iter(0..prefix)
        .map(|index| {
            let files = Arc::clone(files);
            let context = Arc::clone(context);
            let cache = cache.clone();
            let pruning_budget = pruning_budget.clone();
            async move {
                context.check_cancelled()?;
                let file = &files[index];
                let snapshot = context.object_snapshot(file.uri())?;
                let mut metadata = load_parquet_metadata(
                    file,
                    snapshot.clone(),
                    Some(&context),
                    &cache,
                    usize::MAX,
                )
                .await?;
                if attempt_page_index {
                    metadata = load_page_index_metadata(
                        file,
                        &snapshot,
                        metadata,
                        &context,
                        &cache,
                        &pruning_budget,
                    )
                    .await?;
                }
                Ok(PreplannedFile {
                    metadata,
                    page_index_attempted: attempt_page_index,
                })
            }
        })
        .buffered(concurrency)
        .try_collect::<Vec<_>>()
        .await;

    let entries = match loaded {
        Ok(entries) => entries
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        Err(Error::ResourceExhausted(_)) => {
            context.check_cancelled()?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    descriptor_reservation.try_resize(retained_bytes)?;
    Ok(Some(MetadataPreplan {
        entries,
        _descriptor_reservation: descriptor_reservation,
    }))
}

fn preplan_prefix(
    fixed_files: bool,
    file_count: usize,
    io_concurrency: usize,
    target_tasks: usize,
    limit: Option<usize>,
) -> usize {
    if !fixed_files || file_count < 2 || io_concurrency < 2 || target_tasks < 2 || limit.is_some() {
        return 0;
    }
    file_count.min(io_concurrency).min(target_tasks)
}

fn descriptor_bytes(count: usize) -> Option<(usize, usize)> {
    let retained = count.checked_mul(size_of::<Option<PreplannedFile>>())?;
    let futures = count.checked_mul(FUTURE_DESCRIPTOR_BYTES)?;
    Some((retained, retained.checked_add(futures)?))
}

#[cfg(test)]
mod tests {
    use super::{descriptor_bytes, preplan_prefix};

    #[test]
    fn gate_is_fixed_parallel_and_unlimited() {
        assert_eq!(preplan_prefix(true, 4, 8, 3, None), 3);
        assert_eq!(preplan_prefix(false, 4, 8, 3, None), 0);
        assert_eq!(preplan_prefix(true, 1, 8, 3, None), 0);
        assert_eq!(preplan_prefix(true, 4, 1, 3, None), 0);
        assert_eq!(preplan_prefix(true, 4, 8, 1, None), 0);
        assert_eq!(preplan_prefix(true, 4, 8, 3, Some(10)), 0);
    }

    #[test]
    fn descriptor_accounting_is_checked() {
        let (retained, total) = descriptor_bytes(3).unwrap();
        assert!(retained > 0);
        assert!(total > retained);
        assert!(descriptor_bytes(usize::MAX).is_none());
    }
}

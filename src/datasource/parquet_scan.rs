use std::{sync::Arc, time::Instant};

use arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_stream::try_stream;
use futures::{StreamExt, stream::BoxStream};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{RowSelection, RowSelectionPolicy};

mod file_plan;
mod native_sidecar;

pub(super) use file_plan::ParquetFilePlan;

use super::{
    hive::HivePartitions,
    native::{NativePredicateSidecar, SidecarProjectionCandidate},
    parquet_decode_schedule::next_with_compute,
    parquet_predicate_cache::PredicateCachePlan,
    schema_evolution::{align_batch_to_schema, align_batch_to_schema_preserving_dictionaries},
};
use crate::{
    Result,
    runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream},
};

pub(super) struct ParquetMorsel {
    pub(super) file: Arc<ParquetFilePlan>,
    pub(super) row_groups: Vec<usize>,
    pub(super) row_limit: Option<usize>,
    pub(super) row_selection: Option<RowSelection>,
    pub(super) predicate_cache: PredicateCachePlan,
    pub(super) apply_row_filter: bool,
    pub(super) sidecar_selection_leases: Vec<crate::runtime::MemoryReservation>,
    pub(super) native_sidecar: Option<NativeSidecarMorsel>,
}

#[derive(Clone)]
pub(super) struct NativeSidecarMorsel {
    pub(super) sidecar: NativePredicateSidecar,
    pub(super) projection: Option<Arc<SidecarProjectionCandidate>>,
    pub(super) predicate: Arc<super::ScanPredicate>,
    pub(super) table_schema: SchemaRef,
}

pub(super) type ParquetMorselStream = BoxStream<'static, Result<ParquetMorsel>>;

pub(super) fn scan_morsels(
    morsels: ParquetMorselStream,
    output_schema: SchemaRef,
    hive: Option<Arc<HivePartitions>>,
    context: Arc<QueryContext>,
    io_concurrency: usize,
    decode_batch_size: usize,
    limit: Option<usize>,
) -> RecordBatchStream {
    let streams = morsels.map({
        let output_schema = Arc::clone(&output_schema);
        let hive = hive.clone();
        let context = Arc::clone(&context);
        move |morsel| match morsel {
            Ok(morsel) => morsel_stream(
                morsel,
                Arc::clone(&output_schema),
                hive.clone(),
                Arc::clone(&context),
                decode_batch_size,
            ),
            Err(error) => {
                boxed_record_batch_stream(futures::stream::once(async move { Err(error) }))
            }
        }
    });
    let mut merged = streams.flatten_unordered(io_concurrency);
    boxed_record_batch_stream(try_stream! {
        let mut remaining = limit.unwrap_or(usize::MAX);
        while remaining > 0 {
            let Some(batch) = merged.next().await else {
                break;
            };
            context.check_cancelled()?;
            let batch = batch?;
            let batch = if batch.num_rows() > remaining {
                batch.slice(0, remaining)
            } else {
                batch
            };
            remaining = remaining.saturating_sub(batch.num_rows());
            yield batch;
        }
    })
}

pub(super) fn morsel_stream(
    morsel: ParquetMorsel,
    output_schema: SchemaRef,
    hive: Option<Arc<HivePartitions>>,
    context: Arc<QueryContext>,
    decode_batch_size: usize,
) -> RecordBatchStream {
    boxed_record_batch_stream(try_stream! {
        context.check_cancelled()?;
        let file = morsel.file;
        let row_groups = morsel.row_groups;
        let row_limit = morsel.row_limit;
        let mut row_selection = morsel.row_selection;
        let mut apply_row_filter = morsel.apply_row_filter;
        let mut sidecar_selection_leases = morsel.sidecar_selection_leases;
        if let Some(sidecar) = morsel.native_sidecar {
            match native_sidecar::decode(
                sidecar,
                &file,
                &row_groups,
                row_selection.take(),
                decode_batch_size,
                &context,
            ).await? {
                native_sidecar::SidecarDecode::Projected(batches) => {
                    for batch in batches {
                        let (batch, lease) = batch.into_parts();
                        context.metrics.record_scan(
                            u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                            1,
                            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
                        );
                        // scan_tasks already owns a preclaim while polling this
                        // stream. Public scans cross the accounting boundary here.
                        drop(lease);
                        yield batch;
                    }
                    return;
                }
                native_sidecar::SidecarDecode::Empty => return,
                native_sidecar::SidecarDecode::Parquet {
                    row_selection: selection,
                    apply_row_filter: filter,
                    leases,
                } => {
                    row_selection = selection;
                    apply_row_filter = filter;
                    sidecar_selection_leases.extend(leases);
                }
            }
        }
        let predicate_cache = if apply_row_filter {
            morsel.predicate_cache
        } else {
            PredicateCachePlan::default()
        };
        let (predicate_cache_limit, predicate_cache_reservation) =
            predicate_cache.try_reserve(&context.memory);
        let mut batches = {
            let permit = context.acquire_compute().await?;
            context
                .metrics
                .record_parquet_decode_compute_permit_wait(permit.wait_time());
            context.check_cancelled()?;
            let _active = context.scheduler.enter_lane();
            let decoder_started = Instant::now();
            let reader = file.reader();
            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                reader,
                file.reader_metadata().clone(),
            )
            .with_batch_size(decode_batch_size)
            .with_projection(file.projection_mask())
            .with_row_groups(row_groups)
            // Never inherit Arrow's unaccounted 100 MiB per-row-group default.
            .with_max_predicate_cache_size(predicate_cache_limit);
            if let Some(selection) = row_selection {
                builder = builder.with_row_selection(selection);
            }
            if apply_row_filter && file.sparse_decimal_payload() {
                // Arrow's default threshold selects a full mask for the Q6-like
                // sparse shape. A lower Auto threshold retains the safe fallback
                // while allowing selector-based late materialisation. Any decoded
                // predicate cache was admitted and reserved above.
                builder = builder.with_row_selection_policy(RowSelectionPolicy::Auto {
                    threshold: 16,
                });
            }
            if apply_row_filter
                && let Some(filter) = file.row_filter()
            {
                builder = builder.with_row_filter(
                    filter.build(
                        file.metadata().reader_metadata().parquet_schema(),
                        context.metrics.clone(),
                    ),
                );
            }
            if let Some(limit) = row_limit {
                builder = builder.with_limit(limit);
            }
            let batches = builder.build()?;
            context.metrics.record_parquet_decode_activity(
                decoder_started.elapsed(),
                0,
                0,
            );
            context.metrics.add_parquet_reader_build();
            batches
        };

        while let Some((batch, permit)) =
            next_with_compute(&mut batches, Arc::clone(&context)).await?
        {
            context.check_cancelled()?;
            let decoded = batch?;
            let batch = if hive.is_none()
                && file.dictionary_columns().is_empty()
                && decoded.schema() == output_schema
            {
                decoded
            } else {
                let _active = context.scheduler.enter_lane();
                let alignment_started = Instant::now();
                let batch = align_batch_preserving(
                    decoded,
                    &output_schema,
                    hive.as_deref(),
                    file.file_index(),
                    file.file().uri(),
                    file.dictionary_columns(),
                )?;
                context
                    .metrics
                    .record_parquet_alignment_time(alignment_started.elapsed());
                batch
            };
            context.metrics.record_scan(
                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                1,
                u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
            );
            drop(permit);
            yield batch;
        }
        // The reader owns Arrow's row-group cache. Drop it before releasing the
        // reservation that bounds the cache's retained arrays and map entries.
        drop(batches);
        drop(predicate_cache_reservation);
        drop(sidecar_selection_leases);
        drop(file);
    })
}

#[cfg(test)]
pub(super) fn align_batch(
    batch: RecordBatch,
    schema: &SchemaRef,
    hive: Option<&HivePartitions>,
    file: usize,
    uri: &str,
) -> Result<RecordBatch> {
    align_batch_preserving(batch, schema, hive, file, uri, &[])
}

fn align_batch_preserving(
    batch: RecordBatch,
    schema: &SchemaRef,
    hive: Option<&HivePartitions>,
    file: usize,
    uri: &str,
    dictionary_columns: &[usize],
) -> Result<RecordBatch> {
    if hive.is_none() {
        if dictionary_columns.is_empty() && batch.schema().as_ref() == schema.as_ref() {
            validate_non_nullable(&batch, uri)?;
            return Ok(batch);
        }
        return if dictionary_columns.is_empty() {
            align_batch_to_schema(batch, Arc::clone(schema), uri)
        } else {
            align_batch_to_schema_preserving_dictionaries(
                batch,
                Arc::clone(schema),
                uri,
                dictionary_columns,
            )
        };
    }

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();
    if let Some(hive) = hive {
        for field in schema.fields() {
            if batch.schema().field_with_name(field.name()).is_ok() {
                continue;
            }
            if let Some(column) = hive.array(file, field.name(), batch.num_rows())? {
                fields.push(Arc::clone(field));
                columns.push(column);
            }
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    let augmented = RecordBatch::try_new_with_options(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        columns,
        &options,
    )?;
    if dictionary_columns.is_empty() {
        align_batch_to_schema(augmented, Arc::clone(schema), uri)
    } else {
        align_batch_to_schema_preserving_dictionaries(
            augmented,
            Arc::clone(schema),
            uri,
            dictionary_columns,
        )
    }
}

fn validate_non_nullable(batch: &RecordBatch, uri: &str) -> Result<()> {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(crate::Error::Execution(format!(
                "Parquet schema alignment failed for URI '{uri}', column '{}': source contains NULL values for a non-nullable target",
                field.name(),
            )));
        }
    }
    Ok(())
}

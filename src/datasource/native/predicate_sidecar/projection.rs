use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use parquet::{arrow::arrow_reader::RowSelection, file::metadata::RowGroupMetaData};
use sha2::{Digest, Sha256};

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, QueryContext, estimate_schema_batch_bytes},
    storage::NativePredicateBlock,
};

use super::{NativePredicateSidecar, compile_predicate};

mod plan;
use plan::{ProjectionPlan, parquet_required_bytes, projection_plan, selection_mask};

/// A zero-I/O proof that a Native companion may cover an exact predicate and
/// every requested output column. Row-group completeness is checked only
/// after the query-local index has been loaded.
pub(in crate::datasource) struct SidecarProjectionCandidate {
    predicates: BTreeMap<u32, Vec<crate::storage::NativePredicate>>,
    projected_columns: Vec<u32>,
    column_types: BTreeMap<u32, DataType>,
    schema: SchemaRef,
}

pub(in crate::datasource) enum SidecarProjectionExecution {
    Projected(Vec<BatchEnvelope>),
    Fallback,
}

const MAX_PROJECTED_ROW_GROUPS: usize = 4;

impl NativePredicateSidecar {
    /// Performs the cheap, query-shape-only screening for a full sidecar read.
    /// This deliberately does not load metadata or claim that individual row
    /// groups contain blocks.
    pub(in crate::datasource) fn projection_candidate(
        &self,
        predicate: &crate::datasource::ScanPredicate,
        table_schema: &Schema,
        file_schema: &Schema,
        projected_columns: &[usize],
    ) -> Option<SidecarProjectionCandidate> {
        let predicates = compile_predicate(predicate, table_schema, file_schema)?;
        let projected_indices = projected_columns.to_vec();
        let projected_columns = projected_indices
            .iter()
            .map(|column| {
                let field = file_schema.fields().get(*column)?;
                projection_type_supported(field.data_type())?;
                let column = u32::try_from(*column).ok()?;
                self.binding
                    .indexed_column_ordinals()
                    .binary_search(&column)
                    .ok()?;
                Some(column)
            })
            .collect::<Option<Vec<_>>>()?;
        if predicates.keys().any(|column| {
            self.binding
                .indexed_column_ordinals()
                .binary_search(column)
                .is_err()
        }) {
            return None;
        }
        let mut column_types = BTreeMap::new();
        for &column in predicates.keys().chain(projected_columns.iter()) {
            let data_type = file_schema
                .fields()
                .get(usize::try_from(column).ok()?)?
                .data_type()
                .clone();
            projection_type_supported(&data_type)?;
            column_types.insert(column, data_type);
        }
        let projected_fields = projected_indices
            .iter()
            .map(|column| {
                let file_field = file_schema.fields().get(*column)?;
                let table_column = table_schema.index_of(file_field.name()).ok()?;
                let table_field = table_schema.fields().get(table_column)?;
                (table_field.data_type() == file_field.data_type()).then(|| Arc::clone(table_field))
            })
            .collect::<Option<Vec<_>>>()?;
        let schema = Arc::new(Schema::new_with_metadata(
            projected_fields,
            table_schema.metadata().clone(),
        ));
        Some(SidecarProjectionCandidate {
            predicates,
            projected_columns,
            column_types,
            schema,
        })
    }

    /// Attempts to answer one bounded Parquet row-group chunk from its Native
    /// companion. Coverage and cost are admitted atomically for the whole
    /// morsel, ranges are fetched together, and each row group retains its own
    /// output lease so batches can be released independently downstream.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::datasource) async fn try_project_chunk(
        &self,
        candidate: &SidecarProjectionCandidate,
        row_groups: &[usize],
        parquet_row_groups: &[&RowGroupMetaData],
        row_selection: Option<&RowSelection>,
        max_output_rows: usize,
        context: &QueryContext,
    ) -> Result<SidecarProjectionExecution> {
        if row_groups.is_empty() || row_groups.len() != parquet_row_groups.len() {
            return Err(Error::Internal(
                "Native sidecar projection received an invalid row-group chunk".to_owned(),
            ));
        }
        if row_groups.len() > MAX_PROJECTED_ROW_GROUPS {
            return Ok(projection_fallback(context, row_groups.len()));
        }
        let row_counts = row_groups
            .iter()
            .zip(parquet_row_groups)
            .map(|(row_group, metadata)| {
                usize::try_from(metadata.num_rows()).map_err(|_| {
                    Error::Execution(format!(
                        "Parquet row group {row_group} in {} has an invalid row count",
                        self.data_uri
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let total_rows = row_counts.iter().try_fold(0_usize, |total, rows| {
            total.checked_add(*rows).ok_or_else(|| {
                Error::ResourceExhausted(format!(
                    "Native predicate sidecar row-group chunk for {} exceeds this platform",
                    self.data_uri
                ))
            })
        })?;
        let Some(loaded) = self.loaded_index(context).await? else {
            return Ok(projection_fallback(context, row_groups.len()));
        };

        let mut required = candidate.predicates.keys().copied().collect::<Vec<_>>();
        required.extend(candidate.projected_columns.iter().copied());
        required.sort_unstable();
        required.dedup();

        let mut plans = Vec::with_capacity(row_groups.len());
        let mut sidecar_bytes = 0_usize;
        let mut parquet_bytes = 0_u64;
        for ((row_group, row_count), parquet_row_group) in
            row_groups.iter().zip(&row_counts).zip(parquet_row_groups)
        {
            let row_group = u32::try_from(*row_group).map_err(|_| {
                Error::Execution(format!(
                    "Native predicate sidecar row-group index is too large for {}",
                    self.source.uri()
                ))
            })?;
            if loaded
                .index
                .row_group_rows()
                .get(row_group as usize)
                .copied()
                != u32::try_from(*row_count).ok()
            {
                return Err(self.corrupt("row-group row count does not match Parquet metadata"));
            }
            let Some(plan) = projection_plan(&loaded.index, row_group, *row_count, &required)
                .map_err(|message| self.corrupt(message))?
            else {
                return Ok(projection_fallback(context, row_groups.len()));
            };
            sidecar_bytes = sidecar_bytes.checked_add(plan.total_bytes).ok_or_else(|| {
                Error::ResourceExhausted(format!(
                    "Native predicate sidecar projection bytes for {} exceed this platform",
                    self.data_uri
                ))
            })?;
            let Some(group_parquet_bytes) = parquet_required_bytes(parquet_row_group, &required)
            else {
                return Ok(projection_fallback(context, row_groups.len()));
            };
            parquet_bytes = parquet_bytes
                .checked_add(group_parquet_bytes)
                .ok_or_else(|| {
                    Error::ResourceExhausted(format!(
                        "Parquet projection bytes for {} exceed u64",
                        self.data_uri
                    ))
                })?;
            plans.push(plan);
        }
        // Full projection must avoid at least twice its physical query bytes.
        // The bounded SF1 Q6 diagnostics showed that 1:1 admission saved only
        // about 4.4 MiB of Parquet I/O while fixed-width sidecar evaluation
        // still regressed wall time from 25.81 ms to 45.25 ms.
        let cost_admitted = u64::try_from(sidecar_bytes)
            .ok()
            .is_some_and(|sidecar| sidecar <= parquet_bytes / 2);
        if !cost_admitted {
            return Ok(projection_fallback(context, row_groups.len()));
        }

        let output_bytes = row_counts.iter().fold(0_usize, |bytes, rows| {
            bytes.saturating_add(estimate_schema_batch_bytes(
                candidate.schema.as_ref(),
                (*rows).min(max_output_rows),
            ))
        });
        let workspace_bytes = sidecar_bytes
            .saturating_add(total_rows.saturating_mul(2))
            .saturating_add(output_bytes);
        let Ok(mut lease) = context.memory.try_reserve(workspace_bytes) else {
            return Ok(projection_fallback(context, row_groups.len()));
        };
        context.metrics.observe_memory(context.memory.used());

        let snapshot = context.object_snapshot(self.source.uri())?;
        let reader = super::SnapshotParquetReader::new(
            &self.source,
            snapshot,
            Some(super::QueryIo::for_native_predicate_sidecar(
                context.control.clone(),
                context.metrics.clone(),
            )),
        );
        let ranges = plans
            .iter()
            .flat_map(ProjectionPlan::ranges)
            .collect::<Vec<_>>();
        let blocks = reader.query_ranges(ranges).await?;
        let expected_blocks: usize = plans.iter().map(ProjectionPlan::block_count).sum();
        if blocks.len() != expected_blocks {
            return Err(self.corrupt("projection block range read returned an invalid count"));
        }

        let _compute = context.acquire_compute().await?;
        let _active = context.scheduler.enter_lane();
        context.check_cancelled()?;
        for (block, bytes) in plans.iter().flat_map(ProjectionPlan::blocks).zip(&blocks) {
            let (length, sha256) = block;
            if u64::try_from(bytes.len()).ok() != Some(length)
                || format!("{:x}", Sha256::digest(bytes)) != sha256
            {
                return Err(self.corrupt("projection block checksum or length mismatch"));
            }
        }

        let block_offsets = plans
            .iter()
            .scan(0_usize, |offset, plan| {
                let current = *offset;
                *offset += plan.block_count();
                Some(current)
            })
            .collect::<Vec<_>>();
        let mut selected = selection_mask(row_selection, total_rows)?;
        let mut selected_rows = Vec::with_capacity(row_counts.len());
        let mut row_offset = 0_usize;
        for ((plan, block_offset), row_count) in plans.iter().zip(&block_offsets).zip(&row_counts) {
            context.check_cancelled()?;
            let row_end = row_offset + *row_count;
            let group_selected = &mut selected[row_offset..row_end];
            let group_blocks = &blocks[*block_offset..*block_offset + plan.block_count()];
            for (&column, predicates) in &candidate.predicates {
                let bytes = plan.bytes(column, group_blocks)?;
                let data_type = candidate.column_type(column)?;
                let current =
                    NativePredicateBlock::evaluate_all_bytes_for_type(bytes, data_type, predicates)
                        .map_err(|error| self.corrupt(error.to_string()))?;
                if current.len() != *row_count {
                    return Err(self.corrupt("predicate block decoded an invalid row count"));
                }
                group_selected
                    .iter_mut()
                    .zip(current)
                    .for_each(|(selected, current)| *selected &= current);
            }
            let rows = group_selected.iter().filter(|selected| **selected).count();
            if rows > max_output_rows {
                return Ok(projection_fallback(context, row_groups.len()));
            }
            selected_rows.push(rows);
            row_offset = row_end;
        }

        let mut batches = Vec::with_capacity(row_counts.len());
        row_offset = 0;
        for (((plan, block_offset), row_count), selected_rows) in plans
            .iter()
            .zip(&block_offsets)
            .zip(&row_counts)
            .zip(&selected_rows)
        {
            context.check_cancelled()?;
            let row_end = row_offset + *row_count;
            let group_selected = &selected[row_offset..row_end];
            let group_blocks = &blocks[*block_offset..*block_offset + plan.block_count()];
            let mut decoded = BTreeMap::<u32, ArrayRef>::new();
            for &column in &candidate.projected_columns {
                if decoded.contains_key(&column) {
                    continue;
                }
                let bytes = plan.bytes(column, group_blocks)?;
                let data_type = candidate.column_type(column)?;
                let array = NativePredicateBlock::decode_selected_bytes_for_type(
                    bytes,
                    data_type,
                    group_selected,
                )
                .map_err(|error| self.corrupt(error.to_string()))?;
                if array.len() != *selected_rows {
                    return Err(self.corrupt("projection block decoded an invalid row count"));
                }
                decoded.insert(column, array);
            }
            let columns = candidate
                .projected_columns
                .iter()
                .map(|column| {
                    decoded.get(column).cloned().ok_or_else(|| {
                        Error::Internal(
                            "Native sidecar projection lost a decoded column".to_owned(),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            for (field, column) in candidate.schema.fields().iter().zip(&columns) {
                if !field.is_nullable() && column.null_count() != 0 {
                    return Err(self.corrupt(format!(
                        "projection column '{}' contains NULL for a non-nullable field",
                        field.name()
                    )));
                }
            }
            let batch = if columns.is_empty() {
                RecordBatch::try_new_with_options(
                    Arc::clone(&candidate.schema),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(*selected_rows)),
                )?
            } else {
                RecordBatch::try_new(Arc::clone(&candidate.schema), columns)?
            };
            batches.push(batch);
            row_offset = row_end;
        }
        drop(blocks);
        drop(selected);
        context.metrics.record_native_predicate_sidecar_selection(
            u64::try_from(total_rows).unwrap_or(u64::MAX),
            u64::try_from(selected_rows.iter().sum::<usize>()).unwrap_or(u64::MAX),
        );
        batches.retain(|batch| batch.num_rows() != 0);
        let retained_batch_bytes = batches.iter().try_fold(0_usize, |bytes, batch| {
            bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(|| {
                    Error::ResourceExhausted(format!(
                        "Native sidecar projected output for {} exceeds this platform",
                        self.data_uri
                    ))
                })
        })?;
        // All transient blocks and masks have been dropped. Reconcile the
        // admitted workspace to the exact retained Arrow buffers before
        // splitting it into independently releasable batch leases.
        lease.try_resize(retained_batch_bytes)?;
        let mut projected = Vec::with_capacity(batches.len());
        for batch in batches {
            let batch_bytes = batch.get_array_memory_size();
            let batch_lease = lease.split_off(batch_bytes)?;
            projected.push(BatchEnvelope::from_reservation(
                batch,
                batch_lease,
                "Native sidecar projection",
            )?);
        }
        for rows in &selected_rows {
            context.metrics.add_native_predicate_sidecar_exact_bypass();
            context
                .metrics
                .record_native_predicate_sidecar_full_projection(
                    u64::try_from(*rows).unwrap_or(u64::MAX),
                );
        }
        Ok(SidecarProjectionExecution::Projected(projected))
    }
}

impl SidecarProjectionCandidate {
    fn column_type(&self, column: u32) -> Result<&DataType> {
        self.column_types.get(&column).ok_or_else(|| {
            Error::Internal(format!(
                "Native sidecar projection lost the type for column {column}"
            ))
        })
    }
}

fn projection_type_supported(data_type: &DataType) -> Option<()> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Date32 => {
            Some(())
        }
        DataType::Decimal128(precision, _) if *precision <= 18 => Some(()),
        _ => None,
    }
}

fn projection_fallback(context: &QueryContext, row_groups: usize) -> SidecarProjectionExecution {
    context
        .metrics
        .add_native_predicate_sidecar_full_projection_fallback_row_groups(
            u64::try_from(row_groups).unwrap_or(u64::MAX),
        );
    SidecarProjectionExecution::Fallback
}

use std::{collections::BTreeMap, fmt, mem::size_of, sync::Arc};

use arrow::datatypes::{DataType, Schema};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::RowGroupMetaData;
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;

use crate::{
    Error, Result,
    datasource::{ComparisonOp, PredicateValue, ScanPredicate},
    runtime::{MemoryReservation, QueryContext},
    storage::{
        NativePredicate, NativePredicateBlock, NativePredicateComparisonOp,
        NativePredicateSidecarBinding, NativePredicateSidecarIndex, ObjectSource,
    },
};

use super::super::parquet_reader::{QueryIo, SnapshotParquetReader};

mod projection;

pub(in crate::datasource) use projection::{
    SidecarProjectionCandidate, SidecarProjectionExecution,
};

const SIDECAR_HEADER_BYTES: usize = 128;

/// Query-local companion for one fixed Native Parquet segment.
#[derive(Clone)]
pub(in crate::datasource) struct NativePredicateSidecar {
    data_uri: Arc<str>,
    source: ObjectSource,
    binding: NativePredicateSidecarBinding,
    index: Arc<OnceCell<Option<Arc<LoadedIndex>>>>,
}

pub(in crate::datasource) struct SidecarSelection {
    pub(in crate::datasource) selection: RowSelection,
    pub(in crate::datasource) selected_rows: usize,
    pub(in crate::datasource) lease: MemoryReservation,
}

struct LoadedIndex {
    index: NativePredicateSidecarIndex,
    _memory: MemoryReservation,
}

impl fmt::Debug for NativePredicateSidecar {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativePredicateSidecar")
            .field("data_uri", &self.data_uri)
            .field("source", &self.source)
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl NativePredicateSidecar {
    pub(in crate::datasource) fn new(
        data_uri: &str,
        source: ObjectSource,
        binding: NativePredicateSidecarBinding,
    ) -> Self {
        Self {
            data_uri: Arc::from(data_uri),
            source,
            binding,
            index: Arc::new(OnceCell::new()),
        }
    }

    pub(in crate::datasource) fn data_uri(&self) -> &str {
        &self.data_uri
    }

    pub(in crate::datasource) fn supports_predicate(
        &self,
        predicate: &ScanPredicate,
        table_schema: &Schema,
        file_schema: &Schema,
    ) -> bool {
        compile_predicate(predicate, table_schema, file_schema).is_some_and(|leaves| {
            leaves.keys().all(|column| {
                self.binding
                    .indexed_column_ordinals()
                    .binary_search(column)
                    .is_ok()
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::datasource) async fn try_select(
        &self,
        row_group: usize,
        row_count: usize,
        predicate: &ScanPredicate,
        table_schema: &Schema,
        file_schema: &Schema,
        parquet_row_group: &RowGroupMetaData,
        projected_columns: &[usize],
        context: &QueryContext,
    ) -> Result<Option<SidecarSelection>> {
        let Some(leaves) = compile_predicate(predicate, table_schema, file_schema) else {
            context.metrics.add_native_predicate_sidecar_fallback();
            return Ok(None);
        };
        if leaves.keys().any(|column| {
            self.binding
                .indexed_column_ordinals()
                .binary_search(column)
                .is_err()
        }) {
            context.metrics.add_native_predicate_sidecar_fallback();
            return Ok(None);
        }
        let Some(loaded) = self.loaded_index(context).await? else {
            context.metrics.add_native_predicate_sidecar_fallback();
            return Ok(None);
        };
        let row_group = u32::try_from(row_group).map_err(|_| {
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
            != u32::try_from(row_count).ok()
        {
            return Err(self.corrupt("row-group row count does not match Parquet metadata"));
        }

        let mut entries = Vec::with_capacity(leaves.len());
        let mut total_block_bytes = 0_usize;
        for (&column, predicates) in &leaves {
            let Some(entry) = loaded.index.entry(row_group, column) else {
                context.metrics.add_native_predicate_sidecar_fallback();
                return Ok(None);
            };
            if usize::try_from(entry.row_count()).ok() != Some(row_count) {
                return Err(self.corrupt("predicate block row count does not match its row group"));
            }
            let length = usize::try_from(entry.length())
                .map_err(|_| self.corrupt("predicate block length exceeds this platform"))?;
            total_block_bytes = total_block_bytes
                .checked_add(length)
                .ok_or_else(|| self.corrupt("predicate block bytes exceed this platform"))?;
            entries.push((column, entry, predicates));
        }
        let sidecar_bytes = u64::try_from(total_block_bytes).ok();
        let avoided_bytes = parquet_predicate_only_bytes(
            parquet_row_group,
            leaves.keys().copied(),
            projected_columns,
        );
        // Projected payload still has to pass through Parquet, so require a
        // 2:1 compressed-byte advantage there. A metadata-only scan can avoid
        // decoding every Parquet predicate column and admits a sidecar up to
        // twice their compressed size.
        let cost_admitted = sidecar_bytes
            .zip(avoided_bytes)
            .is_some_and(|(sidecar, avoided)| {
                if projected_columns.is_empty() {
                    sidecar <= avoided.saturating_mul(2)
                } else {
                    sidecar.saturating_mul(2) <= avoided
                }
            });
        if !cost_admitted {
            context.metrics.add_native_predicate_sidecar_fallback();
            return Ok(None);
        }

        // Rust's Vec<bool> stores one byte per row. Two selections can coexist
        // while intersecting predicates from different columns. All required
        // block ranges are retained by one batched local I/O job.
        let selection_bytes = row_count;
        let workspace_bytes = total_block_bytes.saturating_add(selection_bytes.saturating_mul(2));
        let Ok(mut lease) = context.memory.try_reserve(workspace_bytes) else {
            context.metrics.add_native_predicate_sidecar_fallback();
            return Ok(None);
        };
        context.metrics.observe_memory(context.memory.used());

        let snapshot = context.object_snapshot(self.source.uri())?;
        let reader = SnapshotParquetReader::new(
            &self.source,
            snapshot,
            Some(QueryIo::for_native_predicate_sidecar(
                context.control.clone(),
                context.metrics.clone(),
            )),
        );
        let ranges = entries
            .iter()
            .map(|(_, entry, _)| {
                let end = entry
                    .offset()
                    .checked_add(entry.length())
                    .ok_or_else(|| self.corrupt("predicate block range overflows u64"))?;
                Ok(entry.offset()..end)
            })
            .collect::<Result<Vec<_>>>()?;
        let blocks = reader.query_ranges(ranges).await?;
        if blocks.len() != entries.len() {
            return Err(self.corrupt("predicate block range read returned an invalid count"));
        }
        let mut selected: Option<Vec<bool>> = None;
        for ((column, entry, predicates), bytes) in entries.into_iter().zip(blocks) {
            context.check_cancelled()?;
            if u64::try_from(bytes.len()).ok() != Some(entry.length())
                || format!("{:x}", Sha256::digest(&bytes)) != entry.sha256()
            {
                return Err(self.corrupt("predicate block checksum or length mismatch"));
            }
            let file_column = usize::try_from(column)
                .map_err(|_| self.corrupt("predicate block column exceeds this platform"))?;
            let data_type = file_schema
                .fields()
                .get(file_column)
                .ok_or_else(|| self.corrupt("predicate block column is outside the file schema"))?
                .data_type();
            let current =
                NativePredicateBlock::evaluate_all_bytes_for_type(&bytes, data_type, predicates)
                    .map_err(|error| self.corrupt(error.to_string()))?;
            if current.len() != row_count {
                return Err(self.corrupt("predicate block decoded an invalid row count"));
            }
            match &mut selected {
                Some(selected) => selected
                    .iter_mut()
                    .zip(current)
                    .for_each(|(selected, current)| *selected &= current),
                None => selected = Some(current),
            }
        }
        let selected = selected.ok_or_else(|| self.corrupt("predicate has no sidecar leaves"))?;
        let selected_rows = selected.iter().filter(|selected| **selected).count();
        let selector_count = selected
            .windows(2)
            .filter(|pair| pair[0] != pair[1])
            .count()
            .saturating_add(usize::from(!selected.is_empty()));
        let downstream_selector_bytes = row_count
            .checked_mul(size_of::<RowSelector>())
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(|| self.corrupt("predicate selection size overflows this platform"))?;
        if lease
            .try_resize(selection_bytes.saturating_add(downstream_selector_bytes))
            .is_err()
        {
            context.metrics.add_native_predicate_sidecar_fallback();
            return Ok(None);
        }
        context.metrics.record_native_predicate_sidecar_selection(
            u64::try_from(row_count).unwrap_or(u64::MAX),
            u64::try_from(selected_rows).unwrap_or(u64::MAX),
        );

        let mut selectors = Vec::new();
        if selectors.try_reserve_exact(selector_count).is_err() {
            context.metrics.add_native_predicate_sidecar_fallback();
            return Ok(None);
        }
        append_selectors(&mut selectors, &selected);
        drop(selected);
        lease.try_resize(downstream_selector_bytes)?;
        context.metrics.add_native_predicate_sidecar_exact_bypass();
        Ok(Some(SidecarSelection {
            selection: RowSelection::from(selectors),
            selected_rows,
            lease,
        }))
    }

    async fn loaded_index(&self, context: &QueryContext) -> Result<Option<Arc<LoadedIndex>>> {
        self.index
            .get_or_try_init(|| self.load_index(context))
            .await
            .cloned()
    }

    async fn load_index(&self, context: &QueryContext) -> Result<Option<Arc<LoadedIndex>>> {
        let Ok(_header_memory) = context.memory.try_reserve(SIDECAR_HEADER_BYTES) else {
            return Ok(None);
        };
        let snapshot = context.object_snapshot(self.source.uri())?;
        let reader = SnapshotParquetReader::new(
            &self.source,
            snapshot.clone(),
            Some(QueryIo::for_native_predicate_sidecar(
                context.control.clone(),
                context.metrics.clone(),
            )),
        );
        let header = reader.query_range(0..SIDECAR_HEADER_BYTES as u64).await?;
        let prefix_len = NativePredicateSidecarIndex::metadata_prefix_len(&header)
            .map_err(|error| self.corrupt(error.to_string()))?;
        if prefix_len < SIDECAR_HEADER_BYTES
            || u64::try_from(prefix_len)
                .ok()
                .is_none_or(|length| length > snapshot.size)
        {
            return Err(self.corrupt("metadata prefix is outside the sidecar object"));
        }

        let prefix_workspace = prefix_len.saturating_mul(3);
        let Ok(mut index_memory) = context.memory.try_reserve(prefix_workspace) else {
            return Ok(None);
        };
        let mut prefix = Vec::new();
        prefix.try_reserve_exact(prefix_len).map_err(|error| {
            Error::ResourceExhausted(format!(
                "Native predicate sidecar metadata for {} cannot allocate {prefix_len} bytes: {error}",
                self.source.uri()
            ))
        })?;
        prefix.extend_from_slice(&header);
        if prefix_len > SIDECAR_HEADER_BYTES {
            let remainder = reader
                .query_range(SIDECAR_HEADER_BYTES as u64..prefix_len as u64)
                .await?;
            prefix.extend_from_slice(&remainder);
        }
        context.check_cancelled()?;
        let index = NativePredicateSidecarIndex::from_metadata(&prefix, snapshot.size)
            .map_err(|error| self.corrupt(error.to_string()))?;
        self.validate_index(&index)?;
        // The parsed index retains less than twice its fixed metadata prefix;
        // keep that conservative lease after dropping the encoded prefix.
        drop(prefix);
        index_memory.try_resize(prefix_len.saturating_mul(2))?;
        Ok(Some(Arc::new(LoadedIndex {
            index,
            _memory: index_memory,
        })))
    }

    fn validate_index(&self, index: &NativePredicateSidecarIndex) -> Result<()> {
        if self.binding.format_version() != 1
            || self.binding.sidecar_sha256().len() != 64
            || index.schema_fingerprint() != self.binding.schema_fingerprint()
            || index.segment_sha256() != self.binding.segment_sha256()
            || index.segment_rows() != self.binding.segment_rows()
            || index.row_group_rows().len() as u64 != self.binding.row_group_count()
            || index.indexed_column_ordinals() != self.binding.indexed_column_ordinals()
        {
            return Err(self.corrupt("metadata bindings do not match the Native manifest"));
        }
        Ok(())
    }

    fn corrupt(&self, message: impl fmt::Display) -> Error {
        Error::Execution(format!(
            "invalid Native predicate sidecar '{}': {message}",
            self.source.uri()
        ))
    }
}

fn parquet_predicate_only_bytes(
    row_group: &RowGroupMetaData,
    predicate_columns: impl IntoIterator<Item = u32>,
    projected_columns: &[usize],
) -> Option<u64> {
    let predicate_columns = predicate_columns
        .into_iter()
        .map(usize::try_from)
        .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()
        .ok()?;
    let schema = row_group.schema_descr();
    let mut bytes = 0_u64;
    for (leaf, column) in row_group.columns().iter().enumerate() {
        let root = schema.get_column_root_idx(leaf);
        if !predicate_columns.contains(&root) || projected_columns.contains(&root) {
            continue;
        }
        bytes = bytes.checked_add(u64::try_from(column.compressed_size()).ok()?)?;
    }
    (bytes != 0).then_some(bytes)
}

fn compile_predicate(
    predicate: &ScanPredicate,
    table_schema: &Schema,
    file_schema: &Schema,
) -> Option<BTreeMap<u32, Vec<NativePredicate>>> {
    let mut leaves = BTreeMap::new();
    compile_into(predicate, table_schema, file_schema, &mut leaves)?;
    (!leaves.is_empty()).then_some(leaves)
}

fn compile_into(
    predicate: &ScanPredicate,
    table_schema: &Schema,
    file_schema: &Schema,
    leaves: &mut BTreeMap<u32, Vec<NativePredicate>>,
) -> Option<()> {
    match predicate {
        ScanPredicate::And(predicates) => {
            for predicate in predicates {
                compile_into(predicate, table_schema, file_schema, leaves)?;
            }
        }
        ScanPredicate::Comparison { column, op, value } => {
            let table_field = table_schema.fields().get(*column)?;
            let file_column = file_schema.index_of(table_field.name()).ok()?;
            let field = file_schema.fields().get(file_column)?;
            let value = narrow_value(field.data_type(), value)?;
            leaves
                .entry(u32::try_from(file_column).ok()?)
                .or_default()
                .push(NativePredicate::Compare {
                    op: comparison(*op),
                    value,
                });
        }
        ScanPredicate::IsNull { column } | ScanPredicate::IsNotNull { column } => {
            let table_field = table_schema.fields().get(*column)?;
            let file_column = file_schema.index_of(table_field.name()).ok()?;
            let field = file_schema.fields().get(file_column)?;
            eligible_type(field.data_type())?;
            leaves
                .entry(u32::try_from(file_column).ok()?)
                .or_default()
                .push(if matches!(predicate, ScanPredicate::IsNull { .. }) {
                    NativePredicate::IsNull
                } else {
                    NativePredicate::IsNotNull
                });
        }
        ScanPredicate::Or(_) => return None,
    }
    Some(())
}

fn eligible_type(data_type: &DataType) -> Option<()> {
    match data_type {
        DataType::Int64 | DataType::Date32 => Some(()),
        DataType::Decimal128(precision, _) if *precision <= 18 => Some(()),
        _ => None,
    }
}

fn narrow_value(data_type: &DataType, value: &PredicateValue) -> Option<i64> {
    match (data_type, value) {
        (DataType::Int64, PredicateValue::Int64(value)) => Some(*value),
        (DataType::Date32, PredicateValue::Date32(value)) => Some(i64::from(*value)),
        (
            DataType::Decimal128(precision, scale),
            PredicateValue::Decimal128 {
                value,
                precision: _,
                scale: value_scale,
            },
        ) if *precision <= 18 && scale == value_scale => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn comparison(op: ComparisonOp) -> NativePredicateComparisonOp {
    match op {
        ComparisonOp::Eq => NativePredicateComparisonOp::Equal,
        ComparisonOp::NotEq => NativePredicateComparisonOp::NotEqual,
        ComparisonOp::Lt => NativePredicateComparisonOp::Less,
        ComparisonOp::LtEq => NativePredicateComparisonOp::LessOrEqual,
        ComparisonOp::Gt => NativePredicateComparisonOp::Greater,
        ComparisonOp::GtEq => NativePredicateComparisonOp::GreaterOrEqual,
    }
}

fn append_selectors(selectors: &mut Vec<RowSelector>, selected: &[bool]) {
    let Some((&first, rest)) = selected.split_first() else {
        return;
    };
    let mut current = first;
    let mut count = 1_usize;
    for &value in rest {
        if value == current {
            count += 1;
        } else {
            selectors.push(if current {
                RowSelector::select(count)
            } else {
                RowSelector::skip(count)
            });
            current = value;
            count = 1;
        }
    }
    selectors.push(if current {
        RowSelector::select(count)
    } else {
        RowSelector::skip(count)
    });
}

#[cfg(test)]
mod tests {
    use super::{append_selectors, compile_predicate};
    use crate::datasource::{ComparisonOp, PredicateValue, ScanPredicate};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::RowSelector;

    #[test]
    fn compiles_only_supported_conjunctive_leaves() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("day", DataType::Date32, true),
        ]);
        let predicate = ScanPredicate::And(vec![
            ScanPredicate::Comparison {
                column: 0,
                op: ComparisonOp::Gt,
                value: PredicateValue::Int64(7),
            },
            ScanPredicate::IsNotNull { column: 1 },
        ]);
        assert_eq!(
            compile_predicate(&predicate, &schema, &schema)
                .unwrap()
                .len(),
            2
        );
        assert!(compile_predicate(&ScanPredicate::Or(vec![predicate]), &schema, &schema).is_none());
    }

    #[test]
    fn selection_runs_preserve_full_row_group_coordinates() {
        let mut selectors = Vec::new();
        append_selectors(&mut selectors, &[false, true, true, false]);
        assert_eq!(
            selectors,
            vec![
                RowSelector::skip(1),
                RowSelector::select(2),
                RowSelector::skip(1),
            ]
        );
    }
}

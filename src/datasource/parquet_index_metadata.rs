use std::sync::Arc;

use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    file::metadata::{PageIndexPolicy, ParquetMetaDataReader},
};

use super::{
    MetadataCache,
    metadata_cache::metadata_weight,
    parquet_metadata::ParquetMetadata,
    parquet_pruning_budget::{MAX_FILE_PAGE_INDEX_BYTES, PruningBudget},
    parquet_reader::{QueryIo, SnapshotParquetReader},
};
use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext},
    storage::{ObjectSnapshot, ObjectSource},
};

const INDEX_DECODE_EXPANSION: usize = 4;
const INDEX_FIXED_OVERHEAD: usize = 64 * 1024;

pub(super) async fn load_page_index_metadata(
    file: &ObjectSource,
    snapshot: &ObjectSnapshot,
    footer: ParquetMetadata,
    context: &QueryContext,
    cache: &MetadataCache,
    budget: &PruningBudget,
) -> Result<ParquetMetadata> {
    let (footer_metadata, mut memory, previous_lease) = footer.into_parts()?;
    debug_assert!(previous_lease.is_none());
    let footer_weight = metadata_weight(file, snapshot, &footer_metadata);

    let Some(encoded_span) = page_index_span(file.uri(), &footer_metadata)? else {
        return Ok(ParquetMetadata::new(
            footer_metadata,
            memory,
            previous_lease,
        ));
    };
    let estimated_index = encoded_span
        .saturating_mul(INDEX_DECODE_EXPANSION)
        .saturating_add(INDEX_FIXED_OVERHEAD);
    if estimated_index > MAX_FILE_PAGE_INDEX_BYTES {
        context.metrics.add_parquet_pruning_budget_skip();
        return Ok(ParquetMetadata::new(
            footer_metadata,
            memory,
            previous_lease,
        ));
    }

    if let Some(indexed) = cache.get_page_index(file, snapshot) {
        let indexed_weight = metadata_weight(file, snapshot, &indexed);
        let index_weight = indexed_weight.saturating_sub(footer_weight);
        if index_weight > MAX_FILE_PAGE_INDEX_BYTES {
            context.metrics.add_parquet_pruning_budget_skip();
            return Ok(ParquetMetadata::new(
                footer_metadata,
                memory,
                previous_lease,
            ));
        }
        let Some(lease) = budget.try_reserve(index_weight) else {
            context.metrics.add_parquet_pruning_budget_skip();
            return Ok(ParquetMetadata::new(
                footer_metadata,
                memory,
                previous_lease,
            ));
        };
        if !resize_optional_memory(&mut memory, indexed_weight) {
            context.metrics.add_parquet_pruning_budget_skip();
            return Ok(ParquetMetadata::new(
                footer_metadata,
                memory,
                previous_lease,
            ));
        }
        return Ok(ParquetMetadata::new(indexed, memory, Some(lease)));
    }

    let Some(mut lease) = budget.try_reserve(estimated_index) else {
        context.metrics.add_parquet_pruning_budget_skip();
        return Ok(ParquetMetadata::new(
            footer_metadata,
            memory,
            previous_lease,
        ));
    };
    if !resize_optional_memory(&mut memory, footer_weight.saturating_add(estimated_index)) {
        context.metrics.add_parquet_pruning_budget_skip();
        return Ok(ParquetMetadata::new(
            footer_metadata,
            memory,
            previous_lease,
        ));
    }

    let parquet_metadata = footer_metadata.metadata().as_ref().clone();
    let query = QueryIo::for_page_index(context.control.clone(), context.metrics.clone());
    let mut reader = SnapshotParquetReader::new(file, snapshot.clone(), Some(query));
    let mut loader = ParquetMetaDataReader::new_with_metadata(parquet_metadata)
        .with_page_index_policy(PageIndexPolicy::Optional);
    loader.load_page_index(&mut reader).await.map_err(|error| {
        Error::Execution(format!(
            "invalid Parquet page index for '{}': {error}",
            file.uri()
        ))
    })?;
    let indexed = ArrowReaderMetadata::try_new(
        Arc::new(loader.finish().map_err(|error| {
            Error::Execution(format!(
                "invalid Parquet page index for '{}': {error}",
                file.uri()
            ))
        })?),
        ArrowReaderOptions::new(),
    )?;
    let indexed_weight = metadata_weight(file, snapshot, &indexed);
    let index_weight = indexed_weight.saturating_sub(footer_weight);
    if index_weight > MAX_FILE_PAGE_INDEX_BYTES
        || !lease.try_resize(index_weight)
        || !resize_optional_memory(&mut memory, indexed_weight)
    {
        context.metrics.add_parquet_pruning_budget_skip();
        let _ = resize_optional_memory(&mut memory, footer_weight);
        return Ok(ParquetMetadata::new(
            footer_metadata,
            memory,
            previous_lease,
        ));
    }

    cache.insert_page_index(file, snapshot, indexed.clone());
    Ok(ParquetMetadata::new(indexed, memory, Some(lease)))
}

fn resize_optional_memory(reservation: &mut Option<MemoryReservation>, bytes: usize) -> bool {
    reservation
        .as_mut()
        .is_some_and(|reservation| reservation.try_resize(bytes).is_ok())
}

fn page_index_span(uri: &str, metadata: &ArrowReaderMetadata) -> Result<Option<usize>> {
    let mut first = u64::MAX;
    let mut last = 0_u64;
    let mut has_column_index = false;
    let mut has_offset_index = false;
    for (row_group, group) in metadata.metadata().row_groups().iter().enumerate() {
        for column in group.columns() {
            let column_name = column.column_descr().path().string();
            if let Some((start, end)) = index_range(
                uri,
                row_group,
                &column_name,
                column.column_index_offset(),
                column.column_index_length(),
                "column index",
            )? {
                has_column_index = true;
                first = first.min(start);
                last = last.max(end);
            }
            if let Some((start, end)) = index_range(
                uri,
                row_group,
                &column_name,
                column.offset_index_offset(),
                column.offset_index_length(),
                "offset index",
            )? {
                has_offset_index = true;
                first = first.min(start);
                last = last.max(end);
            }
        }
    }
    if !has_column_index || !has_offset_index {
        return Ok(None);
    }
    usize::try_from(last.saturating_sub(first))
        .map(Some)
        .map_err(|_| {
            Error::ResourceExhausted(format!(
                "Parquet page index span for '{uri}' exceeds this platform's address space"
            ))
        })
}

fn index_range(
    uri: &str,
    row_group: usize,
    column: &str,
    offset: Option<i64>,
    length: Option<i32>,
    kind: &str,
) -> Result<Option<(u64, u64)>> {
    match (offset, length) {
        (None, None) => Ok(None),
        (Some(offset), Some(length)) if offset >= 0 && length > 0 => {
            let start = u64::try_from(offset).map_err(|_| {
                index_error(
                    uri,
                    row_group,
                    column,
                    kind,
                    &format!("invalid offset {offset}"),
                )
            })?;
            let length = u64::try_from(length).map_err(|_| {
                index_error(
                    uri,
                    row_group,
                    column,
                    kind,
                    &format!("invalid length {length}"),
                )
            })?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| index_error(uri, row_group, column, kind, "range overflows u64"))?;
            Ok(Some((start, end)))
        }
        (Some(offset), Some(length)) => Err(index_error(
            uri,
            row_group,
            column,
            kind,
            &format!("invalid offset {offset} or length {length}"),
        )),
        _ => Err(index_error(
            uri,
            row_group,
            column,
            kind,
            "offset and length must both be present",
        )),
    }
}

fn index_error(uri: &str, row_group: usize, column: &str, kind: &str, reason: &str) -> Error {
    Error::Execution(format!(
        "invalid Parquet {kind} in '{uri}', row group {row_group}, column '{column}': {reason}"
    ))
}

#[cfg(test)]
mod tests {
    use super::index_range;

    #[test]
    fn malformed_advertised_index_includes_uri_and_kind() {
        let uri = "s3://bucket/corrupt.parquet";
        let negative = index_range(uri, 3, "id", Some(-1), Some(10), "column index")
            .unwrap_err()
            .to_string();
        assert!(negative.contains(uri), "{negative}");
        assert!(negative.contains("column index"), "{negative}");
        assert!(negative.contains("row group 3"), "{negative}");
        assert!(negative.contains("id"), "{negative}");

        let incomplete = index_range(uri, 3, "id", Some(10), None, "offset index")
            .unwrap_err()
            .to_string();
        assert!(incomplete.contains(uri), "{incomplete}");
        assert!(incomplete.contains("offset index"), "{incomplete}");
    }
}

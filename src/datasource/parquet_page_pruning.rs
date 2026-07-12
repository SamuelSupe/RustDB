use std::ops::Range;

use arrow::datatypes::{DataType, Schema};
use parquet::{
    arrow::arrow_reader::RowSelection,
    file::{metadata::ParquetMetaData, page_index::column_index::ColumnIndexMetaData},
};

use super::{ScanPredicate, parquet_page_values::comparison_excludes};
use crate::{Error, Result};

pub(super) struct PagePruning {
    pub(super) selection: RowSelection,
    pub(super) selected_rows: usize,
    pub(super) pages_pruned: u64,
    pub(super) rows_pruned: u64,
}

pub(super) fn supports_page_index(predicate: Option<&ScanPredicate>) -> bool {
    predicate.is_some_and(|predicate| match predicate {
        ScanPredicate::And(predicates) => predicates
            .iter()
            .any(|predicate| supports_page_index(Some(predicate))),
        ScanPredicate::Comparison { .. }
        | ScanPredicate::IsNull { .. }
        | ScanPredicate::IsNotNull { .. } => true,
    })
}

pub(super) fn prune_pages(
    uri: &str,
    metadata: &ParquetMetaData,
    file_schema: &Schema,
    table_schema: &Schema,
    row_group: usize,
    predicate: Option<&ScanPredicate>,
) -> Result<Option<PagePruning>> {
    let Some(predicate) = predicate else {
        return Ok(None);
    };
    let rows = usize::try_from(metadata.row_group(row_group).num_rows())
        .map_err(|_| page_error(uri, row_group, "*", "row count does not fit usize"))?;
    #[allow(clippy::single_range_in_vec_init)]
    let mut ranges = vec![0..rows];
    let mut used_index = false;
    let mut pages_pruned = 0_u64;
    for atom in atoms(predicate) {
        let Some(atom_ranges) = atom_ranges(
            uri,
            metadata,
            file_schema,
            table_schema,
            row_group,
            rows,
            atom,
        )?
        else {
            continue;
        };
        used_index = true;
        pages_pruned = pages_pruned.max(atom_ranges.pages_pruned);
        ranges = intersect_ranges(&ranges, &atom_ranges.ranges);
        if ranges.is_empty() {
            break;
        }
    }
    if !used_index {
        return Ok(None);
    }
    let selected_rows = ranges.iter().fold(0_usize, |total, range| {
        total.saturating_add(range.end - range.start)
    });
    Ok(Some(PagePruning {
        selection: RowSelection::from_consecutive_ranges(ranges.into_iter(), rows),
        selected_rows,
        pages_pruned,
        rows_pruned: u64::try_from(rows.saturating_sub(selected_rows)).unwrap_or(u64::MAX),
    }))
}

struct AtomRanges {
    ranges: Vec<Range<usize>>,
    pages_pruned: u64,
}

#[allow(clippy::too_many_arguments)]
fn atom_ranges(
    uri: &str,
    metadata: &ParquetMetaData,
    file_schema: &Schema,
    table_schema: &Schema,
    row_group: usize,
    rows: usize,
    predicate: &ScanPredicate,
) -> Result<Option<AtomRanges>> {
    let table_column = match predicate {
        ScanPredicate::Comparison { column, .. }
        | ScanPredicate::IsNull { column }
        | ScanPredicate::IsNotNull { column } => *column,
        ScanPredicate::And(_) => return Ok(None),
    };
    let Some((file_column, leaf)) = column_leaf(metadata, file_schema, table_schema, table_column)
    else {
        return Ok(None);
    };
    let column_name = file_schema.field(file_column).name();
    let Some(index) = metadata
        .column_index()
        .and_then(|groups| groups.get(row_group))
        .and_then(|columns| columns.get(leaf))
    else {
        return Ok(None);
    };
    if matches!(index, ColumnIndexMetaData::NONE) {
        return Ok(None);
    }
    let Some(offsets) = metadata
        .offset_index()
        .and_then(|groups| groups.get(row_group))
        .and_then(|columns| columns.get(leaf))
    else {
        return Ok(None);
    };
    let locations = offsets.page_locations();
    let page_count = usize::try_from(index.num_pages())
        .map_err(|_| page_error(uri, row_group, column_name, "page count does not fit usize"))?;
    if locations.len() != page_count {
        return Err(page_error(
            uri,
            row_group,
            column_name,
            &format!(
                "column index has {page_count} pages but offset index has {}",
                locations.len()
            ),
        ));
    }
    if page_count == 0 && rows != 0 {
        return Err(page_error(
            uri,
            row_group,
            column_name,
            "non-empty row group has an empty page index",
        ));
    }
    validate_locations(uri, row_group, column_name, locations, rows)?;

    let mut ranges = Vec::with_capacity(page_count);
    let mut pruned = 0_u64;
    for page in 0..page_count {
        let start = usize::try_from(locations[page].first_row_index)
            .map_err(|_| page_error(uri, row_group, column_name, "negative first row index"))?;
        let end = locations
            .get(page + 1)
            .map_or(rows, |location| location.first_row_index as usize);
        if page_excluded(
            index,
            file_schema.field(file_column).data_type(),
            predicate,
            page,
            end - start,
        ) {
            pruned = pruned.saturating_add(1);
        } else {
            push_range(&mut ranges, start..end);
        }
    }
    Ok(Some(AtomRanges {
        ranges,
        pages_pruned: pruned,
    }))
}

fn atoms(predicate: &ScanPredicate) -> Vec<&ScanPredicate> {
    let mut output = Vec::new();
    collect_atoms(predicate, &mut output);
    output
}

fn collect_atoms<'a>(predicate: &'a ScanPredicate, output: &mut Vec<&'a ScanPredicate>) {
    match predicate {
        ScanPredicate::And(predicates) => {
            for predicate in predicates {
                collect_atoms(predicate, output);
            }
        }
        _ => output.push(predicate),
    }
}

fn column_leaf(
    metadata: &ParquetMetaData,
    file_schema: &Schema,
    table_schema: &Schema,
    table_column: usize,
) -> Option<(usize, usize)> {
    let field = table_schema.fields().get(table_column)?;
    let file_column = file_schema.index_of(field.name()).ok()?;
    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut leaves = (0..parquet_schema.num_columns())
        .filter(|leaf| parquet_schema.get_column_root_idx(*leaf) == file_column);
    let leaf = leaves.next()?;
    if leaves.next().is_some() {
        return None;
    }
    Some((file_column, leaf))
}

fn validate_locations(
    uri: &str,
    row_group: usize,
    column: &str,
    locations: &[parquet::file::page_index::offset_index::PageLocation],
    rows: usize,
) -> Result<()> {
    let mut previous = None;
    for location in locations {
        if location.offset < 0 || location.compressed_page_size <= 0 {
            return Err(page_error(
                uri,
                row_group,
                column,
                "page offset or compressed size is invalid",
            ));
        }
        let first = usize::try_from(location.first_row_index)
            .map_err(|_| page_error(uri, row_group, column, "negative first row index"))?;
        if first >= rows || previous.is_some_and(|previous| first <= previous) {
            return Err(page_error(
                uri,
                row_group,
                column,
                "page first row indexes are out of bounds or not strictly increasing",
            ));
        }
        previous = Some(first);
    }
    if !locations.is_empty() && locations[0].first_row_index != 0 {
        return Err(page_error(
            uri,
            row_group,
            column,
            "first page does not begin at row zero",
        ));
    }
    Ok(())
}

fn page_excluded(
    index: &ColumnIndexMetaData,
    data_type: &DataType,
    predicate: &ScanPredicate,
    page: usize,
    page_rows: usize,
) -> bool {
    match predicate {
        ScanPredicate::IsNull { .. } => index.null_count(page) == Some(0),
        ScanPredicate::IsNotNull { .. } => {
            index.is_null_page(page) || index.null_count(page) == i64::try_from(page_rows).ok()
        }
        ScanPredicate::Comparison { op, value, .. } => {
            index.is_null_page(page) || comparison_excludes(index, data_type, *op, value, page)
        }
        ScanPredicate::And(_) => false,
    }
}

fn push_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    if let Some(last) = ranges.last_mut()
        && last.end == range.start
    {
        last.end = range.end;
        return;
    }
    ranges.push(range);
}

fn intersect_ranges(left: &[Range<usize>], right: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut output = Vec::new();
    let (mut left_index, mut right_index) = (0, 0);
    while let (Some(left), Some(right)) = (left.get(left_index), right.get(right_index)) {
        let start = left.start.max(right.start);
        let end = left.end.min(right.end);
        if start < end {
            push_range(&mut output, start..end);
        }
        if left.end <= right.end {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    output
}

fn page_error(uri: &str, row_group: usize, column: &str, reason: &str) -> Error {
    Error::Execution(format!(
        "invalid Parquet page index in '{uri}', row group {row_group}, column '{column}': {reason}"
    ))
}

#[cfg(test)]
mod tests {
    use super::{intersect_ranges, page_error};

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn intersects_page_ranges_without_row_bitmaps() {
        assert_eq!(
            intersect_ranges(&[0..10, 20..30], &[5..25]),
            vec![5..10, 20..25]
        );
    }

    #[test]
    fn malformed_page_index_error_identifies_object_group_and_column() {
        let message =
            page_error("s3://bucket/corrupt.parquet", 7, "event_time", "bad offset").to_string();
        assert!(message.contains("s3://bucket/corrupt.parquet"), "{message}");
        assert!(message.contains("row group 7"), "{message}");
        assert!(message.contains("event_time"), "{message}");
    }
}

use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    file::metadata::RowGroupMetaData,
};

use crate::{Error, Result, storage::NativePredicateSidecarIndex};

struct ProjectionBlock {
    column: u32,
    offset: u64,
    length: u64,
    sha256: String,
}

pub(super) struct ProjectionPlan {
    blocks: Vec<ProjectionBlock>,
    pub(super) total_bytes: usize,
}

impl ProjectionPlan {
    pub(super) fn ranges(&self) -> impl Iterator<Item = std::ops::Range<u64>> + '_ {
        self.blocks
            .iter()
            .map(|block| block.offset..block.offset + block.length)
    }

    pub(super) fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub(super) fn blocks(&self) -> impl Iterator<Item = (u64, &str)> + '_ {
        self.blocks
            .iter()
            .map(|block| (block.length, block.sha256.as_str()))
    }

    pub(super) fn bytes<'a>(&self, column: u32, blocks: &'a [bytes::Bytes]) -> Result<&'a [u8]> {
        let index = self
            .blocks
            .binary_search_by_key(&column, |block| block.column)
            .map_err(|_| {
                Error::Internal("Native sidecar projection block is missing".to_owned())
            })?;
        Ok(blocks[index].as_ref())
    }
}

pub(super) fn projection_plan(
    index: &NativePredicateSidecarIndex,
    row_group: u32,
    row_count: usize,
    required: &[u32],
) -> std::result::Result<Option<ProjectionPlan>, String> {
    let mut blocks = Vec::with_capacity(required.len());
    let mut total_bytes = 0_usize;
    for &column in required {
        let Some(entry) = index.entry(row_group, column) else {
            return Ok(None);
        };
        if usize::try_from(entry.row_count()).ok() != Some(row_count) {
            return Err("projection block row count mismatch".to_owned());
        }
        let length = usize::try_from(entry.length())
            .map_err(|_| "projection block is too large".to_owned())?;
        total_bytes = total_bytes
            .checked_add(length)
            .ok_or_else(|| "projection bytes exceed this platform".to_owned())?;
        blocks.push(ProjectionBlock {
            column,
            offset: entry.offset(),
            length: entry.length(),
            sha256: entry.sha256(),
        });
    }
    Ok(Some(ProjectionPlan {
        blocks,
        total_bytes,
    }))
}

pub(super) fn parquet_required_bytes(
    row_group: &RowGroupMetaData,
    required: &[u32],
) -> Option<u64> {
    let schema = row_group.schema_descr();
    row_group
        .columns()
        .iter()
        .enumerate()
        .filter(|(leaf, _)| {
            u32::try_from(schema.get_column_root_idx(*leaf))
                .ok()
                .is_some_and(|root| required.binary_search(&root).is_ok())
        })
        .try_fold(0_u64, |bytes, (_, column)| {
            bytes.checked_add(u64::try_from(column.compressed_size()).ok()?)
        })
        .filter(|bytes| *bytes != 0)
}

pub(super) fn selection_mask(
    selection: Option<&RowSelection>,
    row_count: usize,
) -> Result<Vec<bool>> {
    let Some(selection) = selection else {
        return Ok(vec![true; row_count]);
    };
    let selectors = Vec::<RowSelector>::from(selection.clone());
    if selectors.iter().try_fold(0_usize, |rows, selector| {
        rows.checked_add(selector.row_count)
    }) != Some(row_count)
    {
        return Err(Error::Internal(
            "Native sidecar input RowSelection has invalid coordinates".to_owned(),
        ));
    }
    let mut selected = Vec::with_capacity(row_count);
    for selector in selectors {
        selected.extend(std::iter::repeat_n(!selector.skip, selector.row_count));
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

    use super::selection_mask;

    #[test]
    fn expands_page_selection_in_chunk_coordinates() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(2),
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(3),
        ]);
        assert_eq!(
            selection_mask(Some(&selection), 8).unwrap(),
            [false, false, true, true, false, true, true, true]
        );
        assert!(selection_mask(Some(&selection), 7).is_err());
        assert_eq!(selection_mask(None, 3).unwrap(), [true, true, true]);
    }
}

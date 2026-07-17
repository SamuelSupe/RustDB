use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

use crate::{Error, Result, runtime::MemoryReservation};

use super::super::parquet_predicate_cache::PredicateCachePlan;

const MAX_ROW_GROUPS_PER_READER: usize = 4;

pub(super) fn size(
    fixed_files: bool,
    limit: Option<usize>,
    candidate_groups: usize,
    target_lanes: usize,
) -> usize {
    if !fixed_files || limit.is_some() || candidate_groups < 2 {
        return 1;
    }
    (candidate_groups / target_lanes.max(1)).clamp(1, MAX_ROW_GROUPS_PER_READER)
}

pub(super) struct RowGroupChunk {
    capacity: usize,
    row_groups: Vec<usize>,
    selected_rows: usize,
    selectors: Option<Vec<RowSelector>>,
    predicate_cache: PredicateCachePlan,
    apply_row_filter: Option<bool>,
    sidecar_selection_leases: Vec<MemoryReservation>,
}

impl RowGroupChunk {
    pub(super) fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 1);
        Self {
            capacity,
            row_groups: Vec::with_capacity(capacity),
            selected_rows: 0,
            selectors: None,
            predicate_cache: PredicateCachePlan::default(),
            apply_row_filter: None,
            sidecar_selection_leases: Vec::new(),
        }
    }

    pub(super) fn push(
        &mut self,
        row_group: usize,
        row_count: usize,
        selection: Option<RowSelection>,
        predicate_cache: PredicateCachePlan,
        apply_row_filter: bool,
        sidecar_selection_lease: Option<MemoryReservation>,
    ) -> Result<()> {
        if self.is_full() {
            return Err(Error::Internal(
                "Parquet row-group chunk exceeded its bounded capacity".to_owned(),
            ));
        }
        if !self.can_accept(apply_row_filter) {
            return Err(Error::Internal(
                "Parquet row-group chunk mixed RowFilter application modes".to_owned(),
            ));
        }
        let selection = selection.map(Vec::<RowSelector>::from);
        if let Some(selectors) = &selection {
            let described_rows = selectors.iter().try_fold(0usize, |rows, selector| {
                rows.checked_add(selector.row_count)
            });
            if described_rows != Some(row_count) {
                return Err(Error::Internal(format!(
                    "Parquet row selection describes {described_rows:?} rows for a {row_count}-row group"
                )));
            }
        }
        self.row_groups.push(row_group);
        match selection {
            Some(selection) => {
                let selectors = self.selectors.get_or_insert_with(|| {
                    if self.selected_rows == 0 {
                        Vec::new()
                    } else {
                        vec![RowSelector::select(self.selected_rows)]
                    }
                });
                selectors.extend(selection);
            }
            None => {
                if let Some(selectors) = &mut self.selectors {
                    selectors.push(RowSelector::select(row_count));
                }
            }
        }
        self.selected_rows = self.selected_rows.checked_add(row_count).ok_or_else(|| {
            Error::ResourceExhausted(
                "Parquet row-group chunk row count exceeds this platform".to_owned(),
            )
        })?;
        self.predicate_cache.arrow_bytes = self
            .predicate_cache
            .arrow_bytes
            .max(predicate_cache.arrow_bytes);
        self.predicate_cache.reservation_bytes = self
            .predicate_cache
            .reservation_bytes
            .max(predicate_cache.reservation_bytes);
        self.apply_row_filter = Some(apply_row_filter);
        if let Some(lease) = sidecar_selection_lease {
            self.sidecar_selection_leases.push(lease);
        }
        // Arrow creates and drops the decoded predicate cache one row group at
        // a time, so the chunk needs the peak reservation, not their sum.
        Ok(())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.row_groups.is_empty()
    }

    pub(super) fn is_full(&self) -> bool {
        self.row_groups.len() == self.capacity
    }

    pub(super) fn can_accept(&self, apply_row_filter: bool) -> bool {
        self.apply_row_filter
            .is_none_or(|current| current == apply_row_filter)
    }

    pub(super) fn finish(
        self,
    ) -> (
        Vec<usize>,
        Option<RowSelection>,
        PredicateCachePlan,
        bool,
        Vec<MemoryReservation>,
    ) {
        (
            self.row_groups,
            self.selectors.map(RowSelection::from),
            self.predicate_cache,
            self.apply_row_filter.unwrap_or(true),
            self.sidecar_selection_leases,
        )
    }
}

#[cfg(test)]
mod tests {
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

    use super::{RowGroupChunk, size};
    use crate::datasource::parquet_predicate_cache::PredicateCachePlan;

    #[test]
    fn chunk_size_preserves_lanes_and_keeps_dynamic_or_limited_scans_single_group() {
        assert_eq!(size(true, None, 49, 4), 4);
        assert_eq!(size(true, None, 9, 4), 2);
        assert_eq!(size(true, None, 4, 8), 1);
        assert_eq!(size(false, None, 49, 4), 1);
        assert_eq!(size(true, Some(10), 49, 4), 1);
    }

    #[test]
    fn selections_and_peak_cache_are_combined_in_selected_group_coordinates() {
        let mut chunk = RowGroupChunk::new(3);
        chunk
            .push(
                0,
                5,
                None,
                PredicateCachePlan {
                    arrow_bytes: 8,
                    reservation_bytes: 12,
                },
                true,
                None,
            )
            .unwrap();
        chunk
            .push(
                2,
                5,
                Some(RowSelection::from(vec![
                    RowSelector::skip(1),
                    RowSelector::select(2),
                    RowSelector::skip(2),
                ])),
                PredicateCachePlan {
                    arrow_bytes: 16,
                    reservation_bytes: 24,
                },
                true,
                None,
            )
            .unwrap();
        chunk
            .push(
                3,
                4,
                None,
                PredicateCachePlan {
                    arrow_bytes: 4,
                    reservation_bytes: 6,
                },
                true,
                None,
            )
            .unwrap();

        let (groups, selection, cache, apply_row_filter, leases) = chunk.finish();
        assert_eq!(groups, vec![0, 2, 3]);
        assert_eq!(
            Vec::<RowSelector>::from(selection.unwrap()),
            vec![
                RowSelector::select(5),
                RowSelector::skip(1),
                RowSelector::select(2),
                RowSelector::skip(2),
                RowSelector::select(4),
            ]
        );
        assert_eq!(cache.arrow_bytes, 16);
        assert_eq!(cache.reservation_bytes, 24);
        assert!(apply_row_filter);
        assert!(leases.is_empty());
    }

    #[test]
    fn rejects_mixed_row_filter_modes() {
        let mut chunk = RowGroupChunk::new(2);
        chunk
            .push(0, 4, None, PredicateCachePlan::default(), false, None)
            .unwrap();
        assert!(!chunk.can_accept(true));
        assert!(
            chunk
                .push(1, 4, None, PredicateCachePlan::default(), true, None)
                .is_err()
        );
    }
}

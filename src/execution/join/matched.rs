use std::{
    mem::size_of,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext},
};

#[derive(Clone)]
pub(super) struct BuildMatchTracker {
    words: Arc<[AtomicU64]>,
    rows: usize,
}

impl BuildMatchTracker {
    pub(super) fn required_bytes(rows: usize) -> usize {
        tracker_bytes(rows)
    }

    #[cfg(test)]
    pub(super) fn new(rows: usize, reservation: &mut MemoryReservation) -> Result<Self> {
        Self::try_new(rows, reservation).ok_or_else(|| {
            Error::ResourceExhausted(format!(
                "join build match tracker requires {} bytes (pool limit {}, available {})",
                tracker_bytes(rows),
                reservation.pool().limit(),
                reservation.pool().available(),
            ))
        })
    }

    pub(super) fn try_new(rows: usize, reservation: &mut MemoryReservation) -> Option<Self> {
        reservation.try_grow(tracker_bytes(rows)).ok()?;
        Some(Self::allocate(rows))
    }

    pub(super) fn from_reserved(rows: usize, reservation: &MemoryReservation) -> Result<Self> {
        let required = tracker_bytes(rows);
        if reservation.size() < required {
            return Err(Error::Internal(format!(
                "join match tracker received {} reserved bytes, requires {required}",
                reservation.size(),
            )));
        }
        Ok(Self::allocate(rows))
    }

    fn allocate(rows: usize) -> Self {
        let words = rows.div_ceil(64);
        Self {
            words: (0..words)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into(),
            rows,
        }
    }

    pub(super) fn mark(&self, row: u32) {
        let row = row as usize;
        self.words[row / 64].fetch_or(1_u64 << (row % 64), Ordering::Relaxed);
    }

    pub(super) async fn unmatched_from(
        &self,
        start: usize,
        limit: usize,
        context: &QueryContext,
        held_bytes: usize,
    ) -> Result<MatchedIndexBatch> {
        self.unmatched_range(start, self.rows, 0, limit, context, held_bytes)
            .await
    }

    pub(super) async fn unmatched_range(
        &self,
        start: usize,
        end: usize,
        base: usize,
        limit: usize,
        context: &QueryContext,
        held_bytes: usize,
    ) -> Result<MatchedIndexBatch> {
        if start > end || end > self.rows || base > start {
            return Err(Error::Internal(
                "join unmatched build index range is invalid".into(),
            ));
        }
        if start == end {
            return Ok(MatchedIndexBatch {
                values: Vec::new(),
                memory: context.memory.reservation(),
            });
        }
        let requested = limit.max(1).min(end - start);
        let (capacity, memory) = reserve_indices(context, held_bytes, requested).await?;
        let mut output = Vec::with_capacity(capacity);
        for row in start..end {
            if self.words[row / 64].load(Ordering::Relaxed) & (1_u64 << (row % 64)) == 0 {
                output.push(u32::try_from(row - base).map_err(|_| {
                    Error::ResourceExhausted("join build side exceeds UINT32_MAX rows".into())
                })?);
                if output.len() == capacity {
                    break;
                }
            }
        }
        Ok(MatchedIndexBatch {
            values: output,
            memory,
        })
    }
}

pub(super) struct MatchedIndexBatch {
    values: Vec<u32>,
    memory: MemoryReservation,
}

impl MatchedIndexBatch {
    pub(super) fn memory_size(&self) -> usize {
        self.memory.size()
    }
}

impl Deref for MatchedIndexBatch {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

fn tracker_bytes(rows: usize) -> usize {
    rows.div_ceil(64)
        .saturating_mul(size_of::<AtomicU64>())
        .saturating_add(size_of::<BuildMatchTracker>())
        .saturating_add(128)
}

async fn reserve_indices(
    context: &QueryContext,
    held_bytes: usize,
    requested: usize,
) -> Result<(usize, MemoryReservation)> {
    let mut capacity = requested;
    loop {
        let bytes = capacity
            .saturating_mul(size_of::<u32>())
            .saturating_add(size_of::<Vec<u32>>())
            .saturating_add(128);
        match context
            .reserve_memory_while_holding(bytes, held_bytes, "join unmatched build indices")
            .await
        {
            Ok(memory) => return Ok((capacity, memory)),
            Err(Error::ResourceExhausted(_)) if capacity > 1 => {
                capacity = (capacity / 2).max(1);
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::{MemoryPool, QueryContext};

    use super::BuildMatchTracker;

    #[test]
    fn tracker_allocation_can_request_spill_without_leaking_a_lease() {
        let pool = MemoryPool::new(64);
        let mut reservation = pool.reservation();
        assert!(BuildMatchTracker::try_new(1, &mut reservation).is_none());
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn unmatched_indices_are_accounted_and_shrink_to_available_memory() {
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(512), temp.path()).unwrap();
        let mut retained = context.memory.reservation();
        let tracker = BuildMatchTracker::new(128, &mut retained).unwrap();
        tracker.mark(0);
        let before = context.memory.used();

        let indices = tracker
            .unmatched_from(0, 128, &context, retained.size())
            .await
            .unwrap();
        assert!(!indices.is_empty());
        assert!(
            indices.len() < 128,
            "the oversized request should be reduced"
        );
        assert!(indices.memory_size() > 0);
        assert!(context.memory.used() <= context.memory.limit());

        drop(indices);
        assert_eq!(context.memory.used(), before);
    }
}

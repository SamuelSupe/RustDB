use crate::runtime::MemoryPool;

/// Engine-wide memory accounting across all concurrent queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct EngineMemorySnapshot {
    pub current_bytes: usize,
    /// Peak reservation since this Engine was created.
    pub lifetime_peak_bytes: usize,
    pub limit_bytes: usize,
}

impl EngineMemorySnapshot {
    pub(super) fn from_pool(pool: &MemoryPool) -> Self {
        Self {
            current_bytes: pool.used(),
            lifetime_peak_bytes: pool.peak(),
            limit_bytes: pool.limit(),
        }
    }
}

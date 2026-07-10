use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{Error, Result};

#[derive(Clone)]
pub struct MemoryPool {
    node: Arc<Node>,
}

struct Node {
    name: Arc<str>,
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    parent: Option<Arc<Node>>,
}

pub struct MemoryReservation {
    pool: MemoryPool,
    bytes: usize,
}

impl MemoryPool {
    pub fn new(limit: usize) -> Self {
        Self::named_root("global", limit)
    }

    pub fn named_root(name: impl Into<Arc<str>>, limit: usize) -> Self {
        Self {
            node: Arc::new(Node {
                name: name.into(),
                limit,
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                parent: None,
            }),
        }
    }

    /// Creates a child whose allocations count against both this pool and the child limit.
    pub fn child(&self, name: impl Into<Arc<str>>, limit: usize) -> Self {
        Self {
            node: Arc::new(Node {
                name: name.into(),
                limit,
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                parent: Some(Arc::clone(&self.node)),
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.node.name
    }

    pub fn limit(&self) -> usize {
        self.node.limit
    }

    pub fn used(&self) -> usize {
        self.node.used.load(Ordering::Acquire)
    }

    pub fn peak(&self) -> usize {
        self.node.peak.load(Ordering::Acquire)
    }

    pub fn available(&self) -> usize {
        self.limit().saturating_sub(self.used())
    }

    pub fn reservation(&self) -> MemoryReservation {
        MemoryReservation {
            pool: self.clone(),
            bytes: 0,
        }
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<MemoryReservation> {
        self.acquire(bytes)?;
        Ok(MemoryReservation {
            pool: self.clone(),
            bytes,
        })
    }

    fn acquire(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }

        let nodes = self.ancestors_root_first();
        let mut acquired = Vec::with_capacity(nodes.len());
        for node in &nodes {
            match node.acquire(bytes) {
                Ok(used) => acquired.push((Arc::clone(node), used)),
                Err(error) => {
                    for (rollback, _) in acquired.iter().rev() {
                        rollback.release(bytes);
                    }
                    return Err(error);
                }
            }
        }
        for (node, used) in acquired {
            node.observe_peak(used);
        }
        Ok(())
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        for node in self.ancestors_root_first().iter().rev() {
            node.release(bytes);
        }
    }

    fn ancestors_root_first(&self) -> Vec<Arc<Node>> {
        let mut nodes = Vec::new();
        let mut current = Some(Arc::clone(&self.node));
        while let Some(node) = current {
            current = node.parent.clone();
            nodes.push(node);
        }
        nodes.reverse();
        nodes
    }
}

impl Node {
    fn acquire(&self, bytes: usize) -> Result<usize> {
        let result = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if bytes <= self.limit.saturating_sub(current) {
                    Some(current + bytes)
                } else {
                    None
                }
            });

        match result {
            Ok(previous) => Ok(previous + bytes),
            Err(current) => Err(Error::ResourceExhausted(format!(
                "memory pool '{}' cannot reserve {bytes} bytes (used {current}, limit {})",
                self.name, self.limit
            ))),
        }
    }

    fn observe_peak(&self, used: usize) {
        self.peak.fetch_max(used, Ordering::AcqRel);
    }

    fn release(&self, bytes: usize) {
        let result = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(bytes)
            });
        debug_assert!(result.is_ok(), "memory reservation accounting underflow");
    }
}

impl MemoryReservation {
    pub fn size(&self) -> usize {
        self.bytes
    }

    pub fn pool(&self) -> &MemoryPool {
        &self.pool
    }

    pub fn try_grow(&mut self, bytes: usize) -> Result<()> {
        self.pool.acquire(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    pub fn shrink(&mut self, bytes: usize) {
        let released = bytes.min(self.bytes);
        self.pool.release(released);
        self.bytes -= released;
    }

    pub fn try_resize(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.bytes {
            self.try_grow(bytes - self.bytes)
        } else {
            self.shrink(self.bytes - bytes);
            Ok(())
        }
    }

    pub fn release(mut self) {
        self.shrink(self.bytes);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.shrink(self.bytes);
    }
}

impl fmt::Debug for MemoryPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryPool")
            .field("name", &self.name())
            .field("limit", &self.limit())
            .field("used", &self.used())
            .field("peak", &self.peak())
            .finish()
    }
}

impl fmt::Debug for MemoryReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryReservation")
            .field("pool", &self.pool.name())
            .field("bytes", &self.bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryPool;

    #[test]
    fn hierarchical_reservations_obey_child_and_parent_limits() {
        let global = MemoryPool::new(100);
        let left = global.child("left", 80);
        let right = global.child("right", 80);
        let left_reservation = left.try_reserve(70).expect("left reservation");

        assert!(right.try_reserve(40).is_err());
        assert_eq!(global.used(), 70);
        assert_eq!(right.used(), 0);
        assert!(left.try_reserve(11).is_err());

        drop(left_reservation);
        assert_eq!(global.used(), 0);
        assert_eq!(global.peak(), 70);
    }

    #[test]
    fn reservation_can_grow_shrink_and_resize() {
        let pool = MemoryPool::new(100);
        let mut reservation = pool.reservation();
        reservation.try_grow(60).expect("grow");
        reservation.shrink(10);
        reservation.try_resize(20).expect("resize down");
        assert_eq!(reservation.size(), 20);
        assert_eq!(pool.used(), 20);
        assert!(reservation.try_resize(101).is_err());
        assert_eq!(reservation.size(), 20);
        drop(reservation);
        assert_eq!(pool.used(), 0);
    }
}

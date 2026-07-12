use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::Notify;

use crate::{Error, Result};

use super::QueryControl;

#[derive(Clone)]
pub struct MemoryPool {
    node: Arc<Node>,
}

struct Node {
    name: Arc<str>,
    limit: usize,
    emergency_headroom: AtomicUsize,
    used: AtomicUsize,
    peak: AtomicUsize,
    parent: Option<Arc<Node>>,
    notify: Arc<Notify>,
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
        let notify = Arc::new(Notify::new());
        Self {
            node: Arc::new(Node {
                name: name.into(),
                limit,
                emergency_headroom: AtomicUsize::new(0),
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                parent: None,
                notify,
            }),
        }
    }

    /// Creates a child whose allocations count against both this pool and the child limit.
    pub fn child(&self, name: impl Into<Arc<str>>, limit: usize) -> Self {
        Self {
            node: Arc::new(Node {
                name: name.into(),
                limit,
                emergency_headroom: AtomicUsize::new(0),
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                parent: Some(Arc::clone(&self.node)),
                notify: Arc::clone(&self.node.notify),
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
        self.ancestors_root_first()
            .into_iter()
            .map(|node| {
                node.allocatable_limit()
                    .saturating_sub(node.used.load(Ordering::Acquire))
            })
            .min()
            .unwrap_or(0)
    }

    pub(crate) fn emergency_headroom(&self) -> usize {
        self.ancestors_root_first()
            .into_iter()
            .map(|node| node.emergency_headroom.load(Ordering::Acquire))
            .min()
            .unwrap_or(0)
    }

    pub(crate) fn operation_limit(&self) -> usize {
        self.ancestors_root_first()
            .into_iter()
            .map(|node| node.allocatable_limit())
            .min()
            .unwrap_or(0)
    }

    /// Keeps a final slice of this pool unavailable to ordinary operators so
    /// spill I/O can always copy at least one bounded chunk. The headroom is
    /// not allocated and therefore does not inflate memory metrics.
    pub(crate) fn protect_emergency_headroom(&self, bytes: usize) -> Result<()> {
        if bytes > self.limit() {
            return Err(Error::ResourceExhausted(format!(
                "memory pool '{}' cannot protect {bytes} emergency bytes (limit {})",
                self.name(),
                self.limit()
            )));
        }
        self.node
            .emergency_headroom
            .fetch_max(bytes, Ordering::AcqRel);
        Ok(())
    }

    pub fn reservation(&self) -> MemoryReservation {
        MemoryReservation {
            pool: self.clone(),
            bytes: 0,
        }
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<MemoryReservation> {
        self.acquire(bytes, false)?;
        Ok(MemoryReservation {
            pool: self.clone(),
            bytes,
        })
    }

    pub(crate) fn try_reserve_emergency(&self, bytes: usize) -> Result<MemoryReservation> {
        self.acquire(bytes, true)?;
        Ok(MemoryReservation {
            pool: self.clone(),
            bytes,
        })
    }

    /// Waits for query memory released by downstream consumers instead of
    /// turning temporary streaming pressure into a query failure.
    ///
    /// All pools in one hierarchy share a notifier, so a child blocked by the
    /// engine root also wakes when another query releases memory. A request
    /// larger than any node limit can never make progress and fails
    /// immediately.
    pub(crate) async fn reserve_wait(
        &self,
        bytes: usize,
        held_bytes: usize,
        control: &QueryControl,
    ) -> Result<MemoryReservation> {
        let single_operation_bytes = bytes.saturating_add(held_bytes);
        if let Some(node) = self
            .ancestors_root_first()
            .into_iter()
            .find(|node| single_operation_bytes > node.allocatable_limit())
        {
            return Err(Error::ResourceExhausted(format!(
                "memory pool '{}' cannot satisfy one operation requiring {bytes} workspace bytes while retaining {held_bytes} bytes (limit {})",
                node.name,
                node.allocatable_limit(),
            )));
        }

        loop {
            control.check_cancelled()?;
            // Register before trying to acquire so a concurrent release cannot
            // be lost between the failed attempt and the await.
            let notified = self.node.notify.notified();
            match self.try_reserve(bytes) {
                Ok(reservation) => return Ok(reservation),
                Err(_) => {
                    tokio::select! {
                        _ = control.cancelled() => return Err(Error::Cancelled),
                        () = notified => {}
                    }
                }
            }
        }
    }

    fn acquire(&self, bytes: usize, use_emergency_headroom: bool) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }

        let nodes = self.ancestors_root_first();
        let mut acquired = Vec::with_capacity(nodes.len());
        for node in &nodes {
            match node.acquire(bytes, use_emergency_headroom) {
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
        self.node.notify.notify_waiters();
        // Preserve one permit for a waiter that observed the failed acquire
        // immediately before this release but had not yet been polled.
        self.node.notify.notify_one();
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
    fn allocatable_limit(&self) -> usize {
        self.limit
            .saturating_sub(self.emergency_headroom.load(Ordering::Acquire))
    }

    fn acquire(&self, bytes: usize, use_emergency_headroom: bool) -> Result<usize> {
        let allocation_limit = if use_emergency_headroom {
            self.limit
        } else {
            self.allocatable_limit()
        };
        let result = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if bytes <= allocation_limit.saturating_sub(current) {
                    Some(current + bytes)
                } else {
                    None
                }
            });

        match result {
            Ok(previous) => Ok(previous + bytes),
            Err(current) => Err(Error::ResourceExhausted(format!(
                "memory pool '{}' cannot reserve {bytes} bytes (used {current}, limit {})",
                self.name, allocation_limit
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
        self.pool.acquire(bytes, false)?;
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

    /// Transfers an already-accounted reservation into this handle without
    /// changing pool usage. Both handles must belong to the same pool.
    pub(crate) fn absorb(&mut self, mut other: Self) -> Result<()> {
        if !Arc::ptr_eq(&self.pool.node, &other.pool.node) {
            return Err(Error::Internal(
                "cannot transfer memory between different pools".into(),
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or_else(|| Error::Internal("memory reservation size overflow".into()))?;
        other.bytes = 0;
        Ok(())
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
    use std::time::Duration;

    use super::MemoryPool;
    use crate::{Error, runtime::QueryControl};

    #[test]
    fn hierarchical_reservations_obey_child_and_parent_limits() {
        let global = MemoryPool::new(100);
        let left = global.child("left", 80);
        let right = global.child("right", 80);
        let left_reservation = left.try_reserve(70).expect("left reservation");

        assert_eq!(left.available(), 10);
        assert_eq!(right.available(), 30);
        assert!(right.try_reserve(40).is_err());
        assert_eq!(global.used(), 70);
        assert_eq!(right.used(), 0);
        assert!(left.try_reserve(11).is_err());

        drop(left_reservation);
        assert_eq!(global.used(), 0);
        assert_eq!(global.peak(), 70);
    }

    #[test]
    fn emergency_headroom_is_protected_at_child_and_parent_nodes() {
        let engine = MemoryPool::named_root("engine", 100);
        engine.protect_emergency_headroom(10).unwrap();
        let query = engine.child("query", 100);
        query.protect_emergency_headroom(10).unwrap();
        let sibling = engine.child("sibling", 100);

        let normal = query.try_reserve(90).unwrap();
        assert_eq!(query.available(), 0);
        assert_eq!(sibling.available(), 0);
        assert!(query.try_reserve(1).is_err());
        assert!(sibling.try_reserve(1).is_err());

        let copy = query.try_reserve_emergency(10).unwrap();
        assert_eq!(query.used(), 100);
        assert_eq!(engine.peak(), 100);
        assert!(query.try_reserve_emergency(1).is_err());

        drop(copy);
        drop(normal);
        assert_eq!(query.used(), 0);
        assert_eq!(engine.used(), 0);
    }

    #[test]
    fn available_uses_the_tightest_ancestor_headroom() {
        let engine = MemoryPool::named_root("engine", 100);
        engine.protect_emergency_headroom(20).unwrap();
        let query = engine.child("query", 100);
        query.protect_emergency_headroom(10).unwrap();

        let reservation = query.try_reserve(80).unwrap();
        // The query node still has 10 ordinary bytes of its own, but the
        // engine root has none. Reporting only the child value would let
        // operators defer spilling until their next parent reservation fails.
        assert_eq!(query.available(), 0);
        assert!(query.try_reserve(1).is_err());

        drop(reservation);
        assert_eq!(query.available(), 80);
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

    #[tokio::test]
    async fn temporary_pressure_wakes_all_waiters_without_losing_a_release() {
        let pool = MemoryPool::new(9);
        let blocker = pool.try_reserve(9).expect("initial reservation");
        let control = QueryControl::new();
        let mut waiters = Vec::new();
        for _ in 0..3 {
            let pool = pool.clone();
            let control = control.clone();
            waiters.push(tokio::spawn(async move {
                pool.reserve_wait(3, 0, &control).await
            }));
        }

        tokio::task::yield_now().await;
        drop(blocker);

        let reservations = tokio::time::timeout(Duration::from_secs(1), async {
            let mut reservations = Vec::new();
            for waiter in waiters {
                reservations.push(waiter.await.expect("waiter task")?);
            }
            Ok::<_, Error>(reservations)
        })
        .await
        .expect("all waiters must observe the release")
        .expect("all reservations must succeed");
        assert_eq!(reservations.iter().map(|r| r.size()).sum::<usize>(), 9);
        drop(reservations);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn impossible_single_batch_request_fails_without_waiting() {
        let pool = MemoryPool::new(100);
        let control = QueryControl::new();
        let error = tokio::time::timeout(
            Duration::from_millis(100),
            pool.reserve_wait(50, 60, &control),
        )
        .await
        .expect("impossible request must not wait")
        .expect_err("workspace plus retained input exceeds the limit");
        assert!(error.to_string().contains("one operation"));
    }

    #[tokio::test]
    async fn cancellation_interrupts_a_memory_wait() {
        let pool = MemoryPool::new(10);
        let _blocker = pool.try_reserve(10).expect("initial reservation");
        let control = QueryControl::new();
        let waiting_pool = pool.clone();
        let waiting_control = control.clone();
        let waiter =
            tokio::spawn(async move { waiting_pool.reserve_wait(1, 0, &waiting_control).await });
        tokio::task::yield_now().await;
        control.cancel();
        let error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancelled waiter must wake")
            .expect("waiter task")
            .expect_err("reservation must be cancelled");
        assert!(matches!(error, Error::Cancelled));
    }
}

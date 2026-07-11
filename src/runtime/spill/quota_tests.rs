use std::{
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::{DiskSpace, DiskSpaceProbe, SpillQuotaPool, SystemDiskSpaceProbe};
use crate::{Error, config::SpillConfig};

#[derive(Debug)]
struct FixedProbe(io::Result<DiskSpace>);

impl DiskSpaceProbe for FixedProbe {
    fn probe(&self, _path: &Path) -> io::Result<DiskSpace> {
        match &self.0 {
            Ok(space) => Ok(*space),
            Err(error) => Err(error.raw_os_error().map_or_else(
                || io::Error::new(error.kind(), error.to_string()),
                io::Error::from_raw_os_error,
            )),
        }
    }
}

#[derive(Debug)]
struct CountingProbe {
    calls: Arc<AtomicUsize>,
    space: DiskSpace,
}

impl DiskSpaceProbe for CountingProbe {
    fn probe(&self, _path: &Path) -> io::Result<DiskSpace> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.space)
    }
}

fn config() -> SpillConfig {
    SpillConfig {
        directory: "/injected/spill".into(),
        engine_limit_bytes: Some(100),
        query_limit_bytes: Some(60),
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        ..SpillConfig::default()
    }
}

fn pool(config: SpillConfig, space: DiskSpace) -> SpillQuotaPool {
    SpillQuotaPool::with_probe(config, Arc::new(FixedProbe(Ok(space)))).expect("valid quota pool")
}

#[test]
fn reservations_enforce_engine_and_query_limits_and_release_exactly() {
    let pool = pool(
        config(),
        DiskSpace {
            available_bytes: 1_000,
            total_bytes: 1_000,
        },
    );
    let first_query = pool.start_query();
    let reservation = first_query.try_reserve(40).expect("reserve 40");
    assert_eq!(reservation.reserved_bytes(), 40);
    assert_eq!(pool.pending_bytes(), 40);
    let charge = reservation.commit(25).expect("commit 25");
    assert_eq!(charge.bytes(), 25);
    assert_eq!(first_query.committed_bytes(), 25);
    assert_eq!(pool.committed_bytes(), 25);

    let pending = first_query.try_reserve(35).expect("query reaches limit");
    assert!(matches!(
        first_query.try_reserve(1),
        Err(Error::ResourceExhausted(message)) if message.contains("query quota")
    ));
    drop(pending);
    assert_eq!(first_query.pending_bytes(), 0);

    let second_query = pool.start_query();
    let second = second_query.try_reserve(60).expect("second query limit");
    let second_charge = second.commit(60).expect("commit second query");
    let third_query = pool.start_query();
    let third = third_query.try_reserve(15).expect("engine reaches limit");
    let third_charge = third.commit(15).expect("commit third query");
    assert!(matches!(
        third_query.try_reserve(1),
        Err(Error::ResourceExhausted(message)) if message.contains("engine quota")
    ));

    third_charge.release();
    second_charge.release();
    drop(charge);
    assert_eq!(pool.committed_bytes(), 0);
    assert_eq!(first_query.committed_bytes(), 0);
    assert_eq!(second_query.committed_bytes(), 0);
    assert_eq!(third_query.committed_bytes(), 0);
}

#[test]
fn disk_reserve_accounts_for_ratio_rounding_and_pending_writes() {
    let config = SpillConfig {
        min_free_ratio: 0.10,
        min_free_bytes: 0,
        engine_limit_bytes: None,
        query_limit_bytes: None,
        ..config()
    };
    let pool = pool(
        config,
        DiskSpace {
            available_bytes: 151,
            total_bytes: 1_001,
        },
    );
    let query = pool.start_query();
    let pending = query.try_reserve(50).expect("151 - 50 leaves 101");
    query
        .check_available()
        .expect("available space exactly meets the reserve");
    let overflow = query
        .try_reserve(1)
        .expect("small writes defer the disk-space probe");
    assert!(matches!(
        query.check_available(),
        Err(Error::ResourceExhausted(message)) if message.contains("required free 101")
    ));
    drop(overflow);
    drop(pending);
    assert_eq!(pool.pending_bytes(), 0);
}

#[test]
fn disk_probe_runs_at_file_creation_and_each_accumulated_64_mib() {
    const CHUNK: u64 = 256 * 1024;
    let calls = Arc::new(AtomicUsize::new(0));
    let config = SpillConfig {
        engine_limit_bytes: None,
        query_limit_bytes: None,
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        ..config()
    };
    let pool = SpillQuotaPool::with_probe(
        config,
        Arc::new(CountingProbe {
            calls: Arc::clone(&calls),
            space: DiskSpace {
                available_bytes: 1 << 40,
                total_bytes: 1 << 40,
            },
        }),
    )
    .unwrap();
    let query = pool.start_query();

    query.check_available().unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    for _ in 0..255 {
        query
            .try_reserve(CHUNK)
            .unwrap()
            .commit(CHUNK)
            .unwrap()
            .release();
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    query
        .try_reserve(CHUNK)
        .unwrap()
        .commit(CHUNK)
        .unwrap()
        .release();
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    let second_query = pool.start_query();
    for index in 0..256 {
        let query = if index % 2 == 0 {
            &query
        } else {
            &second_query
        };
        query
            .try_reserve(CHUNK)
            .unwrap()
            .commit(CHUNK)
            .unwrap()
            .release();
    }
    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "the 64 MiB write interval must be shared across concurrent queries"
    );
}

#[test]
fn injected_enospc_is_returned_without_touching_the_real_disk() {
    let probe = FixedProbe(Err(io::Error::from_raw_os_error(28)));
    let pool = SpillQuotaPool::with_probe(config(), Arc::new(probe)).expect("quota pool");
    let error = pool.start_query().check_available().unwrap_err();
    assert!(matches!(
        error,
        Error::Io { source, .. } if source.raw_os_error() == Some(28)
    ));
    assert_eq!(pool.pending_bytes(), 0);
}

#[test]
fn oversized_commit_keeps_accounting_balanced() {
    let pool = pool(
        config(),
        DiskSpace {
            available_bytes: 1_000,
            total_bytes: 1_000,
        },
    );
    let query = pool.start_query();
    let error = query.try_reserve(10).unwrap().commit(11).unwrap_err();
    assert!(matches!(error, Error::InvalidArgument(_)));
    assert_eq!(query.pending_bytes(), 0);
    assert_eq!(query.committed_bytes(), 0);
    assert_eq!(pool.pending_bytes(), 0);
    assert_eq!(pool.committed_bytes(), 0);
}

#[test]
fn system_probe_resolves_the_temp_filesystem() {
    let root = tempfile::tempdir().expect("tempdir");
    let space = SystemDiskSpaceProbe
        .probe(root.path())
        .expect("filesystem space");
    assert!(space.total_bytes > 0);
    assert!(space.available_bytes <= space.total_bytes);
}

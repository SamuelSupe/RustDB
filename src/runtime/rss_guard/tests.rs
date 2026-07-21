use std::fs;

use tempfile::tempdir;

use super::{RssGuardConfig, RssGuardDecision, RssGuardian, probe::cgroup_limit_from};

#[test]
fn config_requires_strictly_ordered_finite_watermarks() {
    assert!(RssGuardConfig::default().validate().is_ok());
    for config in [
        RssGuardConfig {
            warning_ratio: 0.0,
            high_ratio: 0.8,
            critical_ratio: 0.9,
        },
        RssGuardConfig {
            warning_ratio: 0.8,
            high_ratio: 0.8,
            critical_ratio: 0.9,
        },
        RssGuardConfig {
            warning_ratio: 0.7,
            high_ratio: 0.9,
            critical_ratio: 0.8,
        },
        RssGuardConfig {
            warning_ratio: 0.7,
            high_ratio: f64::NAN,
            critical_ratio: 0.9,
        },
        RssGuardConfig {
            warning_ratio: 0.7,
            high_ratio: 0.8,
            critical_ratio: 1.01,
        },
    ] {
        assert!(config.validate().is_err(), "accepted {config:?}");
    }
}

#[test]
fn decisions_change_at_each_watermark() {
    let guardian = RssGuardian::default();
    let decision = |rss| guardian.assess(rss, 1_000, None).decision;

    assert_eq!(decision(699), RssGuardDecision::Normal);
    assert_eq!(decision(700), RssGuardDecision::Throttle);
    assert_eq!(decision(799), RssGuardDecision::Throttle);
    assert_eq!(decision(800), RssGuardDecision::Reject);
    assert_eq!(decision(899), RssGuardDecision::Reject);
    assert_eq!(decision(900), RssGuardDecision::CancelLargest);
}

#[test]
fn cgroup_limit_bounds_physical_memory() {
    let guardian = RssGuardian::default();
    let snapshot = guardian.assess(450, 4_000, Some(500));

    assert_eq!(snapshot.effective_limit_bytes, 500);
    assert_eq!(snapshot.pressure_ratio, 0.9);
    assert_eq!(snapshot.decision, RssGuardDecision::CancelLargest);

    let host_bounded = guardian.assess(2_800, 4_000, Some(8_000));
    assert_eq!(host_bounded.effective_limit_bytes, 4_000);
    assert_eq!(host_bounded.decision, RssGuardDecision::Throttle);
}

#[test]
fn zero_effective_limit_is_always_critical() {
    let snapshot = RssGuardian::default().assess(0, 1_000, Some(0));
    assert_eq!(snapshot.decision, RssGuardDecision::CancelLargest);
    assert_eq!(snapshot.pressure_ratio, 1.0);
}

#[test]
fn cgroup_v2_uses_the_tightest_finite_ancestor() {
    let root = tempdir().unwrap();
    let unified = root.path().join("unified");
    let v1 = root.path().join("v1");
    fs::create_dir_all(unified.join("tenant/query")).unwrap();
    fs::write(unified.join("memory.max"), "max\n").unwrap();
    fs::write(unified.join("tenant/memory.max"), "1048576\n").unwrap();
    fs::write(unified.join("tenant/query/memory.max"), "max\n").unwrap();

    let limit = cgroup_limit_from("0::/tenant/query\n", &unified, &v1);
    assert_eq!(limit, Some(1_048_576));
}

#[test]
fn cgroup_v1_and_malformed_membership_are_handled_safely() {
    let root = tempdir().unwrap();
    let unified = root.path().join("unified");
    let v1 = root.path().join("v1");
    fs::create_dir_all(v1.join("container/child")).unwrap();
    fs::write(
        v1.join("container/child/memory.limit_in_bytes"),
        "2097152\n",
    )
    .unwrap();

    assert_eq!(
        cgroup_limit_from("5:cpu,memory:/container/child\n", &unified, &v1),
        Some(2_097_152)
    );
    assert_eq!(
        cgroup_limit_from("0::/../../outside\n", &unified, &v1),
        None
    );
    assert_eq!(
        cgroup_limit_from("not-a-cgroup-line\n", &unified, &v1),
        None
    );
}

#[test]
fn live_probe_reports_process_and_physical_memory() {
    let snapshot = RssGuardian::default().sample().unwrap();
    assert!(snapshot.process_rss_bytes > 0);
    assert!(snapshot.physical_memory_bytes > 0);
    assert!(snapshot.effective_limit_bytes <= snapshot.physical_memory_bytes);
}

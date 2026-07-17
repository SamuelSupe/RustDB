use crate::{Engine, EngineConfig};

#[test]
fn snapshot_tracks_reservations_across_query_children() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(
        EngineConfig::builder()
            .memory_limit(1 << 20)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let first = engine.query_context_for_test().unwrap();
    let second = engine.query_context_for_test().unwrap();

    let first_memory = first.try_reserve(128 << 10).unwrap();
    let second_memory = second.try_reserve(256 << 10).unwrap();
    assert_eq!(
        engine.memory_snapshot(),
        crate::EngineMemorySnapshot {
            current_bytes: 384 << 10,
            lifetime_peak_bytes: 384 << 10,
            limit_bytes: 1 << 20,
        }
    );

    drop(first_memory);
    let snapshot = engine.memory_snapshot();
    assert_eq!(snapshot.current_bytes, 256 << 10);
    assert_eq!(snapshot.lifetime_peak_bytes, 384 << 10);

    drop(second_memory);
    let snapshot = engine.memory_snapshot();
    assert_eq!(snapshot.current_bytes, 0);
    assert_eq!(snapshot.lifetime_peak_bytes, 384 << 10);
}

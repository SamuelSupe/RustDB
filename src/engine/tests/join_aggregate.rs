use arrow::array::{Decimal128Array, Int64Array};
use futures::TryStreamExt;

use crate::{Engine, EngineConfig};

#[tokio::test]
async fn simple_inner_join_global_aggregate_consumes_selections() {
    let directory = tempfile::tempdir().unwrap();
    let left = directory.path().join("left.csv");
    let right = directory.path().join("right.csv");
    std::fs::write(&left, "key,value\n1,10\n1,20\n2,30\n3,40\n").unwrap();
    std::fs::write(&right, "key\n1\n1\n2\n4\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .compute_threads(4)
            .memory_limit(256 << 20)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    let sql = format!(
        "SELECT count(*), sum(l.value) \
         FROM read_csv('{}', header = true) l \
         INNER JOIN read_csv('{}', header = true) r ON l.key = r.key",
        left.display(),
        right.display(),
    );

    let mut result = session.execute(&sql).await.unwrap();
    let metrics = result.metrics();
    let batches = result.stream().try_collect::<Vec<_>>().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        90
    );

    let snapshot = metrics.snapshot();
    let join = snapshot
        .operators
        .iter()
        .find(|operator| operator.name == "Join")
        .expect("fused plan must retain a Join operator metric");
    assert_eq!(join.output_rows, 5);
    assert_eq!(join.output_bytes, 0);
    assert!(snapshot.peak_memory_bytes <= 256 << 20);
}

#[tokio::test]
async fn composite_join_keys_preserve_rows_and_release_memory() {
    let directory = tempfile::tempdir().unwrap();
    let left = directory.path().join("left-composite.csv");
    let right = directory.path().join("right-composite.csv");
    std::fs::write(
        &left,
        "id,label,value\n1,a,10\n1,a,20\n1,b,30\n12,3,40\n1,23,50\n,x,60\n",
    )
    .unwrap();
    std::fs::write(&right, "id,label\n1,a\n1,a\n1,b\n12,3\n1,23\n,x\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .compute_threads(2)
            .memory_limit(64 << 20)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    let sql = format!(
        "SELECT count(*), sum(l.value) \
         FROM read_csv('{}', header = true) l \
         INNER JOIN read_csv('{}', header = true) r \
         ON l.id = r.id AND l.label = r.label",
        left.display(),
        right.display(),
    );

    let mut result = session.execute(&sql).await.unwrap();
    let metrics = result.metrics();
    let batches = result.stream().try_collect::<Vec<_>>().await.unwrap();
    drop(result);

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        180
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.join_candidate_pairs, 7);
    assert_eq!(snapshot.current_memory_bytes, 0);
    assert_eq!(snapshot.spill_bytes, 0);
    let join = snapshot
        .operators
        .iter()
        .find(|operator| operator.name == "Join")
        .expect("fused aggregate retains its Join operator");
    for name in ["JoinBuild", "JoinProbe", "JoinKeyWork"] {
        let phase = snapshot
            .operators
            .iter()
            .find(|operator| operator.name == name && operator.parent_id == Some(join.id))
            .unwrap_or_else(|| panic!("missing {name} child of Join"));
        assert!(phase.elapsed.as_nanos() > 0, "{name} did not record time");
        assert_eq!(phase.output_rows, 0);
        assert_eq!(phase.output_batches, 0);
    }
    let build = snapshot
        .operators
        .iter()
        .find(|operator| operator.name == "JoinBuild" && operator.parent_id == Some(join.id))
        .expect("missing JoinBuild child of Join");
    for name in [
        "JoinBuildInputPoll",
        "JoinBuildKeyEval",
        "JoinBuildHashTable",
    ] {
        let phase = snapshot
            .operators
            .iter()
            .find(|operator| operator.name == name && operator.parent_id == Some(build.id))
            .unwrap_or_else(|| panic!("missing {name} child of JoinBuild"));
        assert!(phase.elapsed.as_nanos() > 0, "{name} did not record time");
        assert_eq!(phase.wait.as_nanos(), 0);
        assert_eq!(phase.output_rows, 0);
        assert_eq!(phase.output_batches, 0);
    }
    let permit_wait = snapshot
        .operators
        .iter()
        .find(|operator| {
            operator.name == "JoinBuildPermitWait" && operator.parent_id == Some(build.id)
        })
        .expect("missing JoinBuildPermitWait child of JoinBuild");
    assert_eq!(permit_wait.elapsed.as_nanos(), 0);
    assert_eq!(permit_wait.output_rows, 0);
    assert_eq!(permit_wait.output_batches, 0);
}

#[tokio::test]
async fn materializing_join_records_build_and_probe_phases() {
    let directory = tempfile::tempdir().unwrap();
    let left = directory.path().join("left-materialized.csv");
    let right = directory.path().join("right-materialized.csv");
    std::fs::write(&left, "key,value\n1,10\n2,20\n3,30\n").unwrap();
    std::fs::write(&right, "key\n1\n3\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .compute_threads(2)
            .memory_limit(64 << 20)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    let sql = format!(
        "SELECT l.value FROM read_csv('{}', header = true) l \
         JOIN read_csv('{}', header = true) r ON l.key = r.key",
        left.display(),
        right.display(),
    );

    let mut result = session.execute(&sql).await.unwrap();
    let metrics = result.metrics();
    let rows = result.stream().try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 2);

    let snapshot = metrics.snapshot();
    let join = snapshot
        .operators
        .iter()
        .find(|operator| operator.name == "Join")
        .expect("materializing plan retains its Join operator");
    for name in ["JoinBuild", "JoinProbe"] {
        let phase = snapshot
            .operators
            .iter()
            .find(|operator| operator.name == name && operator.parent_id == Some(join.id))
            .unwrap_or_else(|| panic!("missing {name} child of Join"));
        assert!(phase.elapsed.as_nanos() > 0, "{name} did not record time");
    }
    assert!(
        !snapshot
            .operators
            .iter()
            .any(|operator| operator.name == "JoinKeyWork" && operator.parent_id == Some(join.id))
    );
    for name in [
        "JoinBuildInputPoll",
        "JoinBuildPermitWait",
        "JoinBuildKeyEval",
        "JoinBuildHashTable",
    ] {
        assert!(
            !snapshot
                .operators
                .iter()
                .any(|operator| operator.name == name && operator.parent_id == Some(join.id)),
            "ordinary materializing Join unexpectedly recorded {name}",
        );
    }
}

#[tokio::test]
async fn composite_build_side_sum_preserves_semantics_after_join_ordering() {
    let directory = tempfile::tempdir().unwrap();
    let left = directory.path().join("left-build-sum.csv");
    let right = directory.path().join("right-build-sum.csv");
    std::fs::write(&left, "id,label\n1,a\n1,a\n2,b\n").unwrap();
    std::fs::write(&right, "id,label,value\n1,a,5\n1,a,7\n2,b,11\n").unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .compute_threads(2)
            .memory_limit(64 << 20)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    let sql = format!(
        "SELECT count(*), sum(r.value) \
         FROM read_csv('{}', header = true) l \
         JOIN read_csv('{}', header = true) r \
           ON l.id = r.id AND l.label = r.label",
        left.display(),
        right.display(),
    );

    let mut result = session.execute(&sql).await.unwrap();
    let batches = result.stream().try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        35
    );
}

use std::collections::HashSet;

use super::*;
use crate::runtime::{MemoryPool, QueryContext};

#[test]
fn aggregate_id_is_part_of_the_identity() {
    let left = DistinctKey::new(Vec::new(), 0, CellValue::Int64(1));
    let right = DistinctKey::new(Vec::new(), 1, CellValue::Int64(1));
    assert_ne!(left, right);
}

#[test]
fn high_cardinality_single_group_repartitions_by_complete_identity() {
    let temp = tempfile::tempdir().unwrap();
    let mut context = QueryContext::new(MemoryPool::new(16 << 20), temp.path()).unwrap();
    context.execution.spill_partition_target_bytes = Some(16 << 10);
    let keys = (0..10_000_i64)
        .map(|value| DistinctKey::new(Vec::new(), 0, CellValue::Int64(value)))
        .collect::<HashSet<_>>();
    let mut spiller = DistinctSpiller::new(1, &context).unwrap();
    spiller.spill(keys, &context).unwrap();
    let mut partitions = spiller.finish(&context).unwrap();
    let source = partitions.pop().unwrap();
    let source_bytes = usize::try_from(source.estimated_bytes).unwrap();
    let one_key = DistinctKey::new(Vec::new(), 0, CellValue::Int64(0)).memory_size();
    assert_eq!(source_bytes, one_key.saturating_mul(10_000));
    let source = source.files;
    assert!(matches!(
        load_partition(
            &source,
            1,
            &context,
            context.memory.child("tiny-distinct-test", 4 << 10),
        )
        .unwrap(),
        MergeDistinct::Repartition
    ));

    let expected = adaptive_spill_partitions(&context, source_bytes);
    let children = repartition(&source, source_bytes, 1, 32, &context).unwrap();
    let metrics = context.metrics.snapshot();
    assert_eq!(children.len(), expected);
    assert!(children.len() > 2 && children.len() <= 256);
    assert!(children.len().is_power_of_two());
    assert!(metrics.spill_repartition_bytes > 0);
    assert_eq!(metrics.max_repartition_depth, 1);
    assert!(metrics.max_spill_partition_bytes > 0);
    for file in &source {
        context.spill.remove_file(file).unwrap();
    }
    let mut total = 0;
    for partition in children
        .into_iter()
        .filter(|partition| !partition.files.is_empty())
    {
        let files = partition.files;
        let MergeDistinct::Merged(loaded) = load_partition(
            &files,
            1,
            &context,
            context.memory.child("child-distinct-test", 512 << 10),
        )
        .unwrap() else {
            panic!("repartitioned complete identities should fit");
        };
        let (keys, memory) = loaded.into_parts();
        total += keys.len();
        drop(keys);
        drop(memory);
        for file in &files {
            context.spill.remove_file(file).unwrap();
        }
    }
    assert_eq!(total, 10_000);
    assert_eq!(context.memory.used(), 0);
}

#[test]
fn multi_distinct_run_uses_actual_pending_bytes_for_amplification() {
    let temp = tempfile::tempdir().unwrap();
    let mut context = QueryContext::new(MemoryPool::new(4 << 20), temp.path()).unwrap();
    context.execution.max_spill_write_amplification = Some(1.2);
    let partitions = spill_multi_distinct(&context).unwrap();
    let metrics = context.metrics.snapshot();
    let amplification = metrics.spill_write_amplification().unwrap();
    assert!(
        (amplification - 1.04).abs() <= 0.05,
        "expected about 1.04x physical writes, metrics: {metrics:?}"
    );
    for partition in partitions {
        for file in partition.files {
            context.spill.remove_file(&file).unwrap();
        }
    }
}

#[test]
fn multi_distinct_rejects_actual_amplification_over_limit() {
    let temp = tempfile::tempdir().unwrap();
    let mut context = QueryContext::new(MemoryPool::new(4 << 20), temp.path()).unwrap();
    context.execution.max_spill_write_amplification = Some(1.0);
    let error = spill_multi_distinct(&context).unwrap_err();
    assert!(
        matches!(error, Error::ResourceExhausted(message) if message.contains("write amplification"))
    );
    assert_eq!(context.metrics.snapshot().spill_quota_rejections, 1);
}

fn spill_multi_distinct(context: &QueryContext) -> Result<Vec<SpillPartition>> {
    const VALUES: i64 = 50_000;
    const CHUNK: i64 = 2_048;

    let mut spiller = DistinctSpiller::new(32, context)?;
    for start in (0..VALUES).step_by(usize::try_from(CHUNK).unwrap()) {
        let end = (start + CHUNK).min(VALUES);
        let keys = (start..end)
            .flat_map(|value| {
                (0..2).map(move |aggregate| {
                    DistinctKey::new(Vec::new(), aggregate, CellValue::Int64(value))
                })
            })
            .collect::<HashSet<_>>();
        spiller.spill(keys, context)?;
    }
    spiller.finish(context)
}

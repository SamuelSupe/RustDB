use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field, Schema};

use super::super::spill::{MergeDistinct, load_partition};
use super::*;
use crate::{
    execution::aggregate::{CellValue, GroupState, spill::partition_for_key},
    runtime::{MemoryPool, QueryContext},
    sql::{AggregateExpr, AggregateFunction},
};

fn count_aggregates(distinct: bool) -> Vec<AggregateExpr> {
    vec![AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    }]
}

#[test]
fn group_victim_spill_keeps_other_partitions_resident() {
    let groups = vec![BoundExpr::column(0, DataType::Int64, "key")];
    let aggregates = count_aggregates(false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
    ]));
    let mut by_partition = HashMap::<usize, Vec<i64>>::new();
    for key in 0..10_000_i64 {
        let cells = vec![CellValue::Int64(key)];
        by_partition
            .entry(partition_for_key(&cells, 4, 0))
            .or_default()
            .push(key);
    }
    let mut partitions = by_partition.into_values().filter(|keys| keys.len() >= 3);
    let victim = partitions.next().unwrap();
    let survivor = partitions.next().unwrap()[0];
    let keys = [victim[0], victim[1], victim[2], survivor];
    let mut states = keys
        .iter()
        .map(|key| GroupState::new(vec![CellValue::Int64(*key)], &aggregates))
        .collect::<Vec<_>>();
    let mut index = keys
        .iter()
        .enumerate()
        .map(|(position, key)| (vec![CellValue::Int64(*key)], position))
        .collect::<HashMap<_, _>>();

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(4 << 20), temp.path()).unwrap();
    let mut spiller = StateSpiller::new(&context, 4);
    let resident = spill_largest_group_partition(
        &mut states,
        &mut index,
        &groups,
        &aggregates,
        schema,
        &mut spiller,
        &context,
    )
    .unwrap();

    assert_eq!(states.len(), 1);
    assert_eq!(index.len(), 1);
    assert_eq!(states[0].key, vec![CellValue::Int64(survivor)]);
    assert_eq!(resident, resident_group_bytes(&states));
    for partition in spiller.finish(&context).unwrap() {
        for file in partition.files {
            context.spill.remove_file(&file).unwrap();
        }
    }
}

#[test]
fn high_cardinality_multi_distinct_pressure_spills_only_one_partition() {
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(8 << 20), temp.path()).unwrap();
    let key_pool = context.memory.child("victim-distinct-keys", 32 << 10);
    let mut memory = key_pool.reservation();
    let mut spiller = DistinctSpiller::new(8, &context).unwrap();
    let mut resident = HashSet::new();
    let mut inserted = HashSet::new();

    for value in 0..10_000_i64 {
        let key = DistinctKey::new(
            vec![CellValue::Int64(value % 17)],
            usize::try_from(value % 2).unwrap(),
            CellValue::Int64(value),
        );
        inserted.insert(key.clone());
        insert_distinct(key, &mut resident, &mut memory, &mut spiller, &context).unwrap();
        if spiller.has_files() {
            break;
        }
    }

    assert!(spiller.has_files());
    assert!(
        resident.len() > 1,
        "one pressure event cleared resident keys"
    );
    assert!(resident.len() < inserted.len());
    assert!(inserted.iter().any(|key| key.aggregate == 0));
    assert!(inserted.iter().any(|key| key.aggregate == 1));

    spiller
        .spill(std::mem::take(&mut resident), &context)
        .unwrap();
    memory.try_resize(0).unwrap();
    let partitions = spiller.finish(&context).unwrap();
    let mut loaded_keys = HashSet::new();
    for partition in partitions
        .into_iter()
        .filter(|partition| !partition.files.is_empty())
    {
        let files = partition.files;
        let MergeDistinct::Merged(loaded) = load_partition(
            &files,
            2,
            &context,
            context.memory.child("victim-distinct-load", 1 << 20),
        )
        .unwrap() else {
            panic!("victim partitions should fit the test merge budget");
        };
        let (keys, loaded_memory) = loaded.into_parts();
        loaded_keys.extend(keys);
        drop(loaded_memory);
        for file in files {
            context.spill.remove_file(&file).unwrap();
        }
    }
    assert_eq!(loaded_keys, inserted);
    assert_eq!(context.memory.used(), 0);
}

use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    mem::size_of,
    sync::Arc,
};

use arrow::datatypes::SchemaRef;

use crate::{
    Error, Result,
    runtime::{MemoryPool, MemoryReservation, QueryContext, SpillFile, SpillWriter},
};

use crate::execution::aggregate::spill::{SpillPartition, adaptive_spill_partitions};
use crate::execution::value::CellValue;

mod codec;

use codec::{
    corrupt, decode_row, decoded_batch_estimate, decoded_row_estimate, spill_schema,
    try_build_batch,
};

const SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct DistinctKey {
    pub(super) group: Vec<CellValue>,
    pub(super) aggregate: usize,
    pub(super) value: CellValue,
}

impl DistinctKey {
    pub(super) fn new(group: Vec<CellValue>, aggregate: usize, value: CellValue) -> Self {
        Self {
            group,
            aggregate,
            value,
        }
    }

    pub(super) fn memory_size(&self) -> usize {
        self.group
            .capacity()
            .saturating_mul(size_of::<CellValue>())
            .saturating_add(self.group.iter().map(cell_payload_bytes).sum::<usize>())
            .saturating_add(cell_payload_bytes(&self.value))
            .saturating_add(192)
    }
}

/// Writes cheap, unpartitioned runs while upstream operators are still live.
/// Partition writers are opened only after input reaches EOF and retained
/// aggregate state has been released.
pub(super) struct DistinctSpiller {
    partitions: usize,
    runs: Vec<SpillFile>,
    writer: Option<SpillWriter>,
    headroom: MemoryReservation,
    copy_headroom_bytes: usize,
    run_bytes: u64,
}

impl DistinctSpiller {
    pub(super) fn new(partitions: usize, context: &QueryContext) -> Result<Self> {
        let schema = spill_schema();
        let copy_headroom_bytes = context.spill.write_copy_headroom_bytes();
        let writer_headroom_bytes = context
            .spill
            .writer_headroom_bytes("distinct-run", schema.as_ref());
        let headroom = context
            .memory
            .try_reserve(writer_headroom_bytes)
            .map_err(|_| {
                resource_error(
                    "DISTINCT spill writer headroom",
                    writer_headroom_bytes,
                    context,
                )
            })?;
        Ok(Self {
            partitions,
            runs: Vec::new(),
            writer: None,
            headroom,
            copy_headroom_bytes,
            run_bytes: 0,
        })
    }

    pub(super) fn has_files(&self) -> bool {
        self.writer.is_some() || !self.runs.is_empty()
    }

    pub(super) fn spill(
        &mut self,
        keys: HashSet<DistinctKey>,
        context: &QueryContext,
    ) -> Result<()> {
        let vector_bytes = keys
            .len()
            .saturating_mul(size_of::<DistinctKey>())
            .saturating_add(256)
            .max(1);
        let _vector_memory = context
            .memory
            .try_reserve(vector_bytes)
            .map_err(|_| resource_error("DISTINCT run workspace", vector_bytes, context))?;
        let keys = keys.into_iter().collect::<Vec<_>>();
        let schema = spill_schema();
        if self.writer.is_none() {
            self.headroom.try_resize(0)?;
            self.writer = Some(context.spill.writer("distinct-run", Arc::clone(&schema))?);
            self.restore_copy_headroom(context)?;
        }
        write_run_batches(
            &keys,
            self.writer
                .as_mut()
                .expect("DISTINCT run writer was created above"),
            schema,
            context,
            &mut self.headroom,
            self.copy_headroom_bytes,
            &mut self.run_bytes,
        )?;
        Ok(())
    }

    pub(super) fn spill_largest_partition(
        &mut self,
        keys: &mut HashSet<DistinctKey>,
        context: &QueryContext,
    ) -> Result<usize> {
        let mut partition_bytes = vec![0usize; self.partitions];
        for key in keys.iter() {
            let partition = partition_for(key, self.partitions, 0);
            partition_bytes[partition] =
                partition_bytes[partition].saturating_add(key.memory_size());
        }
        let victim = partition_bytes
            .iter()
            .enumerate()
            .max_by_key(|(_, bytes)| *bytes)
            .map(|(partition, _)| partition)
            .ok_or_else(|| {
                Error::Internal("DISTINCT key victim selection has no partitions".into())
            })?;

        let survivor_count = keys
            .iter()
            .filter(|key| partition_for(key, self.partitions, 0) != victim)
            .count();
        let mut victim_keys = HashSet::new();
        let mut survivors = HashSet::with_capacity(survivor_count);
        for key in std::mem::take(keys) {
            if partition_for(&key, self.partitions, 0) == victim {
                victim_keys.insert(key);
            } else {
                survivors.insert(key);
            }
        }
        let resident_bytes = survivors
            .iter()
            .fold(0usize, |bytes, key| bytes.saturating_add(key.memory_size()));
        *keys = survivors;
        self.spill(victim_keys, context)?;
        Ok(resident_bytes)
    }

    pub(super) fn finish(mut self, context: &QueryContext) -> Result<Vec<SpillPartition>> {
        if let Some(writer) = self.writer.take() {
            self.headroom.try_resize(0)?;
            self.runs.push(writer.finish(1)?);
            context.check_spill_write_amplification("DistinctAggregate", 0, 0, self.run_bytes)?;
        }
        let output = partition_files(&self.runs, self.partitions, 0, 0, "distinct", context)?;
        for run in &self.runs {
            context.spill.remove_file(run)?;
        }
        Ok(output)
    }

    fn restore_copy_headroom(&mut self, context: &QueryContext) -> Result<()> {
        self.headroom
            .try_resize(self.copy_headroom_bytes)
            .map_err(|_| {
                resource_error(
                    "DISTINCT spill I/O copy headroom",
                    self.copy_headroom_bytes,
                    context,
                )
            })
    }
}

struct PartitionSpiller {
    seed: u64,
    depth: usize,
    label: String,
    clock: u64,
    partition_bytes: Vec<u64>,
    partition_estimates: Vec<u64>,
    sinks: Vec<PartitionSink>,
}

struct PartitionSink {
    files: Vec<SpillFile>,
    writer: Option<SpillWriter>,
    last_used: u64,
    uncompressed_bytes: u64,
}

impl PartitionSpiller {
    fn new(partitions: usize, seed: u64, depth: usize, label: impl Into<String>) -> Self {
        Self {
            seed,
            depth,
            label: label.into(),
            clock: 0,
            partition_bytes: vec![0; partitions],
            partition_estimates: vec![0; partitions],
            sinks: (0..partitions)
                .map(|_| PartitionSink {
                    files: Vec::new(),
                    writer: None,
                    last_used: 0,
                    uncompressed_bytes: 0,
                })
                .collect(),
        }
    }

    fn write_keys(&mut self, mut keys: Vec<DistinctKey>, context: &QueryContext) -> Result<()> {
        let partitions = self.sinks.len();
        keys.sort_unstable_by_key(|key| partition_for(key, partitions, self.seed));
        let schema = spill_schema();
        let mut start = 0;
        while start < keys.len() {
            context.check_cancelled()?;
            let partition = partition_for(&keys[start], partitions, self.seed);
            let end = start
                + keys[start..]
                    .partition_point(|key| partition_for(key, partitions, self.seed) == partition);
            self.write_partition(partition, &keys[start..end], Arc::clone(&schema), context)?;
            start = end;
        }
        Ok(())
    }

    fn write_partition(
        &mut self,
        partition: usize,
        keys: &[DistinctKey],
        schema: SchemaRef,
        context: &QueryContext,
    ) -> Result<()> {
        self.ensure_writer(partition, Arc::clone(&schema), context)?;
        let mut offset = 0;
        while offset < keys.len() {
            let mut rows = context.batch_size.max(1).min(keys.len() - offset);
            let (batch, memory) = loop {
                if let Some(built) =
                    try_build_batch(&keys[offset..offset + rows], Arc::clone(&schema), context)?
                {
                    break built;
                }
                if rows == 1 {
                    return Err(resource_error(
                        "one DISTINCT spill row",
                        keys[offset].memory_size().saturating_mul(3),
                        context,
                    ));
                }
                rows = rows.div_ceil(2);
            };
            self.ensure_copy_headroom(partition, context)?;
            let bytes = u64::try_from(batch.get_array_memory_size().max(1)).unwrap_or(u64::MAX);
            let merge_bytes = keys[offset..offset + rows]
                .iter()
                .map(DistinctKey::memory_size)
                .fold(0usize, usize::saturating_add);
            let merge_bytes = u64::try_from(merge_bytes.max(1)).unwrap_or(u64::MAX);
            let projected_partition = self.partition_bytes[partition].saturating_add(bytes);
            let projected_estimate =
                self.partition_estimates[partition].saturating_add(merge_bytes);
            let max_partition = self
                .partition_estimates
                .iter()
                .copied()
                .max()
                .unwrap_or(0)
                .max(projected_estimate);
            let first = self.sinks[partition]
                .writer
                .as_mut()
                .expect("DISTINCT partition writer was created above")
                .write_batch(&batch);
            if let Err(error) = first {
                if !matches!(error, Error::ResourceExhausted(_)) || !self.close_oldest(partition)? {
                    return Err(error);
                }
                self.sinks[partition]
                    .writer
                    .as_mut()
                    .expect("current DISTINCT writer remains active")
                    .write_batch(&batch)?;
            }
            self.partition_bytes[partition] = projected_partition;
            self.partition_estimates[partition] = projected_estimate;
            self.sinks[partition].uncompressed_bytes = self.sinks[partition]
                .uncompressed_bytes
                .saturating_add(bytes);
            let pending_write_bytes = self.pending_write_bytes();
            context.check_spill_write_amplification(
                "DistinctAggregate",
                pending_write_bytes,
                self.depth,
                max_partition,
            )?;
            drop(memory);
            offset += rows;
        }
        Ok(())
    }

    fn ensure_writer(
        &mut self,
        partition: usize,
        schema: SchemaRef,
        context: &QueryContext,
    ) -> Result<()> {
        self.clock = self.clock.saturating_add(1);
        self.sinks[partition].last_used = self.clock;
        if self.sinks[partition].writer.is_some() {
            return Ok(());
        }
        let label = format!("{}-p{partition}", self.label);
        let headroom = context.spill.writer_headroom_bytes(&label, schema.as_ref());
        while context.memory.available() < headroom && self.close_oldest(partition)? {}
        self.sinks[partition].writer = match context.spill.writer(&label, Arc::clone(&schema)) {
            Ok(writer) => Some(writer),
            Err(error @ Error::ResourceExhausted(_)) if self.close_oldest(partition)? => {
                Some(context.spill.writer(&label, schema).map_err(|_| error)?)
            }
            Err(error) => return Err(error),
        };
        Ok(())
    }

    fn ensure_copy_headroom(&mut self, partition: usize, context: &QueryContext) -> Result<()> {
        let headroom = context.spill.write_copy_headroom_bytes();
        while context.memory.available() < headroom && self.close_oldest(partition)? {}
        Ok(())
    }

    fn close_oldest(&mut self, except: usize) -> Result<bool> {
        let candidate = self
            .sinks
            .iter()
            .enumerate()
            .filter(|(index, sink)| *index != except && sink.writer.is_some())
            .min_by_key(|(_, sink)| sink.last_used)
            .map(|(index, _)| index);
        let Some(index) = candidate else {
            return Ok(false);
        };
        let writer = self.sinks[index]
            .writer
            .take()
            .expect("active DISTINCT writer was selected");
        self.sinks[index].files.push(writer.finish(1)?);
        self.sinks[index].uncompressed_bytes = 0;
        Ok(true)
    }

    fn pending_write_bytes(&self) -> u64 {
        self.sinks
            .iter()
            .filter_map(|sink| sink.writer.as_ref())
            .map(SpillWriter::pending_write_bytes)
            .fold(0u64, u64::saturating_add)
    }

    fn finish(mut self, context: &QueryContext) -> Result<Vec<SpillPartition>> {
        for sink in &mut self.sinks {
            if let Some(writer) = sink.writer.take() {
                sink.files.push(writer.finish(1)?);
                sink.uncompressed_bytes = 0;
            }
        }
        context.check_spill_write_amplification(
            "DistinctAggregate",
            0,
            self.depth,
            self.partition_estimates.iter().copied().max().unwrap_or(0),
        )?;
        context.metrics.record_repartition(
            self.partition_bytes
                .iter()
                .copied()
                .fold(0u64, u64::saturating_add),
            self.depth,
            self.partition_bytes.iter().copied().max().unwrap_or(0),
        );
        Ok(self
            .sinks
            .into_iter()
            .zip(self.partition_estimates)
            .map(|(sink, estimated_bytes)| SpillPartition {
                files: sink.files,
                estimated_bytes,
            })
            .collect())
    }
}

fn write_run_batches(
    keys: &[DistinctKey],
    writer: &mut SpillWriter,
    schema: SchemaRef,
    context: &QueryContext,
    headroom: &mut MemoryReservation,
    copy_headroom_bytes: usize,
    run_bytes: &mut u64,
) -> Result<()> {
    let mut offset = 0;
    while offset < keys.len() {
        let mut rows = context.batch_size.max(1).min(keys.len() - offset);
        let (batch, memory) = loop {
            if let Some(built) =
                try_build_batch(&keys[offset..offset + rows], Arc::clone(&schema), context)?
            {
                break built;
            }
            if rows == 1 {
                return Err(resource_error(
                    "one DISTINCT run row",
                    keys[offset].memory_size().saturating_mul(3),
                    context,
                ));
            }
            rows = rows.div_ceil(2);
        };
        headroom.try_resize(0)?;
        let bytes = u64::try_from(batch.get_array_memory_size().max(1)).unwrap_or(u64::MAX);
        context.record_spill_logical_input_bytes(bytes);
        let projected = run_bytes.saturating_add(bytes);
        writer.write_batch(&batch)?;
        context.check_spill_write_amplification(
            "DistinctAggregate",
            writer.pending_write_bytes(),
            0,
            projected,
        )?;
        *run_bytes = projected;
        drop(memory);
        headroom.try_resize(copy_headroom_bytes).map_err(|_| {
            resource_error(
                "DISTINCT spill I/O copy headroom",
                copy_headroom_bytes,
                context,
            )
        })?;
        offset += rows;
    }
    Ok(())
}

fn partition_files(
    files: &[SpillFile],
    partitions: usize,
    seed: u64,
    depth: usize,
    label: &str,
    context: &QueryContext,
) -> Result<Vec<SpillPartition>> {
    let mut spiller = PartitionSpiller::new(partitions, seed, depth, label);
    for file in files {
        for batch in context.spill.read_file(file)? {
            context.check_cancelled()?;
            let batch = batch?;
            let encoded_bytes = batch.get_array_memory_size().max(1);
            let _encoded_memory = context.memory.try_reserve(encoded_bytes).map_err(|_| {
                resource_error("one DISTINCT repartition batch", encoded_bytes, context)
            })?;
            let decoded_bytes = decoded_batch_estimate(&batch)?;
            let _decoded_memory = context.memory.try_reserve(decoded_bytes).map_err(|_| {
                resource_error("decoded DISTINCT repartition keys", decoded_bytes, context)
            })?;
            let keys = (0..batch.num_rows())
                .map(|row| decode_row(&batch, row))
                .collect::<Result<Vec<_>>>()?;
            spiller.write_keys(keys, context)?;
        }
    }
    spiller.finish(context)
}

pub(super) enum MergeDistinct {
    Merged(LoadedDistinct),
    Repartition,
}

pub(super) struct LoadedDistinct {
    keys: HashSet<DistinctKey>,
    memory: MemoryReservation,
}

impl LoadedDistinct {
    pub(super) fn into_parts(self) -> (HashSet<DistinctKey>, MemoryReservation) {
        (self.keys, self.memory)
    }
}

pub(super) fn load_partition(
    files: &[SpillFile],
    aggregate_count: usize,
    context: &QueryContext,
    memory_pool: MemoryPool,
) -> Result<MergeDistinct> {
    let mut distinct_memory = memory_pool.reservation();
    let mut keys = HashSet::<DistinctKey>::new();
    for file in files {
        for batch in context.spill.read_file(file)? {
            context.check_cancelled()?;
            let batch = batch?;
            let encoded_bytes = batch.get_array_memory_size().max(1);
            let _encoded_memory = context.memory.try_reserve(encoded_bytes).map_err(|_| {
                resource_error("one encoded DISTINCT spill batch", encoded_bytes, context)
            })?;
            for row in 0..batch.num_rows() {
                let candidate_bytes = decoded_row_estimate(&batch, row)?;
                let mut candidate = match memory_pool.try_reserve(candidate_bytes) {
                    Ok(candidate) => candidate,
                    Err(_) if !keys.is_empty() => return Ok(MergeDistinct::Repartition),
                    Err(_) => {
                        return Err(resource_error(
                            "one decoded DISTINCT key",
                            candidate_bytes,
                            context,
                        ));
                    }
                };
                let key = decode_row(&batch, row)?;
                if key.aggregate >= aggregate_count {
                    return Err(corrupt(&format!(
                        "aggregate id {} exceeds plan width {}",
                        key.aggregate, aggregate_count
                    )));
                }
                if keys.contains(&key) {
                    continue;
                }
                let bytes = key.memory_size();
                if candidate.try_resize(bytes).is_err() {
                    return if keys.is_empty() {
                        Err(resource_error("one decoded DISTINCT key", bytes, context))
                    } else {
                        Ok(MergeDistinct::Repartition)
                    };
                }
                distinct_memory.absorb(candidate)?;
                keys.insert(key);
            }
        }
    }
    Ok(MergeDistinct::Merged(LoadedDistinct {
        keys,
        memory: distinct_memory,
    }))
}

pub(super) fn repartition(
    files: &[SpillFile],
    estimated_bytes: usize,
    depth: usize,
    _partitions: usize,
    context: &QueryContext,
) -> Result<Vec<SpillPartition>> {
    let partitions = recursive_spill_partitions(context, estimated_bytes);
    partition_files(
        files,
        partitions,
        SEED_STEP.wrapping_mul(depth as u64),
        depth,
        &format!("distinct-r{depth}"),
        context,
    )
}

fn recursive_spill_partitions(context: &QueryContext, source_bytes: usize) -> usize {
    let lanes = context.scheduler.partitioning_lanes();
    let default_target = context
        .memory
        .limit()
        .checked_div(lanes.saturating_mul(2))
        .unwrap_or(0)
        .clamp(8 << 20, 64 << 20);
    let target = context
        .execution
        .spill_partition_target_bytes
        .unwrap_or(default_target)
        .max(1);
    let memory_pressure_floor = target
        .div_ceil(context.memory.limit().max(1))
        .checked_next_power_of_two()
        .unwrap_or(256)
        .clamp(2, 256);
    let memory_pressure_floor = if target > context.memory.limit() {
        memory_pressure_floor.max(32)
    } else {
        memory_pressure_floor
    };
    adaptive_spill_partitions(context, source_bytes).max(memory_pressure_floor)
}

fn partition_for(key: &DistinctKey, partitions: usize, seed: u64) -> usize {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % partitions
}

fn cell_payload_bytes(value: &CellValue) -> usize {
    match value {
        CellValue::Utf8(value) => value.capacity(),
        CellValue::Binary(value) => value.capacity(),
        _ => 0,
    }
}

fn resource_error(kind: &str, bytes: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "DISTINCT aggregate spill requires {bytes} bytes for {kind} (query limit {}, available {})",
        context.memory.limit(),
        context.memory.available()
    ))
}

#[cfg(test)]
mod tests;

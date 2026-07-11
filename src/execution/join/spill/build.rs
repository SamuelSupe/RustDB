use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{ArrayData, ArrayRef},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};

use crate::{
    Result,
    runtime::{MemoryReservation, QueryContext, SpillFile},
};

const COMPACTION_FAN_IN: usize = 64;
const HASH_TABLE_BYTES_PER_ROW: usize = 128;
const IPC_BLOCK_METADATA_BYTES: usize = 64;
const CONCAT_ARRAY_METADATA_BYTES: usize = 256;

pub(in crate::execution::join) enum BuildPartition {
    Loaded(RecordBatch),
    TooLarge { rows: usize },
}

#[derive(Default)]
struct BuildFootprint {
    buffer_bytes: usize,
    rows: usize,
    max_source_batch_bytes: usize,
    max_file_reader_metadata: usize,
}

impl BuildFootprint {
    fn observe_batch(&mut self, batch: &RecordBatch) {
        self.buffer_bytes = self
            .buffer_bytes
            .saturating_add(batch_logical_buffer_bytes(batch));
        self.rows = self.rows.saturating_add(batch.num_rows());
        self.max_source_batch_bytes = self
            .max_source_batch_bytes
            .max(batch.get_array_memory_size());
    }

    fn finish_file(&mut self, source_batches: usize) {
        self.max_file_reader_metadata = self
            .max_file_reader_metadata
            .max(source_batches.saturating_mul(IPC_BLOCK_METADATA_BYTES));
    }
}

pub(in crate::execution::join) fn load_build_partition(
    files: &[SpillFile],
    schema: &SchemaRef,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<BuildPartition> {
    let mut footprint = BuildFootprint::default();
    for file in files {
        let mut source_batches = 0usize;
        for batch in context.spill.read_file(file)? {
            footprint.observe_batch(&batch?);
            source_batches = source_batches.saturating_add(1);
        }
        footprint.finish_file(source_batches);
    }
    let required = estimated_build_bytes(&footprint, files.len(), schema.fields().len());
    if reservation.try_resize(required).is_err() {
        return Ok(BuildPartition::TooLarge {
            rows: footprint.rows,
        });
    }

    let mut batches = Vec::with_capacity(files.len());
    for file in files {
        if let Some(batch) = compact_spill_file(file, schema, context)? {
            batches.push(batch);
        }
    }
    let batch = compact_batches(batches, schema)?
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::clone(schema)));
    Ok(BuildPartition::Loaded(batch))
}

fn compact_spill_file(
    file: &SpillFile,
    schema: &SchemaRef,
    context: &QueryContext,
) -> Result<Option<RecordBatch>> {
    let mut pending = Vec::with_capacity(COMPACTION_FAN_IN);
    let mut compacted = Vec::new();
    for batch in context.spill.read_file(file)? {
        pending.push(batch?);
        if pending.len() == COMPACTION_FAN_IN {
            compacted.push(
                compact_batches(std::mem::take(&mut pending), schema)?
                    .expect("a full compaction group is not empty"),
            );
            pending = Vec::with_capacity(COMPACTION_FAN_IN);
        }
    }
    if !pending.is_empty() {
        compacted.push(
            compact_batches(pending, schema)?.expect("a pending compaction group is not empty"),
        );
    }
    compact_batches(compacted, schema)
}

fn compact_batches(
    mut batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<Option<RecordBatch>> {
    if batches.is_empty() {
        return Ok(None);
    }
    while batches.len() > 1 {
        let mut next = Vec::with_capacity(batches.len().div_ceil(COMPACTION_FAN_IN));
        for group in batches.chunks(COMPACTION_FAN_IN) {
            next.push(if group.len() == 1 {
                group[0].clone()
            } else {
                arrow::compute::concat_batches(schema, group)?
            });
        }
        batches = next;
    }
    Ok(batches.pop())
}

fn batch_logical_buffer_bytes(batch: &RecordBatch) -> usize {
    batch.columns().iter().fold(0usize, |bytes, array| {
        bytes.saturating_add(array_data_logical_buffer_bytes(&array.to_data()))
    })
}

fn array_data_logical_buffer_bytes(data: &ArrayData) -> usize {
    let buffers = data
        .buffers()
        .iter()
        .fold(0usize, |bytes, buffer| bytes.saturating_add(buffer.len()));
    let nulls = data.nulls().map(|nulls| nulls.buffer().len()).unwrap_or(0);
    data.child_data()
        .iter()
        .fold(buffers.saturating_add(nulls), |bytes, child| {
            bytes.saturating_add(array_data_logical_buffer_bytes(child))
        })
}

fn estimated_build_bytes(
    footprint: &BuildFootprint,
    retained_batches: usize,
    columns: usize,
) -> usize {
    let retained_metadata = size_of::<RecordBatch>()
        .saturating_add(
            columns
                .saturating_mul(size_of::<ArrayRef>().saturating_add(CONCAT_ARRAY_METADATA_BYTES)),
        )
        .saturating_mul(retained_batches.max(1));
    footprint
        .buffer_bytes
        .saturating_mul(2)
        .saturating_add(footprint.rows.saturating_mul(HASH_TABLE_BYTES_PER_ROW))
        .saturating_add(retained_metadata)
        .saturating_add(
            footprint
                .max_source_batch_bytes
                .saturating_mul(COMPACTION_FAN_IN),
        )
        .saturating_add(footprint.max_file_reader_metadata)
}

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    mem::size_of,
    sync::Arc,
};

use arrow::{
    compute::interleave_record_batch,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::{RowConverter, Rows},
};

use crate::runtime::{BatchEnvelope, MemoryReservation, QueryContext, SpillFile};
use crate::sql::SortExpr;
use crate::{Error, Result};

use super::{empty_columns_batch, evaluate_keys, make_converter};

struct RunCursor {
    reader: Box<dyn Iterator<Item = Result<RecordBatch>> + Send>,
    batch: Option<RecordBatch>,
    keys: Option<Rows>,
    row: usize,
    token: u64,
    reserved: usize,
    retained: Option<MemoryReservation>,
}

impl RunCursor {
    fn open(file: &SpillFile, context: &QueryContext) -> Result<Self> {
        let reader = context.spill.read_file(file)?;
        Ok(Self {
            reader: Box::new(reader),
            batch: None,
            keys: None,
            row: 0,
            token: 0,
            reserved: 0,
            retained: None,
        })
    }

    fn memory(run: MemoryRun) -> Self {
        let (batch, memory) = run.into_parts();
        Self {
            reader: Box::new(std::iter::once(Ok(batch))),
            batch: None,
            keys: None,
            row: 0,
            token: 0,
            reserved: 0,
            retained: Some(memory),
        }
    }

    fn key(&self) -> Vec<u8> {
        self.keys
            .as_ref()
            .expect("loaded cursor has keys")
            .row(self.row)
            .as_ref()
            .to_vec()
    }

    fn key_len(&self) -> usize {
        self.keys
            .as_ref()
            .expect("loaded cursor has keys")
            .row(self.row)
            .as_ref()
            .len()
    }

    fn has_row(&self) -> bool {
        self.batch
            .as_ref()
            .is_some_and(|batch| self.row < batch.num_rows())
    }
}

struct HeapEntry {
    key: Vec<u8>,
    run: usize,
    reserved: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.run == other.run
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) struct MergeIterator {
    cursors: Vec<RunCursor>,
    heap: BinaryHeap<HeapEntry>,
    expressions: Vec<SortExpr>,
    converter: RowConverter,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    reservation: MemoryReservation,
    batch_size: usize,
    remaining: usize,
    next_token: u64,
    pending_runs: Vec<usize>,
    held_output: Option<MemoryReservation>,
    finished: bool,
}

pub(super) struct MemoryRun {
    batch: RecordBatch,
    memory: MemoryReservation,
}

impl MemoryRun {
    pub(super) fn new(batch: RecordBatch, memory: MemoryReservation) -> Self {
        Self { batch, memory }
    }

    pub(super) fn into_parts(self) -> (RecordBatch, MemoryReservation) {
        (self.batch, self.memory)
    }
}

pub(super) enum MergeRun {
    Memory(MemoryRun),
    Spill(SpillFile),
}

impl MergeIterator {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        runs: &[SpillFile],
        expressions: Vec<SortExpr>,
        fetch: Option<usize>,
        schema: SchemaRef,
        context: Arc<QueryContext>,
        reservation: MemoryReservation,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_mixed(
            runs.iter().cloned().map(MergeRun::Spill).collect(),
            expressions,
            fetch,
            schema,
            context,
            reservation,
            batch_size,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_mixed(
        runs: Vec<MergeRun>,
        expressions: Vec<SortExpr>,
        fetch: Option<usize>,
        schema: SchemaRef,
        context: Arc<QueryContext>,
        reservation: MemoryReservation,
        batch_size: usize,
    ) -> Result<Self> {
        let converter = make_converter(&expressions)?;
        let cursors = runs
            .into_iter()
            .map(|run| match run {
                MergeRun::Memory(run) => Ok(RunCursor::memory(run)),
                MergeRun::Spill(file) => RunCursor::open(&file, &context),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut reservation = reservation;
        reservation.try_grow(cursors.capacity().saturating_mul(size_of::<RunCursor>()))?;
        let mut merge = Self {
            cursors,
            heap: BinaryHeap::new(),
            expressions,
            converter,
            schema,
            context,
            reservation,
            batch_size: batch_size.max(1),
            remaining: fetch.unwrap_or(usize::MAX),
            next_token: 0,
            pending_runs: Vec::new(),
            held_output: None,
            finished: false,
        };
        for run in 0..merge.cursors.len() {
            if merge.load_cursor(run)? {
                merge.push_cursor(run)?;
            }
        }
        Ok(merge)
    }

    fn load_cursor(&mut self, run: usize) -> Result<bool> {
        self.context.check_cancelled()?;
        loop {
            let Some(batch) = self.cursors[run].reader.next() else {
                self.cursors[run].retained.take();
                return Ok(false);
            };
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }

            let memory_run = self.cursors[run].retained.is_some();
            let estimate = if memory_run {
                estimate_key_bytes(&batch, self.expressions.len())
            } else {
                estimate_merge_bytes(&batch, self.expressions.len())
            };
            self.reservation.try_grow(estimate)?;
            let result = (|| {
                let arrays = evaluate_keys(&self.expressions, &batch)?;
                let keys = self.converter.convert_columns(&arrays)?;
                Ok::<_, Error>(keys)
            })();
            let keys = match result {
                Ok(keys) => keys,
                Err(error) => {
                    self.reservation.shrink(estimate);
                    return Err(error);
                }
            };
            let actual = if memory_run {
                keys.size().max(1)
            } else {
                batch
                    .get_array_memory_size()
                    .saturating_add(keys.size())
                    .max(1)
            };
            if actual > estimate {
                if let Err(error) = self.reservation.try_grow(actual - estimate) {
                    self.reservation.shrink(estimate);
                    return Err(error);
                }
            } else {
                self.reservation.shrink(estimate - actual);
            }
            self.context
                .metrics
                .observe_memory(self.context.memory.used());

            self.next_token = self.next_token.wrapping_add(1);
            self.cursors[run].batch = Some(batch);
            self.cursors[run].keys = Some(keys);
            self.cursors[run].row = 0;
            self.cursors[run].token = self.next_token;
            self.cursors[run].reserved = actual;
            return Ok(true);
        }
    }

    fn push_cursor(&mut self, run: usize) -> Result<()> {
        let estimated = self.cursors[run]
            .key_len()
            .saturating_add(size_of::<HeapEntry>())
            .saturating_add(16);
        self.reservation.try_grow(estimated)?;
        let key = self.cursors[run].key();
        let reserved = key
            .capacity()
            .saturating_add(size_of::<HeapEntry>())
            .saturating_add(16);
        if reserved > estimated {
            if let Err(error) = self.reservation.try_grow(reserved - estimated) {
                self.reservation.shrink(estimated);
                return Err(error);
            }
        } else {
            self.reservation.shrink(estimated - reserved);
        }
        self.heap.push(HeapEntry { key, run, reserved });
        Ok(())
    }

    fn advance_cursor(
        &mut self,
        run: usize,
        deferred_release: &mut usize,
        deferred_memory: &mut Vec<MemoryReservation>,
    ) -> Result<bool> {
        self.cursors[run].row += 1;
        if self.cursors[run].has_row() {
            self.push_cursor(run)?;
            return Ok(false);
        }

        *deferred_release = deferred_release.saturating_add(self.cursors[run].reserved);
        self.cursors[run].reserved = 0;
        self.cursors[run].batch = None;
        self.cursors[run].keys = None;
        if let Some(memory) = self.cursors[run].retained.take() {
            deferred_memory.push(memory);
        }
        self.pending_runs.push(run);
        Ok(true)
    }

    fn prepare_output(&mut self) -> Result<bool> {
        if self.remaining == 0 {
            return Ok(false);
        }
        self.context.check_cancelled()?;
        for run in std::mem::take(&mut self.pending_runs) {
            if self.load_cursor(run)? {
                self.push_cursor(run)?;
            }
        }
        if self.heap.is_empty() {
            return Ok(false);
        }
        Ok(true)
    }

    fn output_workspace_bytes(&self) -> usize {
        let target = self.batch_size.min(self.remaining);
        let row_bytes = self
            .cursors
            .iter()
            .filter_map(|cursor| cursor.batch.as_ref())
            .map(estimate_row_bytes)
            .max()
            .unwrap_or(1);
        let estimate = target
            .saturating_mul(row_bytes.saturating_mul(2).saturating_add(64))
            .saturating_add(indices_workspace_bytes(target))
            .saturating_add(self.schema.fields().len().saturating_mul(512))
            .saturating_add(1_024)
            .max(1);
        estimate.min(
            self.context
                .memory
                .limit()
                .checked_div(2)
                .unwrap_or(0)
                .max(1),
        )
    }

    fn held_bytes(&self) -> usize {
        self.cursors
            .iter()
            .filter_map(|cursor| cursor.retained.as_ref())
            .map(MemoryReservation::size)
            .fold(self.reservation.size(), usize::saturating_add)
    }

    fn materialize_output(
        &mut self,
        mut output_memory: MemoryReservation,
    ) -> Result<Option<RecordBatch>> {
        if !self.prepare_output()? {
            return Ok(None);
        }

        let requested = self.batch_size.min(self.remaining);
        let index_bytes_per_row = size_of::<(usize, usize)>().saturating_add(24);
        let target = requested
            .min(
                output_memory
                    .size()
                    .saturating_sub(1_024)
                    .checked_div(index_bytes_per_row.max(1))
                    .unwrap_or(0)
                    .max(1),
            )
            .max(1);
        let mut sources = Vec::<RecordBatch>::new();
        let mut source_index = HashMap::<u64, usize>::new();
        let mut indices = Vec::<(usize, usize)>::with_capacity(target);
        let mut deferred_release = 0_usize;
        let mut deferred_memory = Vec::new();
        let mut output_reserved = indices_workspace_bytes(target);

        while indices.len() < target {
            let Some(entry) = self.heap.pop() else {
                break;
            };
            if indices.len().is_multiple_of(1024) {
                self.context.check_cancelled()?;
            }
            let cursor = &self.cursors[entry.run];
            let batch = cursor.batch.as_ref().expect("heap cursor has a batch");
            let source = match source_index.get(&cursor.token) {
                Some(source) => *source,
                None => {
                    let source = sources.len();
                    sources.push(batch.clone());
                    source_index.insert(cursor.token, source);
                    source
                }
            };
            let row_bytes = selected_row_workspace_bytes(batch, cursor.row)?;
            if row_bytes > output_memory.size().saturating_sub(output_reserved) {
                self.heap.push(entry);
                if indices.is_empty() {
                    output_memory
                        .try_grow(
                            output_reserved
                                .saturating_add(row_bytes)
                                .saturating_sub(output_memory.size()),
                        )
                        .map_err(|_| {
                            Error::ResourceExhausted(format!(
                                "sort merge output needs {row_bytes} bytes for one row, but only {} bytes of its pre-reserved workspace remain",
                                output_memory.size().saturating_sub(output_reserved),
                            ))
                        })?;
                    continue;
                }
                break;
            }
            output_reserved = output_reserved.saturating_add(row_bytes);
            indices.push((source, cursor.row));
            let run = entry.run;
            let key_reserved = entry.reserved;
            drop(entry);
            self.reservation.shrink(key_reserved);
            if self.advance_cursor(run, &mut deferred_release, &mut deferred_memory)? {
                // Loading the next chunk before materializing this output would
                // retain both chunks and can exceed the budget. It could also
                // violate global order if other heap entries were emitted first.
                break;
            }
        }

        let result = if self.schema.fields().is_empty() {
            empty_columns_batch(Arc::clone(&self.schema), indices.len())
        } else {
            let references: Vec<_> = sources.iter().collect();
            Ok(interleave_record_batch(&references, &indices)?)
        };
        self.reservation.shrink(deferred_release);
        drop(deferred_memory);
        let batch = result?;
        output_memory.try_resize(batch.get_array_memory_size()).map_err(|_| {
            Error::ResourceExhausted(format!(
                "sort merge output batch requires {} bytes after reserving its materialization workspace",
                batch.get_array_memory_size(),
            ))
        })?;
        self.remaining = self.remaining.saturating_sub(batch.num_rows());
        self.held_output = Some(output_memory);
        Ok(Some(batch))
    }

    pub(super) async fn next_envelope(&mut self) -> Result<Option<BatchEnvelope>> {
        self.held_output.take();
        if !self.prepare_output()? {
            self.finished = true;
            return Ok(None);
        }
        let held_bytes = self.held_bytes();
        let workspace_bytes = self.output_workspace_bytes().min(
            self.context
                .memory
                .limit()
                .saturating_sub(held_bytes)
                .max(1),
        );
        let workspace = self
            .context
            .reserve_memory_while_holding(
                workspace_bytes,
                held_bytes,
                "sort merge output workspace",
            )
            .await?;
        let Some(batch) = self.materialize_output(workspace)? else {
            self.finished = true;
            return Ok(None);
        };
        let memory = self
            .held_output
            .take()
            .expect("materialized merge output retains its reservation");
        Ok(Some(BatchEnvelope::from_reservation(
            batch,
            memory,
            "sort merge output",
        )?))
    }
}

impl Iterator for MergeIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        self.held_output.take();
        let result = self.prepare_output().and_then(|ready| {
            if !ready {
                return Ok(None);
            }
            let workspace_bytes = self
                .output_workspace_bytes()
                .min(self.context.memory.available())
                .max(1);
            let workspace = self.context.memory.try_reserve(workspace_bytes)?;
            self.materialize_output(workspace)
        });
        match result {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

fn estimate_merge_bytes(batch: &RecordBatch, key_count: usize) -> usize {
    batch
        .get_array_memory_size()
        .saturating_add(
            batch
                .num_rows()
                .saturating_mul(key_count.saturating_mul(16).saturating_add(8)),
        )
        .max(1)
}

fn estimate_key_bytes(batch: &RecordBatch, key_count: usize) -> usize {
    batch
        .num_rows()
        .saturating_mul(key_count.saturating_mul(16).saturating_add(8))
        .max(1)
}

fn estimate_row_bytes(batch: &RecordBatch) -> usize {
    batch
        .get_array_memory_size()
        .checked_div(batch.num_rows().max(1))
        .unwrap_or(0)
        .saturating_add(batch.num_columns().saturating_mul(8))
        .max(1)
}

fn indices_workspace_bytes(rows: usize) -> usize {
    rows.saturating_mul(size_of::<(usize, usize)>().saturating_add(24))
        .saturating_add(1_024)
}

fn selected_row_workspace_bytes(batch: &RecordBatch, row: usize) -> Result<usize> {
    let logical = batch.columns().iter().try_fold(0usize, |bytes, column| {
        let data = column.to_data().slice(row, 1);
        Ok::<_, arrow::error::ArrowError>(bytes.saturating_add(data.get_slice_memory_size()?))
    })?;
    Ok(logical
        .saturating_mul(2)
        .saturating_add(batch.num_columns().saturating_mul(64))
        .saturating_add(size_of::<(usize, usize)>())
        .max(1))
}

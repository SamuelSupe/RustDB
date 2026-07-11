use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    fs::File,
    sync::Arc,
};

use arrow::{
    compute::interleave_record_batch,
    datatypes::SchemaRef,
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
    row::{RowConverter, Rows},
};

use crate::runtime::{MemoryReservation, QueryContext, SpillFile};
use crate::sql::SortExpr;
use crate::{Error, Result};

use super::{empty_columns_batch, evaluate_keys, make_converter};

struct RunCursor {
    reader: Box<dyn Iterator<Item = arrow::error::Result<RecordBatch>> + Send>,
    batch: Option<RecordBatch>,
    keys: Option<Rows>,
    row: usize,
    token: u64,
    reserved: usize,
}

impl RunCursor {
    fn open(file: &SpillFile) -> Result<Self> {
        let input = File::open(file.path())
            .map_err(|error| Error::io(Some(file.path().to_path_buf()), error))?;
        let reader = StreamReader::try_new_buffered(input, None)?;
        Ok(Self {
            reader: Box::new(reader),
            batch: None,
            keys: None,
            row: 0,
            token: 0,
            reserved: 0,
        })
    }

    fn key(&self) -> Vec<u8> {
        self.keys
            .as_ref()
            .expect("loaded cursor has keys")
            .row(self.row)
            .as_ref()
            .to_vec()
    }

    fn has_row(&self) -> bool {
        self.batch
            .as_ref()
            .is_some_and(|batch| self.row < batch.num_rows())
    }
}

#[derive(Eq, PartialEq)]
struct HeapEntry {
    key: Vec<u8>,
    run: usize,
}

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
    finished: bool,
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
        let converter = make_converter(&expressions)?;
        let cursors = runs
            .iter()
            .map(RunCursor::open)
            .collect::<Result<Vec<_>>>()?;
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
            finished: false,
        };
        for run in 0..merge.cursors.len() {
            if merge.load_cursor(run)? {
                merge.push_cursor(run);
            }
        }
        Ok(merge)
    }

    fn load_cursor(&mut self, run: usize) -> Result<bool> {
        self.context.check_cancelled()?;
        loop {
            let Some(batch) = self.cursors[run].reader.next() else {
                return Ok(false);
            };
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }

            let estimate = estimate_merge_bytes(&batch, self.expressions.len());
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
            let actual = batch
                .get_array_memory_size()
                .saturating_add(keys.size())
                .max(1);
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

    fn push_cursor(&mut self, run: usize) {
        self.heap.push(HeapEntry {
            key: self.cursors[run].key(),
            run,
        });
    }

    fn advance_cursor(&mut self, run: usize, deferred_release: &mut usize) -> bool {
        self.cursors[run].row += 1;
        if self.cursors[run].has_row() {
            self.push_cursor(run);
            return false;
        }

        *deferred_release = deferred_release.saturating_add(self.cursors[run].reserved);
        self.cursors[run].reserved = 0;
        self.cursors[run].batch = None;
        self.cursors[run].keys = None;
        self.pending_runs.push(run);
        true
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.context.check_cancelled()?;
        for run in std::mem::take(&mut self.pending_runs) {
            if self.load_cursor(run)? {
                self.push_cursor(run);
            }
        }
        if self.heap.is_empty() {
            return Ok(None);
        }

        let target = self.batch_size.min(self.remaining);
        let mut sources = Vec::<RecordBatch>::new();
        let mut source_index = HashMap::<u64, usize>::new();
        let mut indices = Vec::<(usize, usize)>::with_capacity(target);
        let mut deferred_release = 0_usize;
        let mut output_reserved = 0_usize;

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
            let row_bytes = estimate_row_bytes(batch);
            if let Err(error) = self.reservation.try_grow(row_bytes) {
                self.heap.push(entry);
                if indices.is_empty() {
                    return Err(error);
                }
                break;
            }
            output_reserved = output_reserved.saturating_add(row_bytes);
            indices.push((source, cursor.row));
            if self.advance_cursor(entry.run, &mut deferred_release) {
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
        self.reservation
            .shrink(output_reserved.saturating_add(deferred_release));
        let batch = result?;
        self.remaining = self.remaining.saturating_sub(batch.num_rows());
        Ok(Some(batch))
    }
}

impl Iterator for MergeIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.next_batch() {
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

fn estimate_row_bytes(batch: &RecordBatch) -> usize {
    batch
        .get_array_memory_size()
        .checked_div(batch.num_rows().max(1))
        .unwrap_or(0)
        .saturating_add(batch.num_columns().saturating_mul(8))
        .max(1)
}

use std::{mem::size_of, sync::Arc};

use crate::{
    Result,
    runtime::{BatchEnvelope, MemoryReservation, QueryContext, SpillFile},
};

pub(super) struct PartitionIndex {
    entries: Vec<(u64, u64)>,
    _memory: MemoryReservation,
}

impl PartitionIndex {
    pub(super) fn build(
        partition: &SpillFile,
        context: &Arc<QueryContext>,
        rows: u64,
    ) -> Result<Option<Self>> {
        let capacity = (context.memory.available() / 32 / size_of::<(u64, u64)>()).min(1024);
        if capacity < 2 || rows == 0 {
            return Ok(None);
        }
        let Ok(memory) = context
            .memory
            .try_reserve(capacity * size_of::<(u64, u64)>())
        else {
            return Ok(None);
        };
        let mut entries = Vec::with_capacity(capacity);
        let step = rows.div_ceil(capacity as u64);
        let mut reader = context.spill.read_file(partition)?;
        let mut row = 0u64;
        let mut next_entry = 0u64;
        loop {
            context.check_cancelled()?;
            let offset = reader.position()?;
            let Some(batch) = reader.next() else {
                break;
            };
            let batch = BatchEnvelope::try_new(batch?, &context.memory, "window index input")?;
            if batch.num_rows() == 0 {
                continue;
            }
            if row >= next_entry {
                entries.push((row, offset));
                next_entry = row.saturating_add(step);
            }
            row += batch.num_rows() as u64;
        }
        Ok(Some(Self {
            entries,
            _memory: memory,
        }))
    }

    pub(super) fn locate(&self, target: u64) -> Option<(u64, u64)> {
        let end = self.entries.partition_point(|(row, _)| *row <= target);
        end.checked_sub(1).map(|index| self.entries[index])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::PartitionIndex;
    use crate::{
        execution::window::navigation::cursor::ValueCursor,
        runtime::{MemoryPool, QueryContext},
        sql::BoundExpr,
    };

    #[test]
    fn indexed_value_cursor_matches_sequential_cursor_across_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![3, 1])),
            ],
        )
        .unwrap();
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        let second = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![2, 7])),
            ],
        )
        .unwrap();
        let third = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![4, 5])),
            ],
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(16 << 20), temp.path()).unwrap();
        let mut writer = context
            .spill
            .writer("window-index-test", Arc::clone(&schema))
            .unwrap();
        for batch in [first, empty, second, third] {
            writer.write_batch(&batch).unwrap();
        }
        let file = writer.finish(1).unwrap();
        let rows = 6;
        let index = PartitionIndex::build(&file, &context, rows)
            .unwrap()
            .expect("the test partition should receive a sparse index");
        let expression = BoundExpr::column(1, DataType::Int64, "v");
        let mut sequential =
            ValueCursor::new(&file, expression.clone(), Arc::clone(&context)).unwrap();
        let expected = (0..rows)
            .map(|row| sequential.value_at(row).unwrap())
            .collect::<Vec<_>>();
        drop(sequential);

        for (row, expected) in expected.iter().enumerate() {
            let (start, offset) = index
                .locate(row as u64)
                .expect("every row is represented by this dense test index");
            let mut indexed = ValueCursor::new_at(
                &file,
                expression.clone(),
                Arc::clone(&context),
                start,
                offset,
            )
            .unwrap();
            assert_eq!(&indexed.value_at(row as u64).unwrap(), expected);
        }
        drop(index);
        context.spill.remove_file(&file).unwrap();
        assert!(context.metrics.snapshot().spill_read_bytes > 0);
        assert_eq!(context.memory.used(), 0);
    }
}

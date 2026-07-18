use std::collections::HashMap;

use arrow::{
    array::{ArrayRef, BooleanArray, BooleanBuilder, UInt32Array},
    compute::{concat_batches, take},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
};
use futures::StreamExt;

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryBatchStream, MemoryReservation, QueryContext},
    storage::NativeDeleteVector,
};

impl super::Session {
    pub(super) async fn native_match_stream(
        &self,
        sql: &str,
        context: std::sync::Arc<QueryContext>,
    ) -> Result<(SchemaRef, MemoryBatchStream)> {
        let plan = self
            .prepare_statement_for_query(sql, Some(std::sync::Arc::clone(&context)))
            .await?;
        let crate::sql::StatementPlan::Query(plan) = plan else {
            return Err(Error::Internal(
                "native DML match query did not produce a query plan".to_owned(),
            ));
        };
        let schema = std::sync::Arc::clone(plan.schema().arrow());
        let stream =
            crate::execution::execute_internal(crate::sql::StatementPlan::Query(plan), context)
                .await?;
        Ok((schema, stream))
    }
}

pub(super) struct DeleteMatches {
    converter: RowConverter,
    keys: HashMap<Vec<u8>, ()>,
    _memory: MemoryReservation,
}

pub(super) struct UpdateMatches {
    converter: RowConverter,
    keys: HashMap<Vec<u8>, u32>,
    values: BatchEnvelope,
    key_width: usize,
    _memory: MemoryReservation,
}

impl DeleteMatches {
    pub(super) async fn collect(
        mut input: MemoryBatchStream,
        key_schema: &SchemaRef,
        context: &QueryContext,
    ) -> Result<Self> {
        let converter = converter(key_schema)?;
        let mut keys = HashMap::new();
        let mut memory = context.memory.reservation();
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            let encoded = converter.convert_columns(batch.batch().columns())?;
            for row in 0..encoded.num_rows() {
                let key = encoded.row(row).data();
                if !keys.contains_key(key) {
                    memory.try_grow(key.len().saturating_add(64))?;
                    keys.insert(key.to_vec(), ());
                }
            }
        }
        Ok(Self {
            converter,
            keys,
            _memory: memory,
        })
    }

    pub(super) fn mask(
        &self,
        batch: &RecordBatch,
        vector: &NativeDeleteVector,
        offset: u64,
    ) -> Result<BooleanArray> {
        let rows = self.converter.convert_columns(batch.columns())?;
        let mut mask = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let physical = add_rows(offset, row)?;
            mask.append_value(
                !vector.contains(physical) && self.keys.contains_key(rows.row(row).data()),
            );
        }
        Ok(mask.finish())
    }
}

impl UpdateMatches {
    pub(super) async fn collect(
        mut input: MemoryBatchStream,
        result_schema: SchemaRef,
        key_schema: &SchemaRef,
        context: &QueryContext,
    ) -> Result<Self> {
        let key_width = key_schema.fields().len();
        let converter = converter(key_schema)?;
        let mut batches = Vec::new();
        let mut envelopes = Vec::new();
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            batches.push(batch.batch().clone());
            envelopes.push(batch);
        }
        let combined = if batches.is_empty() {
            RecordBatch::new_empty(result_schema)
        } else {
            concat_batches(&result_schema, &batches)?
        };
        if combined.num_rows() > u32::MAX as usize {
            return Err(Error::ResourceExhausted(
                "UPDATE FROM match rows exceed the u32 index limit".to_owned(),
            ));
        }
        let values =
            BatchEnvelope::try_new(combined, &context.memory, "UPDATE FROM retained matches")?;
        drop(envelopes);
        let key_columns = values.batch().columns()[..key_width].to_vec();
        let encoded = converter.convert_columns(&key_columns)?;
        let mut keys = HashMap::new();
        let mut memory = context.memory.reservation();
        for row in 0..encoded.num_rows() {
            let key = encoded.row(row).data();
            if !keys.contains_key(key) {
                memory.try_grow(key.len().saturating_add(68))?;
                keys.insert(key.to_vec(), u32::try_from(row).unwrap_or(u32::MAX));
            }
        }
        Ok(Self {
            converter,
            keys,
            values,
            key_width,
            _memory: memory,
        })
    }

    pub(super) fn select(
        &self,
        batch: &RecordBatch,
        schema: SchemaRef,
        vector: &NativeDeleteVector,
        offset: u64,
    ) -> Result<(BooleanArray, RecordBatch)> {
        let rows = self.converter.convert_columns(batch.columns())?;
        let mut mask = BooleanBuilder::with_capacity(batch.num_rows());
        let mut indices = Vec::new();
        for row in 0..batch.num_rows() {
            let physical = add_rows(offset, row)?;
            let matched = (!vector.contains(physical))
                .then(|| self.keys.get(rows.row(row).data()).copied())
                .flatten();
            mask.append_value(matched.is_some());
            if let Some(index) = matched {
                indices.push(index);
            }
        }
        let indices = UInt32Array::from(indices);
        let columns = self.values.batch().columns()
            [self.key_width..self.key_width.saturating_add(schema.fields().len())]
            .iter()
            .map(|column| take(column.as_ref(), &indices, None).map_err(Into::into))
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok((mask.finish(), RecordBatch::try_new(schema, columns)?))
    }
}

fn converter(schema: &SchemaRef) -> Result<RowConverter> {
    RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )
    .map_err(Into::into)
}

fn add_rows(offset: u64, rows: usize) -> Result<u64> {
    offset
        .checked_add(u64::try_from(rows).map_err(|_| {
            Error::ResourceExhausted("native row offset does not fit in u64".to_owned())
        })?)
        .ok_or_else(|| Error::ResourceExhausted("native row offset overflow".to_owned()))
}

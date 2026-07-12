use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_stream::try_stream;
use futures::{StreamExt, stream::BoxStream};
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use super::{
    hive::HivePartitions,
    parquet_metadata::ParquetMetadata,
    parquet_reader::{QueryIo, SnapshotParquetReader},
    schema_evolution::align_batch_to_schema,
};
use crate::{
    Result,
    runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream},
    storage::{ObjectSnapshot, ObjectSource},
};

pub(super) struct ParquetMorsel {
    pub(super) file_index: usize,
    pub(super) file: ObjectSource,
    pub(super) snapshot: ObjectSnapshot,
    pub(super) metadata: ParquetMetadata,
    pub(super) projection: Vec<usize>,
    pub(super) row_group: usize,
    pub(super) row_limit: Option<usize>,
    pub(super) row_selection: Option<RowSelection>,
}

pub(super) type ParquetMorselStream = BoxStream<'static, Result<ParquetMorsel>>;

pub(super) fn scan_morsels(
    morsels: ParquetMorselStream,
    output_schema: SchemaRef,
    hive: Option<Arc<HivePartitions>>,
    context: Arc<QueryContext>,
    io_concurrency: usize,
    batch_size: usize,
    limit: Option<usize>,
) -> RecordBatchStream {
    let streams = morsels.map({
        let output_schema = Arc::clone(&output_schema);
        let hive = hive.clone();
        let context = Arc::clone(&context);
        move |morsel| match morsel {
            Ok(morsel) => morsel_stream(
                morsel,
                Arc::clone(&output_schema),
                hive.clone(),
                Arc::clone(&context),
                batch_size,
            ),
            Err(error) => {
                boxed_record_batch_stream(futures::stream::once(async move { Err(error) }))
            }
        }
    });
    let mut merged = streams.flatten_unordered(io_concurrency);
    boxed_record_batch_stream(try_stream! {
        let mut remaining = limit.unwrap_or(usize::MAX);
        while remaining > 0 {
            let Some(batch) = merged.next().await else {
                break;
            };
            context.check_cancelled()?;
            let batch = batch?;
            let batch = if batch.num_rows() > remaining {
                batch.slice(0, remaining)
            } else {
                batch
            };
            remaining = remaining.saturating_sub(batch.num_rows());
            yield batch;
        }
    })
}

pub(super) fn morsel_stream(
    morsel: ParquetMorsel,
    output_schema: SchemaRef,
    hive: Option<Arc<HivePartitions>>,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> RecordBatchStream {
    boxed_record_batch_stream(try_stream! {
        context.check_cancelled()?;
        let metadata = morsel.metadata;
        let reader = SnapshotParquetReader::new(
            &morsel.file,
            morsel.snapshot,
            Some(QueryIo::new(
                context.control.clone(),
                context.metrics.clone(),
            )),
        );
        let mask = ProjectionMask::roots(
            metadata.reader_metadata().parquet_schema(),
            morsel.projection,
        );
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            reader,
            metadata.reader_metadata().clone(),
        )
        .with_batch_size(batch_size)
        .with_projection(mask)
        .with_row_groups(vec![morsel.row_group]);
        if let Some(selection) = morsel.row_selection {
            builder = builder.with_row_selection(selection);
        }
        if let Some(limit) = morsel.row_limit {
            builder = builder.with_limit(limit);
        }
        let mut batches = builder.build()?;

        while let Some(batch) = batches.next().await {
            context.check_cancelled()?;
            let batch = align_batch(
                batch?,
                &output_schema,
                hive.as_deref(),
                morsel.file_index,
                morsel.file.uri(),
            )?;
            context.metrics.record_scan(
                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                1,
                u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
            );
            yield batch;
        }
        drop(metadata);
    })
}

pub(super) fn align_batch(
    batch: RecordBatch,
    schema: &SchemaRef,
    hive: Option<&HivePartitions>,
    file: usize,
    uri: &str,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();
    if let Some(hive) = hive {
        for field in schema.fields() {
            if batch.schema().field_with_name(field.name()).is_ok() {
                continue;
            }
            if let Some(column) = hive.array(file, field.name(), batch.num_rows())? {
                fields.push(Arc::clone(field));
                columns.push(column);
            }
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    let augmented = RecordBatch::try_new_with_options(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        columns,
        &options,
    )?;
    align_batch_to_schema(augmented, Arc::clone(schema), uri)
}

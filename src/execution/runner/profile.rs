use std::{sync::Arc, time::Instant};

use arrow::{
    array::StringArray,
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::{
    Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, OperatorHandle, QueryContext, boxed_memory_batch_stream,
    },
    sql::LogicalPlan,
};

use super::execute_plan;

pub(super) fn track_operator(
    mut input: MemoryBatchStream,
    operator: OperatorHandle,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let _timer = OperatorTimer::new(operator.clone());
        loop {
            let wait_started = Instant::now();
            let next = input.next().await;
            operator.record_wait(wait_started.elapsed());
            let Some(batch) = next else { break };
            let batch = batch?;
            operator.record_output(
                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                u64::try_from(batch.batch().get_array_memory_size()).unwrap_or(u64::MAX),
            );
            yield batch;
        }
    })
}

struct OperatorTimer {
    operator: OperatorHandle,
    started: Instant,
}

impl OperatorTimer {
    fn new(operator: OperatorHandle) -> Self {
        Self {
            operator,
            started: Instant::now(),
        }
    }
}

impl Drop for OperatorTimer {
    fn drop(&mut self) {
        self.operator.finish(self.started.elapsed());
    }
}

pub(super) fn operator_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Empty { .. } => "Empty",
        LogicalPlan::Scan { .. } => "Scan",
        LogicalPlan::Filter { .. } => "Filter",
        LogicalPlan::Projection { .. } => "Projection",
        LogicalPlan::Append { .. } => "Append",
        LogicalPlan::Repeat { .. } => "Repeat",
        LogicalPlan::Window { .. } => "Window",
        LogicalPlan::Scalarize { .. } => "Scalarize",
        LogicalPlan::DependentJoin { .. } => "DependentJoin",
        LogicalPlan::Limit { .. } => "Limit",
        LogicalPlan::Aggregate { .. } => "Aggregate",
        LogicalPlan::Sort { .. } => "Sort",
        LogicalPlan::Join { .. } => "Join",
    }
}

pub(super) fn explain_analyze_stream(
    plan: LogicalPlan,
    context: Arc<QueryContext>,
) -> Result<MemoryBatchStream> {
    let explain = plan.explain_for_query(&context);
    let mut input = execute_plan(plan, Arc::clone(&context));
    let schema = explain_schema();
    Ok(boxed_memory_batch_stream(async_stream::try_stream! {
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            context.metrics.record_output(
                u64::try_from(batch.batch().num_rows()).unwrap_or(u64::MAX),
                1,
                u64::try_from(batch.batch().get_array_memory_size()).unwrap_or(u64::MAX),
            );
        }
        // The public EXPLAIN row describes the analyzed query; it is not part
        // of that query's result metrics.
        context.metrics.seal_output();
        context.metrics.finish();
        let metrics = context.metrics.snapshot();
        let mut summary = format!(
            "{explain}\nGlobal Metrics\n  elapsed={:?} query_admission_wait={:?}\n  sql_parse={:?} table_function_prepare={:?} bind={:?} provider_prepare={:?} optimize={:?}\n  native_verification={:?} native_full_verification_segments={}\n  scanned_rows={} scanned_batches={} scanned_bytes={}\n  returned_rows={} returned_batches={} returned_bytes={}\n  discovered_files={} files_pruned={} row_groups_pruned={}\n  parquet_page_index_bytes={} parquet_bloom_bytes={} pages_pruned={} page_rows_pruned={} bloom_row_groups_pruned={} pruning_budget_skips={}\n  s3_requests={} s3_bytes={}\n  csv_source_bytes={} csv_decompressed_bytes={} csv_morsels={} csv_parser_lanes={}\n  metadata_cache_hits={} metadata_cache_misses={} metadata_singleflight_wait={:?}\n  peak_memory_bytes={} peak_active_lanes={} scheduler_wait={:?}\n  spill_bytes={} spill_read_bytes={} spill_write_bytes={} spill_logical_input_bytes={} spill_write_amplification_millionths={} active_spill_bytes={} peak_active_spill_bytes={}\n  spill_files={} active_spill_files={} peak_active_spill_files={} spill_partitions={} repartition_bytes={} max_repartition_depth={} max_partition_bytes={} quota_rejections={}\n  join_candidate_pairs={} join_short_circuits={} runtime_filter_hits={} cancel_to_quiesce={:?}\n",
            metrics.elapsed,
            metrics.query_admission_wait,
            metrics.sql_parse_time,
            metrics.table_function_prepare_time,
            metrics.bind_time,
            metrics.provider_prepare_time,
            metrics.optimize_time,
            metrics.native_verification_time,
            metrics.native_full_verification_segments,
            metrics.rows_scanned,
            metrics.batches_scanned,
            metrics.bytes_scanned,
            metrics.rows_returned,
            metrics.batches_returned,
            metrics.bytes_returned,
            metrics.discovered_files,
            metrics.files_pruned,
            metrics.row_groups_pruned,
            metrics.parquet_page_index_bytes_read,
            metrics.parquet_bloom_filter_bytes_read,
            metrics.parquet_pages_pruned,
            metrics.parquet_page_rows_pruned,
            metrics.parquet_bloom_row_groups_pruned,
            metrics.parquet_pruning_budget_skips,
            metrics.s3_requests,
            metrics.s3_bytes_transferred,
            metrics.csv_source_bytes,
            metrics.csv_decompressed_bytes,
            metrics.csv_morsels,
            metrics.peak_csv_parser_lanes,
            metrics.metadata_cache_hits,
            metrics.metadata_cache_misses,
            metrics.metadata_singleflight_wait,
            metrics.peak_memory_bytes,
            metrics.peak_active_lanes,
            metrics.scheduler_wait,
            metrics.spill_bytes,
            metrics.spill_read_bytes,
            metrics.spill_write_bytes,
            metrics.spill_logical_input_bytes,
            metrics.spill_write_amplification_millionths,
            metrics.active_spill_bytes,
            metrics.peak_active_spill_bytes,
            metrics.spill_files,
            metrics.active_spill_files,
            metrics.peak_active_spill_files,
            metrics.spill_partitions,
            metrics.spill_repartition_bytes,
            metrics.max_repartition_depth,
            metrics.max_spill_partition_bytes,
            metrics.spill_quota_rejections,
            metrics.join_candidate_pairs,
            metrics.join_short_circuits,
            metrics.runtime_filter_hits,
            metrics.cancel_to_quiesce,
        );
        if !metrics.operators.is_empty() {
            summary.push_str("Operator Metrics\n");
            for operator in &metrics.operators {
                summary.push_str(&format!(
                    "  id={} parent={:?} name={} input_rows={} input_batches={} output_rows={} output_batches={} output_bytes={} elapsed={:?} wait={:?}\n",
                    operator.id,
                    operator.parent_id,
                    operator.name,
                    operator.input_rows,
                    operator.input_batches,
                    operator.output_rows,
                    operator.output_batches,
                    operator.output_bytes,
                    operator.elapsed,
                    operator.wait,
                ));
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![summary]))],
        )?;
        yield BatchEnvelope::try_new(batch, &context.memory, "explain analyze")?;
    }))
}

fn explain_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "explain_value",
        arrow::datatypes::DataType::Utf8,
        false,
    )]))
}

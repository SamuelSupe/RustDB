use std::sync::Arc;

use arrow::{
    array::StringArray,
    datatypes::{Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use futures::{StreamExt, stream};

use crate::Result;
use crate::runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream};
use crate::sql::{LogicalPlan, StatementPlan};

use super::{aggregate, expr, join, pipeline, scalar, scan, sort, window};

pub(super) async fn execute(
    plan: StatementPlan,
    context: Arc<QueryContext>,
) -> Result<MemoryBatchStream> {
    match plan {
        StatementPlan::Query(plan) => {
            prepare_plan_if_needed(&plan, Arc::clone(&context)).await?;
            Ok(execute_plan(plan, context))
        }
        StatementPlan::Explain(plan) => explain_stream(plan.explain_for_query(&context), context),
        StatementPlan::ExplainAnalyze(plan) => {
            prepare_plan_if_needed(&plan, Arc::clone(&context)).await?;
            explain_analyze_stream(plan, context)
        }
    }
}

async fn prepare_plan_if_needed(plan: &LogicalPlan, context: Arc<QueryContext>) -> Result<()> {
    if context.object_snapshots_sealed() {
        return Ok(());
    }
    prepare_plan(plan, Arc::clone(&context)).await?;
    context.seal_object_snapshots();
    Ok(())
}

pub(super) async fn prepare_plan(plan: &LogicalPlan, context: Arc<QueryContext>) -> Result<()> {
    let mut providers = Vec::new();
    plan.collect_scan_providers(&mut providers);
    for provider in providers {
        context.check_cancelled()?;
        provider.prepare(Arc::clone(&context)).await?;
    }
    Ok(())
}

fn execute_plan(plan: LogicalPlan, context: Arc<QueryContext>) -> MemoryBatchStream {
    let plan = match pipeline::try_execute(plan, Arc::clone(&context)) {
        Ok(stream) => return stream,
        Err(plan) => *plan,
    };
    let batch_size = context.batch_size;
    match plan {
        LogicalPlan::Empty {
            produce_one_row,
            schema,
        } => {
            let result = empty_batch(Arc::clone(schema.arrow()), produce_one_row);
            boxed_memory_batch_stream(stream::once(async move {
                let batch = result?;
                BatchEnvelope::try_new(batch, &context.memory, "empty input")
            }))
        }
        LogicalPlan::Scan {
            provider,
            projection,
            pushed_filter,
            limit,
            schema,
            ..
        } => boxed_memory_batch_stream(async_stream::try_stream! {
            let mut input = scan::scan(
                provider,
                projection,
                pushed_filter.as_ref(),
                limit,
                Arc::clone(schema.arrow()),
                Arc::clone(&context),
                batch_size,
            ).await?;
            while let Some(batch) = input.next().await {
                context.check_cancelled()?;
                yield batch?;
            }
        }),
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            let mut input = execute_plan(*input, Arc::clone(&context));
            boxed_memory_batch_stream(async_stream::try_stream! {
                while let Some(batch) = input.next().await {
                    context.check_cancelled()?;
                    let batch = batch?;
                    let workspace = context
                        .reserve_memory_while_holding(
                            expr::filter_workspace_bytes(&predicate, batch.batch()),
                            batch.memory_size(),
                            "filter workspace",
                        )
                        .await?;
                    let filtered = expr::filter(&predicate, batch.batch())?;
                    if filtered.num_rows() != 0 {
                        yield batch.replace_with_reservation(filtered, workspace, "filter")?;
                    }
                }
            })
        }
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => {
            let mut input = execute_plan(*input, Arc::clone(&context));
            boxed_memory_batch_stream(async_stream::try_stream! {
                while let Some(batch) = input.next().await {
                    context.check_cancelled()?;
                    let batch = batch?;
                    let workspace = context
                        .reserve_memory_while_holding(
                            expr::projection_workspace_bytes(&expressions, batch.batch()),
                            batch.memory_size(),
                            "projection workspace",
                        )
                        .await?;
                    let projected = expr::project(
                        &expressions,
                        Arc::clone(schema.arrow()),
                        batch.batch(),
                    )?;
                    yield batch.replace_with_reservation(projected, workspace, "projection")?;
                }
            })
        }
        LogicalPlan::Append { inputs, .. } => {
            boxed_memory_batch_stream(async_stream::try_stream! {
                for input in inputs {
                    let mut input = execute_plan(input, Arc::clone(&context));
                    while let Some(batch) = input.next().await {
                        context.check_cancelled()?;
                        yield batch?;
                    }
                }
            })
        }
        LogicalPlan::Window {
            input,
            expressions,
            schema,
        } => {
            let input_schema = Arc::clone(input.schema().arrow());
            window::window(
                execute_plan(*input, Arc::clone(&context)),
                expressions,
                input_schema,
                Arc::clone(schema.arrow()),
                context,
                batch_size,
            )
        }
        LogicalPlan::Scalarize { input, schema } => scalar::scalarize(
            execute_plan(*input, Arc::clone(&context)),
            Arc::clone(schema.arrow()),
            context,
        ),
        LogicalPlan::DependentJoin { .. } => boxed_memory_batch_stream(stream::once(async {
            Err(crate::Error::Internal(
                "DependentJoin reached physical execution without decorrelation".into(),
            ))
        })),
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            ..
        } => limit_stream(
            execute_plan(*input, Arc::clone(&context)),
            offset,
            limit,
            context,
        ),
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => aggregate::aggregate(
            execute_plan(*input, Arc::clone(&context)),
            group_exprs,
            aggregate_exprs,
            Arc::clone(schema.arrow()),
            context,
            batch_size,
        ),
        LogicalPlan::Sort {
            input,
            expressions,
            fetch,
            schema,
        } => sort::sort(
            execute_plan(*input, Arc::clone(&context)),
            expressions,
            fetch,
            Arc::clone(schema.arrow()),
            context,
            batch_size,
        ),
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        } => {
            let left_schema = Arc::clone(left.schema().arrow());
            let right_schema = Arc::clone(right.schema().arrow());
            join::join_with_null_keys(
                execute_plan(*left, Arc::clone(&context)),
                execute_plan(*right, Arc::clone(&context)),
                on,
                null_equal_keys,
                residual,
                null_aware,
                left_schema,
                right_schema,
                join_type,
                Arc::clone(schema.arrow()),
                context,
                batch_size,
            )
        }
    }
}

fn limit_stream(
    input: MemoryBatchStream,
    mut offset: usize,
    limit: Option<usize>,
    context: Arc<QueryContext>,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let mut input = Some(input);
        let mut remaining = limit.unwrap_or(usize::MAX);
        while remaining != 0 {
            let Some(batch) = input.as_mut().expect("LIMIT input is live").next().await else {
                break;
            };
            context.check_cancelled()?;
            let batch = batch?;
            if offset >= batch.batch().num_rows() {
                offset -= batch.batch().num_rows();
                continue;
            }
            let available = batch.batch().num_rows() - offset;
            let length = available.min(remaining);
            let sliced = batch.batch().slice(offset, length);
            let output = batch.replace(sliced, "limit")?;
            offset = 0;
            remaining -= length;
            if remaining == 0 {
                // Drop the fused pipeline and its local cancellation guard
                // before exposing the terminal batch to a slow consumer.
                drop(input.take());
            }
            yield output;
        }
    })
}

fn empty_batch(schema: Arc<Schema>, one_row: bool) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(usize::from(one_row)));
    Ok(RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &options,
    )?)
}

fn explain_stream(explain: String, context: Arc<QueryContext>) -> Result<MemoryBatchStream> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "explain_value",
        arrow::datatypes::DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![explain]))])?;
    Ok(boxed_memory_batch_stream(stream::once(async move {
        BatchEnvelope::try_new(batch, &context.memory, "explain")
    })))
}

fn explain_analyze_stream(
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
        let summary = format!(
            "{explain}\nGlobal Metrics\n  elapsed={:?}\n  scanned_rows={} scanned_batches={} scanned_bytes={}\n  returned_rows={} returned_batches={} returned_bytes={}\n  discovered_files={} files_pruned={} row_groups_pruned={}\n  parquet_page_index_bytes={} parquet_bloom_bytes={} pages_pruned={} page_rows_pruned={} bloom_row_groups_pruned={} pruning_budget_skips={}\n  s3_requests={} s3_bytes={}\n  peak_memory_bytes={} peak_active_lanes={} scheduler_wait={:?}\n  spill_bytes={} spill_read_bytes={} spill_write_bytes={} spill_files={} spill_partitions={} quota_rejections={}\n",
            metrics.elapsed,
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
            metrics.peak_memory_bytes,
            metrics.peak_active_lanes,
            metrics.scheduler_wait,
            metrics.spill_bytes,
            metrics.spill_read_bytes,
            metrics.spill_write_bytes,
            metrics.spill_files,
            metrics.spill_partitions,
            metrics.spill_quota_rejections,
        );
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

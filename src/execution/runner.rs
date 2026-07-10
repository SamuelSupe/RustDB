use std::sync::Arc;

use arrow::{
    array::StringArray,
    datatypes::{Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use futures::{StreamExt, stream};

use crate::Result;
use crate::runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream};
use crate::sql::{LogicalPlan, StatementPlan};

use super::{aggregate, expr, join, scalar, scan, sort};

pub(super) async fn execute(
    plan: StatementPlan,
    context: Arc<QueryContext>,
) -> Result<RecordBatchStream> {
    match plan {
        StatementPlan::Query(plan) => {
            prepare_plan_if_needed(&plan, Arc::clone(&context)).await?;
            Ok(execute_plan(plan, context))
        }
        StatementPlan::Explain(plan) => explain_stream(plan.explain()),
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

fn execute_plan(plan: LogicalPlan, context: Arc<QueryContext>) -> RecordBatchStream {
    let batch_size = context.batch_size;
    match plan {
        LogicalPlan::Empty {
            produce_one_row,
            schema,
        } => {
            let result = empty_batch(Arc::clone(schema.arrow()), produce_one_row);
            boxed_record_batch_stream(stream::once(async move { result }))
        }
        LogicalPlan::Scan {
            provider,
            projection,
            pushed_filter,
            limit,
            schema,
            ..
        } => boxed_record_batch_stream(async_stream::try_stream! {
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
            boxed_record_batch_stream(async_stream::try_stream! {
                while let Some(batch) = input.next().await {
                    context.check_cancelled()?;
                    let filtered = expr::filter(&predicate, &batch?)?;
                    if filtered.num_rows() != 0 {
                        yield filtered;
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
            boxed_record_batch_stream(async_stream::try_stream! {
                while let Some(batch) = input.next().await {
                    context.check_cancelled()?;
                    yield expr::project(&expressions, Arc::clone(schema.arrow()), &batch?)?;
                }
            })
        }
        LogicalPlan::Scalarize { input, schema } => scalar::scalarize(
            execute_plan(*input, Arc::clone(&context)),
            Arc::clone(schema.arrow()),
            context,
        ),
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
            join_type,
            schema,
        } => {
            let left_schema = Arc::clone(left.schema().arrow());
            let right_schema = Arc::clone(right.schema().arrow());
            join::join(
                execute_plan(*left, Arc::clone(&context)),
                execute_plan(*right, Arc::clone(&context)),
                on,
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
    mut input: RecordBatchStream,
    mut offset: usize,
    limit: Option<usize>,
    context: Arc<QueryContext>,
) -> RecordBatchStream {
    boxed_record_batch_stream(async_stream::try_stream! {
        let mut remaining = limit.unwrap_or(usize::MAX);
        while remaining != 0 {
            let Some(batch) = input.next().await else { break };
            context.check_cancelled()?;
            let batch = batch?;
            if offset >= batch.num_rows() {
                offset -= batch.num_rows();
                continue;
            }
            let available = batch.num_rows() - offset;
            let length = available.min(remaining);
            yield batch.slice(offset, length);
            offset = 0;
            remaining -= length;
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

fn explain_stream(explain: String) -> Result<RecordBatchStream> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "explain_value",
        arrow::datatypes::DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![explain]))])?;
    Ok(boxed_record_batch_stream(stream::once(
        async move { Ok(batch) },
    )))
}

fn explain_analyze_stream(
    plan: LogicalPlan,
    context: Arc<QueryContext>,
) -> Result<RecordBatchStream> {
    let explain = plan.explain();
    let mut input = execute_plan(plan, Arc::clone(&context));
    let schema = explain_schema();
    Ok(boxed_record_batch_stream(async_stream::try_stream! {
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            context.metrics.record_output(
                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                1,
                u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
            );
        }
        context.metrics.finish();
        let metrics = context.metrics.snapshot();
        let summary = format!(
            "{explain}\nGlobal Metrics\n  elapsed={:?}\n  scanned_rows={} scanned_batches={} scanned_bytes={}\n  returned_rows={} returned_batches={} returned_bytes={}\n  files_pruned={} row_groups_pruned={}\n  s3_requests={} s3_bytes={}\n  peak_memory_bytes={} spill_bytes={} spill_partitions={}\n",
            metrics.elapsed,
            metrics.rows_scanned,
            metrics.batches_scanned,
            metrics.bytes_scanned,
            metrics.rows_returned,
            metrics.batches_returned,
            metrics.bytes_returned,
            metrics.files_pruned,
            metrics.row_groups_pruned,
            metrics.s3_requests,
            metrics.s3_bytes_transferred,
            metrics.peak_memory_bytes,
            metrics.spill_bytes,
            metrics.spill_partitions,
        );
        yield RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![summary]))],
        )?;
    }))
}

fn explain_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "explain_value",
        arrow::datatypes::DataType::Utf8,
        false,
    )]))
}

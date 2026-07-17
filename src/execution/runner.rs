use std::{sync::Arc, time::Instant};

use arrow::{
    array::StringArray,
    datatypes::{Field, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use futures::{StreamExt, stream};

use crate::Result;
use crate::runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream};
use crate::sql::{BoundExpr, JoinType, LogicalPlan, StatementPlan};

use super::{aggregate, expr, join, pipeline, repeat, runtime_filter, scalar, scan, sort, window};

mod profile;
use profile::{explain_analyze_stream, operator_name, track_operator};

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
        let started = Instant::now();
        let prepared = provider.prepare(Arc::clone(&context)).await;
        context
            .metrics
            .record_provider_prepare_time(started.elapsed());
        prepared?;
    }
    Ok(())
}

fn execute_plan(plan: LogicalPlan, context: Arc<QueryContext>) -> MemoryBatchStream {
    execute_plan_with_parent(plan, context, None)
}

fn execute_plan_with_parent(
    plan: LogicalPlan,
    context: Arc<QueryContext>,
    parent_id: Option<u64>,
) -> MemoryBatchStream {
    let plan = match pipeline::try_execute(plan, Arc::clone(&context), parent_id) {
        Ok(stream) => return stream,
        Err(plan) => *plan,
    };
    let operator = context
        .metrics
        .register_operator(operator_name(&plan), parent_id);
    let operator_id = operator.id();
    let input = execute_plan_inner(plan, context, operator_id);
    track_operator(input, operator)
}

fn execute_plan_inner(
    plan: LogicalPlan,
    context: Arc<QueryContext>,
    parent_id: u64,
) -> MemoryBatchStream {
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
            exact_filter,
            limit,
            schema,
            ..
        } => boxed_memory_batch_stream(async_stream::try_stream! {
            let filter = match exact_filter {
                Some(predicate) => scan::ScanFilter::Exact(predicate),
                None => pushed_filter
                    .as_ref()
                    .map_or(scan::ScanFilter::None, scan::ScanFilter::BestEffort),
            };
            let mut input = scan::scan(
                provider,
                projection,
                filter,
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
            let mut input = execute_plan_with_parent(*input, Arc::clone(&context), Some(parent_id));
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
            let mut input = execute_plan_with_parent(*input, Arc::clone(&context), Some(parent_id));
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
                    let mut input = execute_plan_with_parent(input, Arc::clone(&context), Some(parent_id));
                    while let Some(batch) = input.next().await {
                        context.check_cancelled()?;
                        yield batch?;
                    }
                }
            })
        }
        LogicalPlan::Repeat {
            input,
            count,
            schema,
        } => repeat::repeat(
            execute_plan_with_parent(*input, Arc::clone(&context), Some(parent_id)),
            count,
            Arc::clone(schema.arrow()),
            context,
            batch_size,
        ),
        LogicalPlan::Window {
            input,
            expressions,
            schema,
        } => {
            let input_schema = Arc::clone(input.schema().arrow());
            window::window(
                execute_plan_with_parent(*input, Arc::clone(&context), Some(parent_id)),
                expressions,
                input_schema,
                Arc::clone(schema.arrow()),
                context,
                batch_size,
            )
        }
        LogicalPlan::Scalarize { input, schema } => scalar::scalarize(
            execute_plan_with_parent(*input, Arc::clone(&context), Some(parent_id)),
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
            execute_plan_with_parent(*input, Arc::clone(&context), Some(parent_id)),
            offset,
            limit,
            context,
        ),
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => match aggregate::join_match::match_plan(*input, &group_exprs, &aggregate_exprs) {
            Ok(mut matched) => {
                let join_operator = context.metrics.register_operator("Join", Some(parent_id));
                let join_id = join_operator.id();
                let runtime_filter = install_join_runtime_filter(
                    &context,
                    &mut matched.left,
                    &matched.on,
                    JoinType::Inner,
                );
                let left_schema = Arc::clone(matched.left.schema().arrow());
                let right_schema = Arc::clone(matched.right.schema().arrow());
                join::join_global_aggregate(
                    execute_join_input(*matched.left, Arc::clone(&context), Some(join_id)),
                    execute_join_input(*matched.right, Arc::clone(&context), Some(join_id)),
                    matched.on,
                    left_schema,
                    right_schema,
                    Arc::clone(matched.schema.arrow()),
                    matched.aggregates,
                    Arc::clone(schema.arrow()),
                    context,
                    batch_size,
                    runtime_filter,
                    join_operator,
                )
            }
            Err(input) => {
                let input = match pipeline::try_execute_grouped(
                    input,
                    &group_exprs,
                    &aggregate_exprs,
                    Arc::clone(&context),
                    Some(parent_id),
                ) {
                    Ok(stream) => stream,
                    Err(input) => {
                        execute_plan_with_parent(*input, Arc::clone(&context), Some(parent_id))
                    }
                };
                aggregate::aggregate(
                    input,
                    group_exprs,
                    aggregate_exprs,
                    Arc::clone(schema.arrow()),
                    context,
                    batch_size,
                )
            }
        },
        LogicalPlan::Sort {
            input,
            expressions,
            fetch,
            schema,
        } => sort::sort(
            execute_plan_with_parent(*input, Arc::clone(&context), Some(parent_id)),
            expressions,
            fetch,
            Arc::clone(schema.arrow()),
            context,
            batch_size,
        ),
        LogicalPlan::Join {
            mut left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        } => {
            let runtime_filter = install_join_runtime_filter(&context, &mut left, &on, join_type);
            let left_schema = Arc::clone(left.schema().arrow());
            let right_schema = Arc::clone(right.schema().arrow());
            join::join_with_runtime_filter(
                execute_plan_with_parent(*left, Arc::clone(&context), Some(parent_id)),
                execute_plan_with_parent(*right, Arc::clone(&context), Some(parent_id)),
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
                runtime_filter,
                Some(parent_id),
            )
        }
    }
}

fn execute_join_input(
    plan: LogicalPlan,
    context: Arc<QueryContext>,
    parent_id: Option<u64>,
) -> MemoryBatchStream {
    match pipeline::try_execute_join_input(plan, Arc::clone(&context), parent_id) {
        Ok(stream) => stream,
        Err(plan) => execute_plan_with_parent(*plan, context, parent_id),
    }
}

fn install_join_runtime_filter(
    context: &QueryContext,
    left: &mut LogicalPlan,
    on: &[(BoundExpr, BoundExpr)],
    join_type: JoinType,
) -> Option<Arc<runtime_filter::RuntimeFilterSlot>> {
    if context.execution.runtime_filter_bytes != 0
        && matches!(join_type, JoinType::Inner | JoinType::Semi)
        && on.len() == 1
    {
        runtime_filter::install(left, &on[0].0)
    } else {
        None
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

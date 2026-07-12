use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;

use crate::{
    Result,
    runtime::{MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
    sql::{AggregateExpr, BoundExpr},
};

use super::spill::remove_files;
use super::{
    GroupState, MergeOutcome, OutputMode, PartitionTask, SPILL_PARTITIONS, StateSpiller,
    build_output_envelope, cell, merge_partition, partial_schema, repartition_partition,
    spill_states,
};

mod parallel;
mod spill;
mod state;

use spill::{DistinctKey, DistinctSpiller};

pub(super) fn aggregate(
    mut input: MemoryBatchStream,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let state_pool = context.memory.child(
            format!("distinct-aggregate-state-{}", context.query_id),
            state::state_pool_limit(context.memory.limit()),
        );
        let distinct_pool = context.memory.child(
            format!("distinct-aggregate-keys-{}", context.query_id),
            state::key_pool_limit(context.memory.limit()),
        );
        let mut state_memory = state_pool.reservation();
        let mut distinct_memory = distinct_pool.reservation();
        let partial_schema = partial_schema(&groups, &aggregates);
        let mut states = Vec::<GroupState>::new();
        let mut group_index = HashMap::<Vec<super::CellValue>, usize>::new();
        let mut distinct_keys = HashSet::<DistinctKey>::new();
        let mut state_spiller: Option<StateSpiller> = None;
        let mut distinct_spiller = DistinctSpiller::new(
            state::partition_count(context.memory.limit()),
            &context,
        )?;
        let workspace_expressions = groups
            .iter()
            .cloned()
            .chain(aggregates.iter().filter_map(|aggregate| aggregate.expr.clone()))
            .collect::<Vec<_>>();

        if groups.is_empty() {
            state::insert_group(
                Vec::new(),
                &groups,
                &aggregates,
                &mut states,
                &mut group_index,
                &mut state_memory,
                &partial_schema,
                &mut state_spiller,
                &context,
            )?;
        }

        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            let workspace_estimate = super::super::expr::projection_workspace_bytes(
                &workspace_expressions,
                batch.batch(),
            );
            let mut workspace = context.memory.try_reserve(workspace_estimate.max(1))?;
            let group_arrays = groups
                .iter()
                .map(|expression| super::evaluate(expression, batch.batch()))
                .collect::<Result<Vec<_>>>()?;
            let aggregate_arrays = aggregates
                .iter()
                .map(|aggregate| {
                    aggregate
                        .expr
                        .as_ref()
                        .map(|expression| super::evaluate(expression, batch.batch()))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            workspace.try_resize(state::retained_arrays_bytes(
                batch.batch(),
                &group_arrays,
                &aggregate_arrays,
            ))?;
            context.metrics.observe_memory(context.memory.used());

            for row in 0..batch.batch().num_rows() {
                context.check_cancelled()?;
                let group = group_arrays
                    .iter()
                    .map(|array| cell(array, row))
                    .collect::<Result<Vec<_>>>()?;
                let group_index_value = if let Some(index) = group_index.get(&group) {
                    *index
                } else {
                    state::insert_group(
                        group.clone(),
                        &groups,
                        &aggregates,
                        &mut states,
                        &mut group_index,
                        &mut state_memory,
                        &partial_schema,
                        &mut state_spiller,
                        &context,
                    )?
                };

                for (aggregate_index, aggregate) in aggregates.iter().enumerate() {
                    let value = aggregate_arrays[aggregate_index]
                        .as_ref()
                        .map(|array| cell(array, row))
                        .transpose()?;
                    if aggregate.distinct {
                        let Some(value) = value.filter(|value| !value.is_null()) else {
                            continue;
                        };
                        state::insert_distinct(
                            DistinctKey::new(group.clone(), aggregate_index, value),
                            &mut distinct_keys,
                            &mut distinct_memory,
                            &mut distinct_spiller,
                            &context,
                        )?;
                    } else {
                        states[group_index_value].aggregates[aggregate_index]
                            .update(aggregate, value)?;
                    }
                }
            }
        }

        if state_spiller.is_none() && !distinct_spiller.has_files() {
            state::apply_in_memory(
                distinct_keys,
                &group_index,
                &mut states,
                &aggregates,
            )?;
            distinct_memory.try_resize(0)?;
            for chunk in states.chunks(batch_size.max(1)) {
                context.check_cancelled()?;
                yield build_output_envelope(
                    chunk,
                    &groups,
                    &aggregates,
                    Arc::clone(&schema),
                    OutputMode::Final,
                    &context,
                    state_memory.size(),
                ).await?;
            }
            return;
        }

        let active_spiller = state_spiller.get_or_insert_with(|| {
            StateSpiller::new(&context, SPILL_PARTITIONS)
        });
        spill_states(
            &mut states,
            &mut group_index,
            &groups,
            &aggregates,
            Arc::clone(&partial_schema),
            active_spiller,
            &context,
        )?;
        state_memory.try_resize(0)?;

        if distinct_spiller.has_files() {
            if !distinct_keys.is_empty() {
                distinct_spiller.spill(distinct_keys, &context)?;
            }
            distinct_memory.try_resize(0)?;
            active_spiller.close_writers()?;
            parallel::merge_partitions(
                distinct_spiller.finish(&context)?,
                &groups,
                &aggregates,
                Arc::clone(&partial_schema),
                active_spiller,
                Arc::clone(&context),
                distinct_pool,
                state_pool,
            ).await?;
        } else {
            state::contribute(
                distinct_keys,
                &groups,
                &aggregates,
                Arc::clone(&partial_schema),
                active_spiller,
                &context,
                &mut state_memory,
            )?;
            distinct_memory.try_resize(0)?;
        }

        state_memory.try_resize(0)?;
        let mut pending = state_spiller
            .take()
            .expect("DISTINCT aggregate created a state spiller above")
            .finish()?
            .into_iter()
            .rev()
            .filter(|files| !files.is_empty())
            .map(PartitionTask::initial)
            .collect::<Vec<_>>();
        while let Some(task) = pending.pop() {
            context.check_cancelled()?;
            match merge_partition(
                &task.files,
                &groups,
                &aggregates,
                &context,
                &mut state_memory,
            )? {
                MergeOutcome::Merged(partition_states) => {
                    remove_files(&context, &task.files)?;
                    for chunk in partition_states.chunks(batch_size.max(1)) {
                        yield build_output_envelope(
                            chunk,
                            &groups,
                            &aggregates,
                            Arc::clone(&schema),
                            OutputMode::Final,
                            &context,
                            state_memory.size(),
                        ).await?;
                    }
                    state_memory.try_resize(0)?;
                }
                MergeOutcome::Repartition => {
                    state_memory.try_resize(0)?;
                    let depth = task.next_depth()?;
                    let children = repartition_partition(
                        &task.files,
                        &groups,
                        depth,
                        &context,
                    )?;
                    remove_files(&context, &task.files)?;
                    for files in children.into_iter().rev() {
                        if !files.is_empty() {
                            pending.push(PartitionTask::child(files, depth));
                        }
                    }
                }
            }
        }
    })
}

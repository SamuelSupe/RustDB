use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;

use crate::{
    Error, Result,
    execution::aggregate::join_sink::{JoinAggregateSink, SelectionTarget},
    execution::runtime_filter::RuntimeFilterSlot,
    runtime::{
        BatchEnvelope, IntoMemoryBatchStream, MemoryBatchStream, OperatorHandle, QueryContext,
        boxed_memory_batch_stream,
    },
    sql::{AggregateExpr, BoundExpr, JoinType},
};

use super::{
    ProbeCursor,
    build::{self, BuildOutcome, InMemoryBuild},
    condition::JoinPredicates,
    evaluate_keys_accounted,
    metrics::JoinPhaseMetrics,
    multiplicity,
    output::JoinEmission,
    parallel, spilled,
};

#[allow(clippy::too_many_arguments)]
pub(in crate::execution) fn join_global_aggregate<L, R>(
    left: L,
    right: R,
    on: Vec<(BoundExpr, BoundExpr)>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_schema: SchemaRef,
    aggregates: Vec<AggregateExpr>,
    aggregate_schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    runtime_filter: Option<Arc<RuntimeFilterSlot>>,
    join_operator: OperatorHandle,
) -> MemoryBatchStream
where
    L: IntoMemoryBatchStream,
    R: IntoMemoryBatchStream,
{
    let mut left = left.into_memory_batch_stream(Arc::clone(&context), "join left input");
    let mut right = right.into_memory_batch_stream(Arc::clone(&context), "join right input");
    let predicates = JoinPredicates::new(None, None, &left_schema, &right_schema);
    let timer = OperatorTimer::new(join_operator.clone());

    boxed_memory_batch_stream(async_stream::try_stream! {
        let _timer = timer.start();
        if on.is_empty() {
            Err(Error::Internal(
                "global join aggregate requires at least one equality key".into(),
            ))?;
        }
        let left_keys = on.iter().map(|(left, _)| left.clone()).collect::<Vec<_>>();
        let right_keys = on.iter().map(|(_, right)| right.clone()).collect::<Vec<_>>();
        let use_multiplicity =
            multiplicity::eligible(&aggregates, left_schema.fields().len(), &right_keys);
        let phases = JoinPhaseMetrics::register(&context, Some(join_operator.id()));
        let phases = if use_multiplicity {
            phases.with_multiplicity_build(&context)
        } else {
            phases
        };
        let mut build_phase = Some(phases.start_build());
        let mut reservation = context.memory.reservation();
        if use_multiplicity {
            match build::multiplicity::build(
                &mut right,
                &right_keys,
                &context,
                &mut reservation,
                &phases,
            )
            .await?
            {
                build::multiplicity::MultiplicityBuildOutcome::InMemory(table) => {
                    if let Some(runtime_filter) = &runtime_filter {
                        runtime_filter.publish_none();
                    }
                    drop(build_phase.take());
                    let _probe_phase = phases.start_probe(&context);
                    if parallel::is_supported(&context, reservation.size()) {
                        let mut output = parallel::probe_global_multiplicity(
                            left,
                            left_keys,
                            table,
                            reservation,
                            join_schema,
                            aggregates,
                            aggregate_schema,
                            join_operator,
                            Arc::clone(&context),
                            phases.clone(),
                        );
                        while let Some(batch) = output.next().await {
                            yield batch?;
                        }
                        return;
                    }
                    let mut sink = JoinAggregateSink::try_new(
                        aggregates,
                        aggregate_schema,
                        &join_schema,
                        &context,
                    )?;
                    multiplicity::probe(
                        &mut left,
                        &left_keys,
                        &table,
                        reservation.size(),
                        &mut sink,
                        &join_operator,
                        &context,
                        &phases,
                    )
                    .await?;
                    drop(table);
                    drop(reservation);
                    yield sink.finish(&context).await?;
                    return;
                }
                build::multiplicity::MultiplicityBuildOutcome::Spilled(right_partitions) => {
                    if let Some(runtime_filter) = &runtime_filter {
                        runtime_filter.publish_none();
                    }
                    drop(build_phase.take());
                    let _spill_phase = phases.start_spill(&context);
                    let mut sink = JoinAggregateSink::try_new(
                        aggregates,
                        aggregate_schema,
                        &join_schema,
                        &context,
                    )?;
                    let mut output = spilled::execute(
                        left,
                        right_partitions,
                        left_keys,
                        right_keys,
                        left_schema,
                        right_schema,
                        predicates,
                        false,
                        JoinType::Inner,
                        join_schema,
                        Arc::clone(&context),
                        batch_size,
                        reservation,
                    );
                    while let Some(batch) = output.next().await {
                        let batch = batch?;
                        record_materialized(&join_operator, &batch);
                        sink.consume_batch(batch.batch(), &context).await?;
                    }
                    drop(output);
                    yield sink.finish(&context).await?;
                    return;
                }
                build::multiplicity::MultiplicityBuildOutcome::Unsupported => {}
            }
        }
        let outcome = build::build(
            &mut right,
            &right_keys,
            &predicates,
            false,
            left_schema.fields().len(),
            &right_schema,
            JoinType::Inner,
            false,
            &context,
            &mut reservation,
        )
        .await?;

        match outcome {
            BuildOutcome::InMemory(build) => {
                publish_runtime_filter(runtime_filter.as_deref(), &build, &context).await?;
                drop(build_phase.take());
                let _probe_phase = phases.start_probe(&context);
                let (right_batch, hash_table, right_values, global_membership, matched_build) =
                    build.into_parts();
                if right_values.is_some() || global_membership.is_some() || matched_build.is_some() {
                    Err(Error::Internal(
                        "non-equality state reached the global join aggregate".into(),
                    ))?;
                }
                if parallel::is_supported(&context, reservation.size()) {
                    let build = parallel::FrozenBuild::new(
                        right_batch,
                        hash_table,
                        right_values,
                        global_membership,
                        false,
                        matched_build,
                        reservation,
                    );
                    let mut output = parallel::probe_global_aggregate(
                        left,
                        left_keys,
                        build,
                        predicates,
                        join_schema,
                        aggregates,
                        aggregate_schema,
                        join_operator,
                        Arc::clone(&context),
                        batch_size,
                    );
                    while let Some(batch) = output.next().await {
                        yield batch?;
                    }
                    return;
                }

                let mut sink = JoinAggregateSink::try_new(
                    aggregates,
                    aggregate_schema,
                    &join_schema,
                    &context,
                )?;
                while let Some(batch) = left.next().await {
                    context.check_cancelled()?;
                    let left_batch = batch?;
                    let (left_key_values, fixed_rows) = {
                        let _permit = context.acquire_compute().await?;
                        let _active = context.scheduler.enter_lane();
                        let keys = evaluate_keys_accounted(
                            &left_keys,
                            left_batch.batch(),
                            &context,
                            "join aggregate probe keys",
                        )?;
                        let rows = hash_table
                            .fixed_probe(&keys)?
                            .map(|probe| {
                                sink.consume_fixed_matches(
                                    left_batch.batch(),
                                    &right_batch,
                                    |row| probe.lookup(row),
                                    &context,
                                )
                            })
                            .transpose()?;
                        (keys, rows)
                    };
                    if let Some(rows) = fixed_rows {
                        record_direct(&context, &join_operator, rows);
                        continue;
                    }
                    let mut probe = ProbeCursor::new(
                        left_batch.batch(),
                        &right_batch,
                        &left_key_values,
                        &hash_table,
                        &predicates,
                        None,
                        None,
                        None,
                        false,
                        None,
                        JoinType::Inner,
                        Arc::clone(&join_schema),
                        batch_size,
                        reservation
                            .size()
                            .saturating_add(left_batch.memory_size())
                            .saturating_add(left_key_values.memory_size()),
                    );
                    let mut target = SelectionTarget::new(&mut sink);
                    loop {
                        match probe.next_output(&mut target, &context).await? {
                            JoinEmission::Consumed { rows } => join_operator.record_output(
                                u64::try_from(rows).unwrap_or(u64::MAX),
                                0,
                            ),
                            JoinEmission::Exhausted => break,
                            JoinEmission::Batch(_) => Err(Error::Internal(
                                "selection-aware join aggregate materialized an output batch"
                                    .into(),
                            ))?,
                        }
                    }
                }
                drop(hash_table);
                drop(right_batch);
                drop(reservation);
                yield sink.finish(&context).await?;
            }
            BuildOutcome::Spilled(right_partitions) => {
                if let Some(runtime_filter) = &runtime_filter {
                    runtime_filter.publish_none();
                }
                drop(build_phase.take());
                let _spill_phase = phases.start_spill(&context);
                let mut sink = JoinAggregateSink::try_new(
                    aggregates,
                    aggregate_schema,
                    &join_schema,
                    &context,
                )?;
                let mut output = spilled::execute(
                    left,
                    right_partitions,
                    left_keys,
                    right_keys,
                    left_schema,
                    right_schema,
                    predicates,
                    false,
                    JoinType::Inner,
                    join_schema,
                    Arc::clone(&context),
                    batch_size,
                    reservation,
                );
                while let Some(batch) = output.next().await {
                    let batch = batch?;
                    record_materialized(&join_operator, &batch);
                    sink.consume_batch(batch.batch(), &context).await?;
                }
                drop(output);
                yield sink.finish(&context).await?;
            }
        }
    })
}

async fn publish_runtime_filter(
    runtime_filter: Option<&RuntimeFilterSlot>,
    build: &InMemoryBuild,
    context: &QueryContext,
) -> Result<()> {
    let Some(runtime_filter) = runtime_filter else {
        return Ok(());
    };
    let _permit = context.acquire_compute().await?;
    let _active = context.scheduler.enter_lane();
    let hash_table = build.hash_table();
    if let Some(values) = hash_table.generic_values() {
        runtime_filter.publish_hash(values, context);
    } else if let Some(keys) = hash_table.fixed_keys() {
        runtime_filter.publish_fixed_keys(keys, context);
    } else if let Some(keys) = hash_table.utf8_keys() {
        runtime_filter.publish_utf8_keys(keys, context);
    } else if let Some(keys) = hash_table.binary_keys() {
        runtime_filter.publish_binary_keys(keys, context);
    } else {
        runtime_filter.publish_none();
    }
    Ok(())
}

fn record_materialized(operator: &OperatorHandle, batch: &BatchEnvelope) {
    operator.record_output(
        u64::try_from(batch.batch().num_rows()).unwrap_or(u64::MAX),
        u64::try_from(batch.batch().get_array_memory_size()).unwrap_or(u64::MAX),
    );
}

fn record_direct(context: &QueryContext, operator: &OperatorHandle, rows: usize) {
    if rows == 0 {
        return;
    }
    let rows = u64::try_from(rows).unwrap_or(u64::MAX);
    context.metrics.add_join_candidates(rows);
    operator.record_output(rows, 0);
}

struct OperatorTimer {
    operator: OperatorHandle,
    started: Option<Instant>,
}

#[cfg(test)]
#[path = "aggregate_output/tests.rs"]
mod tests;

impl OperatorTimer {
    fn new(operator: OperatorHandle) -> Self {
        Self {
            operator,
            started: None,
        }
    }

    fn start(mut self) -> Self {
        self.started = Some(Instant::now());
        self
    }
}

impl Drop for OperatorTimer {
    fn drop(&mut self) {
        self.operator.finish(
            self.started
                .map_or(Duration::ZERO, |started| started.elapsed()),
        );
    }
}

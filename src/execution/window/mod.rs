mod frame_aggregate;
mod frame_index;
mod keys;
mod memory;
mod navigation;
mod output;
mod sidecar;
mod spool;

#[cfg(test)]
mod memory_tests;
#[cfg(test)]
mod tests;

use std::{mem::size_of, sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use tokio::sync::mpsc;

use crate::runtime::{
    IntoMemoryBatchStream, MemoryBatchStream, QueryContext, boxed_memory_batch_stream,
};
use crate::sql::{SortExpr, WindowExpr};

use super::{expr, sort};
use crate::{Error, Result};

enum PartitionEvent {
    Batch(crate::runtime::BatchEnvelope),
    Done,
}

pub(crate) fn window<I>(
    input: I,
    expressions: Vec<WindowExpr>,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream
where
    I: IntoMemoryBatchStream,
{
    let input = input.into_memory_batch_stream(Arc::clone(&context), "window input");
    boxed_memory_batch_stream(async_stream::try_stream! {
        let Some(first) = expressions.first() else {
            Err(Error::Internal("Window plan has no expressions".into()))?;
            return;
        };
        if expressions.iter().skip(1).any(|expression| {
            expression.partition_by != first.partition_by
                || expression.order_by != first.order_by
                || expression.frame != first.frame
        }) {
            Err(Error::Internal(
                "Window stage contains expressions with different specifications".into(),
            ))?;
        }

        let mut sort_expressions = first
            .partition_by
            .iter()
            .cloned()
            .map(|expr| SortExpr {
                expr,
                descending: false,
                nulls_first: false,
            })
            .collect::<Vec<_>>();
        sort_expressions.extend(first.order_by.iter().cloned());
        let mut sorted = if sort_expressions.is_empty() {
            input
        } else {
            sort::sort(
                input,
                sort_expressions,
                None,
                Arc::clone(&input_schema),
                Arc::clone(&context),
                batch_size,
            )
        };
        let partition_exprs = first.partition_by.clone();
        let eval_exprs = partition_exprs
            .iter()
            .cloned()
            .chain(expressions.iter().filter_map(|expression| match &expression.function {
                crate::sql::WindowFunction::Aggregate(aggregate) => aggregate.expr.clone(),
                _ => None,
            }))
            .collect::<Vec<_>>();
        let mut pending: Option<spool::PendingPartition> = None;
        let lanes = context.scheduler.configured_lanes();
        let (partition_sender, mut partition_receiver) =
            mpsc::channel(lanes.saturating_mul(2).max(2));
        let mut active_partitions = 0usize;

        while let Some(batch) = sorted.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let evaluation = context
                .reserve_memory_while_holding(
                    expr::projection_workspace_bytes(&eval_exprs, batch.batch()),
                    batch.memory_size().saturating_add(
                        pending
                            .as_ref()
                            .map(spool::PendingPartition::retained_bytes)
                            .unwrap_or(0),
                    ),
                    "window input expression workspace",
                )
                .await?;
            let (partition_keys, aggregate_inputs, key_payload) = {
                let _compute = context.acquire_compute().await?;
                let _active = context.scheduler.enter_lane();
                let partition_keys = keys::evaluate_keys(&partition_exprs, batch.batch())?;
                let aggregate_inputs = spool::aggregate_inputs(&expressions, batch.batch())?;
                let key_payload = (0..batch.num_rows()).try_fold(
                    0usize,
                    |bytes, row| -> Result<usize> {
                        Ok(bytes.max(memory::row_payload_bytes(&partition_keys, row)?))
                    },
                )?;
                (partition_keys, aggregate_inputs, key_payload)
            };
            let key_workspace_bytes = key_payload.saturating_add(
                partition_exprs
                    .len()
                    .saturating_mul(size_of::<super::value::CellValue>()),
            )
            .saturating_mul(2);
            // Every boundary comparison materializes one owned key. Acquire a
            // reusable credit before the first row_key clone.
            let key_workspace = context
                .reserve_memory_while_holding(
                    key_workspace_bytes,
                    batch
                        .memory_size()
                        .saturating_add(evaluation.size())
                        .saturating_add(
                            pending
                                .as_ref()
                                .map(spool::PendingPartition::retained_bytes)
                                .unwrap_or(0),
                        ),
                    "window partition key workspace",
                )
                .await?;
            let mut start = 0usize;
            while start < batch.num_rows() {
                let (key, end, partition_changed, state_updated) = {
                    let _compute = context.acquire_compute().await?;
                    let _active = context.scheduler.enter_lane();
                    let key = keys::row_key(&partition_keys, start)?;
                    let mut end = start + 1;
                    while end < batch.num_rows()
                        && keys::row_key(&partition_keys, end)? == key
                    {
                        end += 1;
                    }
                    let partition_changed = pending
                        .as_ref()
                        .is_some_and(|partition| !partition.matches(&key));
                    let state_updated = if !partition_changed
                        && let Some(partition) = pending.as_mut()
                    {
                        partition.update(
                            start,
                            end - start,
                            &expressions,
                            &aggregate_inputs,
                        )?;
                        true
                    } else {
                        false
                    };
                    (key, end, partition_changed, state_updated)
                };
                if partition_changed {
                    spawn_partition(
                        pending.take().expect("pending checked above").finish()?,
                        expressions.clone(),
                        Arc::clone(&output_schema),
                        Arc::clone(&context),
                        batch_size,
                        partition_sender.clone(),
                    )?;
                    active_partitions += 1;
                    while active_partitions >= lanes {
                        match next_partition_event(&mut partition_receiver, &context).await? {
                            PartitionEvent::Batch(batch) => yield batch,
                            PartitionEvent::Done => active_partitions -= 1,
                        }
                    }
                }
                if pending.is_none() {
                    pending = Some(spool::PendingPartition::new(
                        &context,
                        Arc::clone(&input_schema),
                        key,
                        &expressions,
                    )?);
                }
                if !state_updated {
                    let _compute = context.acquire_compute().await?;
                    let _active = context.scheduler.enter_lane();
                    pending.as_mut().expect("partition initialized").update(
                        start,
                        end - start,
                        &expressions,
                        &aggregate_inputs,
                    )?;
                }
                // Spill I/O is deliberately outside the global compute slot.
                pending
                    .as_mut()
                    .expect("partition initialized")
                    .write_slice(
                    batch.batch(),
                    start,
                    end - start,
                )?;
                start = end;
            }
            drop(key_workspace);
            drop(aggregate_inputs);
            drop(partition_keys);
            drop(evaluation);
            drop(batch);

        }
        if let Some(partition) = pending.take() {
            spawn_partition(
                partition.finish()?,
                expressions,
                output_schema,
                Arc::clone(&context),
                batch_size,
                partition_sender.clone(),
            )?;
            active_partitions += 1;
        }
        drop(partition_sender);
        while active_partitions != 0 {
            match next_partition_event(&mut partition_receiver, &context).await? {
                PartitionEvent::Batch(batch) => yield batch,
                PartitionEvent::Done => active_partitions -= 1,
            }
        }
    })
}

fn spawn_partition(
    partition: spool::CompletedPartition,
    expressions: Vec<WindowExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    sender: mpsc::Sender<PartitionEvent>,
) -> Result<()> {
    let tasks = context.tasks.clone();
    tasks.spawn("window-partition-lane", async move {
        let mut output = output::process(
            partition,
            expressions,
            schema,
            Arc::clone(&context),
            batch_size,
        );
        while let Some(batch) = output.next().await {
            sender
                .send(PartitionEvent::Batch(batch?))
                .await
                .map_err(|_| Error::Cancelled)?;
        }
        sender
            .send(PartitionEvent::Done)
            .await
            .map_err(|_| Error::Cancelled)
    })
}

async fn next_partition_event(
    receiver: &mut mpsc::Receiver<PartitionEvent>,
    context: &QueryContext,
) -> Result<PartitionEvent> {
    let started = Instant::now();
    let event = tokio::select! {
        biased;
        _ = context.control.cancelled() => Err(context
            .check_cancelled()
            .expect_err("cancelled query has a terminal error")),
        event = receiver.recv() => event.ok_or_else(|| {
            context.tasks.first_failure().unwrap_or_else(|| {
                Error::Execution("window partition lanes stopped before completion".into())
            })
        }),
    };
    context.scheduler.record_wait(started.elapsed());
    event
}

#[cfg(test)]
mod fault_injection {
    use std::{collections::HashSet, sync::Mutex};

    use uuid::Uuid;

    static PANIC_QUERIES: Mutex<Option<HashSet<Uuid>>> = Mutex::new(None);

    pub(super) fn arm(query_id: Uuid) {
        PANIC_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get_or_insert_with(HashSet::new)
            .insert(query_id);
    }

    pub(super) fn panic_if_armed(query_id: Uuid) {
        let armed = PANIC_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_mut()
            .is_some_and(|queries| queries.remove(&query_id));
        if armed {
            panic!("injected window partition panic");
        }
    }
}

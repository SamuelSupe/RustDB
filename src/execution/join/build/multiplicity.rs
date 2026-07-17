use arrow::datatypes::DataType;
use futures::StreamExt;
use std::time::Instant;

#[cfg(test)]
use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, OnceLock},
};

use crate::{
    Error, Result,
    runtime::{MemoryBatchStream, MemoryReservation, QueryContext},
    sql::{BoundExpr, JoinType},
};

use super::{BuildOutcome, spilling};
use crate::execution::join::{
    evaluate_keys_accounted, hash_table::CompositeMultiplicityTable, metrics::JoinPhaseMetrics,
    spill::PartitionManifest,
};

pub(in crate::execution::join) enum MultiplicityBuildOutcome {
    InMemory(CompositeMultiplicityTable),
    Spilled(PartitionManifest),
    Unsupported,
}

pub(in crate::execution::join) fn supports(keys: &[BoundExpr]) -> bool {
    let types = key_types(keys);
    CompositeMultiplicityTable::supports(&types)
}

pub(in crate::execution::join) async fn build(
    right: &mut MemoryBatchStream,
    right_key_expressions: &[BoundExpr],
    context: &QueryContext,
    reservation: &mut MemoryReservation,
    phases: &JoinPhaseMetrics,
) -> Result<MultiplicityBuildOutcome> {
    let types = key_types(right_key_expressions);
    let mut table_memory = context.memory.reservation();
    let Some(mut table) = CompositeMultiplicityTable::try_new(&types, &mut table_memory)? else {
        return Ok(MultiplicityBuildOutcome::Unsupported);
    };

    let mut buffered = Vec::new();
    let mut buffered_bytes = 0usize;
    let mut buffered_rows = 0usize;
    let buffer_limit = context.memory.limit().checked_div(4).unwrap_or(0).max(1);

    loop {
        let input_started = Instant::now();
        let batch = right.next().await;
        phases.record_build_input_poll(input_started.elapsed());
        let Some(batch) = batch else {
            break;
        };
        context.check_cancelled()?;
        let batch = batch?;
        let projected_bytes = buffered_bytes.saturating_add(batch.memory_size());
        let projected_rows = buffered_rows.saturating_add(batch.batch().num_rows());
        if projected_bytes > buffer_limit {
            drop(table);
            drop(table_memory);
            return spill(
                right,
                buffered,
                batch,
                projected_bytes,
                projected_rows,
                right_key_expressions,
                context,
                reservation,
            )
            .await;
        }

        #[cfg(test)]
        if force_fallback(context.query_id, buffered.len()) {
            drop(table);
            drop(table_memory);
            return spill(
                right,
                buffered,
                batch,
                projected_bytes,
                projected_rows,
                right_key_expressions,
                context,
                reservation,
            )
            .await;
        }

        let inserted = {
            let permit_started = Instant::now();
            let permit = context.acquire_compute().await;
            phases.record_build_permit_wait(permit_started.elapsed());
            let _permit = permit?;
            let _active = context.scheduler.enter_lane();
            let keys = phases.measure_build_key_eval(|| {
                evaluate_keys_accounted(
                    right_key_expressions,
                    batch.batch(),
                    context,
                    "join multiplicity build keys",
                )
            })?;
            phases.measure_build_hash_table(|| table.try_insert(&keys, &mut table_memory))?
        };
        if !inserted {
            drop(table);
            drop(table_memory);
            return spill(
                right,
                buffered,
                batch,
                projected_bytes,
                projected_rows,
                right_key_expressions,
                context,
                reservation,
            )
            .await;
        }

        let (batch, memory) = batch.into_parts();
        reservation.absorb(memory)?;
        buffered.push(batch);
        buffered_bytes = projected_bytes;
        buffered_rows = projected_rows;
    }

    drop(buffered);
    reservation.shrink(buffered_bytes);
    reservation.absorb(table_memory)?;
    context.metrics.observe_memory(context.memory.used());
    #[cfg(test)]
    observed_builds()
        .lock()
        .expect("multiplicity observation lock poisoned")
        .insert(context.query_id);
    Ok(MultiplicityBuildOutcome::InMemory(table))
}

#[cfg(test)]
pub(in crate::execution::join) fn force_fallback_after(query_id: uuid::Uuid, batches: usize) {
    forced_fallbacks()
        .lock()
        .expect("multiplicity fallback lock poisoned")
        .insert(query_id, batches);
}

#[cfg(test)]
pub(in crate::execution::join) fn take_observed_build(query_id: uuid::Uuid) -> bool {
    observed_builds()
        .lock()
        .expect("multiplicity observation lock poisoned")
        .remove(&query_id)
}

#[cfg(test)]
fn force_fallback(query_id: uuid::Uuid, buffered_batches: usize) -> bool {
    let mut requests = forced_fallbacks()
        .lock()
        .expect("multiplicity fallback lock poisoned");
    requests
        .get(&query_id)
        .is_some_and(|threshold| buffered_batches >= *threshold)
        .then(|| requests.remove(&query_id))
        .flatten()
        .is_some()
}

#[cfg(test)]
fn forced_fallbacks() -> &'static Mutex<HashMap<uuid::Uuid, usize>> {
    static REQUESTS: OnceLock<Mutex<HashMap<uuid::Uuid, usize>>> = OnceLock::new();
    REQUESTS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn observed_builds() -> &'static Mutex<HashSet<uuid::Uuid>> {
    static OBSERVED: OnceLock<Mutex<HashSet<uuid::Uuid>>> = OnceLock::new();
    OBSERVED.get_or_init(|| Mutex::new(HashSet::new()))
}

fn key_types(keys: &[BoundExpr]) -> Vec<DataType> {
    keys.iter().map(|key| key.data_type.clone()).collect()
}

#[allow(clippy::too_many_arguments)]
async fn spill(
    right: &mut MemoryBatchStream,
    buffered: Vec<arrow::record_batch::RecordBatch>,
    batch: crate::runtime::BatchEnvelope,
    projected_bytes: usize,
    projected_rows: usize,
    keys: &[BoundExpr],
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<MultiplicityBuildOutcome> {
    match spilling::streaming(
        right,
        buffered,
        batch,
        projected_bytes,
        projected_rows,
        keys,
        JoinType::Inner,
        false,
        context,
        reservation,
    )
    .await?
    {
        BuildOutcome::Spilled(manifest) => Ok(MultiplicityBuildOutcome::Spilled(manifest)),
        BuildOutcome::InMemory(_) => Err(Error::Internal(
            "streaming multiplicity fallback unexpectedly returned an in-memory build".into(),
        )),
    }
}

use std::collections::HashMap;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use tokio_util::sync::CancellationToken;

use crate::{
    Result,
    runtime::{MemoryReservation, QueryContext},
    sql::{BoundExpr, JoinType},
};

use super::super::{
    CellValue, EvaluatedKeys,
    condition::JoinPredicates,
    evaluate_keys_accounted, evaluate_optional_values,
    matched::BuildMatchTracker,
    optional_array,
    probe::{try_build_existence_hash_table_with_nulls, try_build_hash_table_with_nulls},
    spill::{self, BuildPartition, PartitionTask},
};
use super::admission::{BuildAdmission, BuildPermit};

pub(super) enum TaskHashBuild {
    Ready(
        RecordBatch,
        HashMap<Vec<CellValue>, Vec<u32>>,
        Option<EvaluatedKeys>,
        Option<BuildMatchTracker>,
    ),
    TooLarge(usize),
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load_with_admission(
    task: &PartitionTask,
    admission: &BuildAdmission,
    cancellation: &CancellationToken,
    right_key_expressions: &[BoundExpr],
    right_schema: &SchemaRef,
    predicates: &JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    left_columns: usize,
    context: &QueryContext,
) -> Result<(TaskHashBuild, MemoryReservation, BuildPermit)> {
    let mut permit = admission
        .acquire(task.build.estimated_bytes, cancellation, context)
        .await?;
    let mut reservation = build_reservation(context, "build", permit.limit_bytes());
    let mut build = try_load_hash_build(
        task,
        right_key_expressions,
        right_schema,
        predicates,
        null_equal_keys,
        join_type,
        left_columns,
        context,
        &mut reservation,
    )?;

    if matches!(build, TaskHashBuild::TooLarge(_)) && !permit.is_exclusive() {
        reservation.try_resize(0)?;
        drop(reservation);
        drop(permit);
        permit = admission.acquire_exclusive(cancellation, context).await?;
        reservation = build_reservation(context, "straggler", permit.limit_bytes());
        build = try_load_hash_build(
            task,
            right_key_expressions,
            right_schema,
            predicates,
            null_equal_keys,
            join_type,
            left_columns,
            context,
            &mut reservation,
        )?;
    }
    Ok((build, reservation, permit))
}

fn build_reservation(context: &QueryContext, label: &str, limit: usize) -> MemoryReservation {
    context
        .memory
        .child(format!("Grace-join-{label}-{}", context.query_id), limit)
        .reservation()
}

#[allow(clippy::too_many_arguments)]
fn try_load_hash_build(
    task: &PartitionTask,
    right_key_expressions: &[BoundExpr],
    right_schema: &SchemaRef,
    predicates: &JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    left_columns: usize,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<TaskHashBuild> {
    let _active = context.scheduler.enter_lane();
    let right_batch = match spill::load_build_partition(
        &task.right,
        right_schema,
        context,
        reservation,
        task.build,
    )? {
        BuildPartition::Loaded(batch) => batch,
        BuildPartition::TooLarge { rows } => return Ok(TaskHashBuild::TooLarge(rows)),
    };
    let right_keys = evaluate_keys_accounted(
        right_key_expressions,
        &right_batch,
        context,
        "Grace join build keys",
    )?;
    let rows = right_batch.num_rows();
    let inequality_value = predicates.existence_inequality_right_value(join_type, left_columns);
    let inequality_values = evaluate_optional_values(
        inequality_value.as_ref(),
        &right_batch,
        context,
        "Grace join existence inequality value",
    )?;
    let hash_table = if let Some(values) = optional_array(&inequality_values) {
        try_build_existence_hash_table_with_nulls(
            &right_keys,
            rows,
            null_equal_keys,
            values,
            reservation,
        )?
    } else {
        try_build_hash_table_with_nulls(
            &right_keys,
            rows,
            super::super::can_deduplicate_build(join_type, predicates),
            null_equal_keys,
            reservation,
        )?
    };
    drop(inequality_values);
    drop(right_keys);
    let right_values = evaluate_optional_values(
        predicates.right_value(),
        &right_batch,
        context,
        "Grace join build membership value",
    )?;
    Ok(match hash_table {
        Some(hash_table)
            if let Some(matched_build) =
                super::super::try_build_match_tracker(join_type, rows, reservation) =>
        {
            TaskHashBuild::Ready(right_batch, hash_table, right_values, matched_build)
        }
        Some(hash_table) => {
            drop(hash_table);
            drop(right_values);
            TaskHashBuild::TooLarge(rows)
        }
        None => {
            drop(right_values);
            TaskHashBuild::TooLarge(rows)
        }
    })
}

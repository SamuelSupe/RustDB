use arrow::record_batch::RecordBatch;

use crate::{
    Result,
    runtime::{MemoryReservation, QueryContext},
    sql::{BoundExpr, JoinType},
};

use super::super::{
    EvaluatedKeys, GlobalMembershipState, JoinHashTable, can_deduplicate_build,
    condition::JoinPredicates,
    evaluate_keys_accounted, evaluate_optional_values, optional_array,
    probe::{
        try_build_existence_hash_table_with_nulls, try_build_hash_table,
        try_build_primary_hash_table_with_nulls,
    },
    try_build_match_tracker,
};
use super::{BuildOutcome, InMemoryBuild};

#[allow(clippy::too_many_arguments)]
pub(super) async fn finish(
    right_batch: RecordBatch,
    right_key_expressions: &[BoundExpr],
    predicates: &JoinPredicates,
    use_global_membership_hash: bool,
    left_width: usize,
    join_type: JoinType,
    null_equal_keys: bool,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<BuildOutcome> {
    let rows = right_batch.num_rows();
    let (hash_table, right_values, global_membership) = {
        let _permit = context.acquire_compute().await?;
        let _active = context.scheduler.enter_lane();
        if use_global_membership_hash {
            global_membership(&right_batch, rows, predicates, context, reservation)?
        } else {
            regular(
                &right_batch,
                rows,
                right_key_expressions,
                predicates,
                left_width,
                join_type,
                null_equal_keys,
                context,
                reservation,
            )?
        }
    };

    if let Some(hash_table) = hash_table {
        if let Some(matched_build) = try_build_match_tracker(join_type, rows, reservation) {
            return Ok(BuildOutcome::InMemory(Box::new(InMemoryBuild {
                batch: right_batch,
                hash_table,
                right_values,
                global_membership,
                matched_build,
            })));
        }
        drop(hash_table);
    }
    drop(right_values);
    super::spilling::batch(
        right_batch,
        right_key_expressions,
        join_type,
        null_equal_keys,
        context,
        reservation,
    )
    .map(BuildOutcome::Spilled)
}

fn global_membership(
    right_batch: &RecordBatch,
    rows: usize,
    predicates: &JoinPredicates,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<(
    Option<JoinHashTable>,
    Option<EvaluatedKeys>,
    Option<GlobalMembershipState>,
)> {
    let right_values = evaluate_optional_values(
        predicates.right_value(),
        right_batch,
        context,
        "join build membership value",
    )?;
    let right_array = optional_array(&right_values).ok_or_else(|| {
        crate::Error::Internal("global membership hash is missing its right value array".into())
    })?;
    let state = GlobalMembershipState::new(rows, right_array);
    let hash_table =
        try_build_hash_table(std::slice::from_ref(right_array), rows, true, reservation)?;
    Ok((hash_table, right_values, Some(state)))
}

#[allow(clippy::too_many_arguments)]
fn regular(
    right_batch: &RecordBatch,
    rows: usize,
    right_key_expressions: &[BoundExpr],
    predicates: &JoinPredicates,
    left_width: usize,
    join_type: JoinType,
    null_equal_keys: bool,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<(
    Option<JoinHashTable>,
    Option<EvaluatedKeys>,
    Option<GlobalMembershipState>,
)> {
    let right_keys = evaluate_keys_accounted(
        right_key_expressions,
        right_batch,
        context,
        "join build keys",
    )?;
    let inequality_value = predicates.existence_inequality_right_value(join_type, left_width);
    let inequality_values = evaluate_optional_values(
        inequality_value.as_ref(),
        right_batch,
        context,
        "join build existence inequality value",
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
        try_build_primary_hash_table_with_nulls(
            &right_keys,
            rows,
            can_deduplicate_build(join_type, predicates),
            null_equal_keys,
            reservation,
        )?
    };
    drop(inequality_values);
    drop(right_keys);
    let right_values = evaluate_optional_values(
        predicates.right_value(),
        right_batch,
        context,
        "join build membership value",
    )?;
    Ok((hash_table, right_values, None))
}

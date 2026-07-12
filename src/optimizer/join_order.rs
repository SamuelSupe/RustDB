use std::{cmp::Ordering, mem};

use crate::sql::{BoundExpr, JoinType, LogicalPlan, PlanSchema};

/// Chooses the smaller build input for each independent inner-join node. This
/// deliberately does not search alternative multi-table join trees.
pub(super) fn choose_build_sides(plan: &mut LogicalPlan) {
    match plan {
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => {}
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Scalarize { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => choose_build_sides(input),
        LogicalPlan::Append { inputs, .. } => inputs.iter_mut().for_each(choose_build_sides),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::DependentJoin { left, right, .. } => {
            choose_build_sides(left);
            choose_build_sides(right);
        }
    }

    let should_swap = match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type: JoinType::Inner,
            residual: None,
            null_aware: None,
            ..
        } => smaller(left, right),
        _ => false,
    };
    if !should_swap {
        return;
    }

    let placeholder = LogicalPlan::Empty {
        produce_one_row: false,
        schema: PlanSchema::empty(),
    };
    let LogicalPlan::Join {
        left,
        right,
        on,
        null_equal_keys,
        residual: None,
        null_aware: None,
        join_type: JoinType::Inner,
        schema,
    } = mem::replace(plan, placeholder)
    else {
        unreachable!("swap decision requires an inner join")
    };

    let left_width = left.schema().arrow().fields().len();
    let right_width = right.schema().arrow().fields().len();
    let mut restore = Vec::with_capacity(left_width + right_width);
    restore.extend(
        left.schema()
            .arrow()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                BoundExpr::column(right_width + index, field.data_type().clone(), field.name())
            }),
    );
    restore.extend(
        right
            .schema()
            .arrow()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                BoundExpr::column(index, field.data_type().clone(), field.name())
            }),
    );

    let join_schema = PlanSchema::join(right.schema(), left.schema());
    let swapped = LogicalPlan::Join {
        left: right,
        right: left,
        on: on.into_iter().map(|(left, right)| (right, left)).collect(),
        null_equal_keys,
        residual: None,
        null_aware: None,
        join_type: JoinType::Inner,
        schema: join_schema,
    };
    *plan = LogicalPlan::Projection {
        input: Box::new(swapped),
        expressions: restore,
        schema,
    };
}

fn smaller(left: &LogicalPlan, right: &LogicalPlan) -> bool {
    let left = estimate(left);
    let right = estimate(right);
    if let (Some(left), Some(right)) = (left.bytes, right.bytes) {
        return left < right;
    }
    matches!(
        (left.rows, right.rows),
        (Some(left), Some(right)) if left.cmp(&right) == Ordering::Less
    )
}

#[derive(Clone, Copy, Default)]
struct Estimate {
    rows: Option<u64>,
    bytes: Option<u64>,
}

fn estimate(plan: &LogicalPlan) -> Estimate {
    match plan {
        LogicalPlan::Empty {
            produce_one_row, ..
        } => Estimate {
            rows: Some(u64::from(*produce_one_row)),
            bytes: Some(0),
        },
        LogicalPlan::Scan { statistics, .. } => Estimate {
            rows: statistics.row_count,
            bytes: statistics.total_byte_size,
        },
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Sort { input, .. } => estimate(input),
        LogicalPlan::Append { inputs, .. } => {
            inputs.iter().fold(Estimate::default(), |total, input| {
                let input = estimate(input);
                Estimate {
                    rows: match (total.rows, input.rows) {
                        (None, rows) => rows,
                        (rows, None) => rows,
                        (Some(left), Some(right)) => Some(left.saturating_add(right)),
                    },
                    bytes: match (total.bytes, input.bytes) {
                        (None, bytes) => bytes,
                        (bytes, None) => bytes,
                        (Some(left), Some(right)) => Some(left.saturating_add(right)),
                    },
                }
            })
        }
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            ..
        } => {
            let mut estimate = estimate(input);
            estimate.rows = estimate.rows.map(|rows| {
                rows.saturating_sub(u64::try_from(*offset).unwrap_or(u64::MAX))
                    .min(
                        limit
                            .map(|limit| u64::try_from(limit).unwrap_or(u64::MAX))
                            .unwrap_or(u64::MAX),
                    )
            });
            estimate
        }
        LogicalPlan::Scalarize { .. } => Estimate {
            rows: Some(1),
            bytes: None,
        },
        // Avoid pretending a local estimate is a global multi-table cost
        // model. Each child join has already made its own safe build choice.
        LogicalPlan::Join { .. } | LogicalPlan::DependentJoin { .. } => Estimate::default(),
    }
}

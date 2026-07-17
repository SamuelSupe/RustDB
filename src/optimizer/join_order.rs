use std::mem;

use crate::Result;
use crate::sql::{BoundExpr, JoinType, LogicalPlan, PlanSchema};

use super::{decorrelate::remap_columns, estimate::estimate};

/// Chooses the smaller build input for each independent inner-join node. This
/// deliberately does not search alternative multi-table join trees.
pub(super) fn choose_build_sides(plan: &mut LogicalPlan) -> Result<()> {
    match plan {
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => {}
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Scalarize { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Repeat { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => choose_build_sides(input)?,
        LogicalPlan::Append { inputs, .. } => {
            for input in inputs {
                choose_build_sides(input)?;
            }
        }
        LogicalPlan::Join { left, right, .. } | LogicalPlan::DependentJoin { left, right, .. } => {
            choose_build_sides(left)?;
            choose_build_sides(right)?;
        }
    }

    let should_swap = match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type: JoinType::Inner,
            residual,
            null_aware: None,
            ..
        } if residual
            .as_ref()
            .is_none_or(BoundExpr::is_structurally_infallible) =>
        {
            smaller(left, right)
        }
        _ => false,
    };
    if !should_swap {
        return Ok(());
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
        mut residual,
        null_aware: None,
        join_type: JoinType::Inner,
        schema,
    } = mem::replace(plan, placeholder)
    else {
        unreachable!("swap decision requires an inner join")
    };

    let left_width = left.schema().arrow().fields().len();
    let right_width = right.schema().arrow().fields().len();
    if let Some(residual) = residual.as_mut() {
        let mapping = (0..left_width)
            .map(|index| right_width + index)
            .chain(0..right_width)
            .collect::<Vec<_>>();
        remap_columns(residual, &mapping)?;
    }
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
        residual,
        null_aware: None,
        join_type: JoinType::Inner,
        schema: join_schema,
    };
    *plan = LogicalPlan::Projection {
        input: Box::new(swapped),
        expressions: restore,
        schema,
    };
    Ok(())
}

fn smaller(left: &LogicalPlan, right: &LogicalPlan) -> bool {
    let left = estimate(left);
    let right = estimate(right);
    if let (Some(left), Some(right)) = (left.output_bytes, right.output_bytes) {
        return left < right;
    }
    matches!((left.rows, right.rows), (Some(left), Some(right)) if left < right)
}

#[cfg(test)]
mod tests;

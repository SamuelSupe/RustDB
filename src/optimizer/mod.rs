use crate::Result;
use crate::sql::{BoundExpr, LogicalPlan};

mod constant;
mod decorrelate;
mod estimate;
mod exact_filter;
mod filter_coalesce;
mod join_order;
mod predicate_relocation;
mod projection;
mod q21_summary;
mod verify;

/// Applies conservative scan pushdowns. Residual operators remain in the plan,
/// so a data source is always free to ignore a pushed predicate.
pub fn optimize(mut plan: LogicalPlan) -> Result<LogicalPlan> {
    constant::fold_plan(&mut plan);
    // Shrink safe outer branches before a correlated aggregate snapshots its
    // parameter domain. Run the same conservative pass again after lowering
    // DependentJoin because decorrelation creates new joins.
    plan = predicate_relocation::apply(plan);
    plan = decorrelate::apply(plan)?;
    plan = predicate_relocation::apply(plan);
    plan = q21_summary::apply(plan)?;
    plan = filter_coalesce::apply(plan);
    push_filter(&mut plan);
    join_order::choose_build_sides(&mut plan)?;
    plan = exact_filter::apply(plan);
    projection::push_required_columns(&mut plan);
    push_limit(&mut plan);
    verify::executable(&plan)?;
    Ok(plan)
}

fn push_filter(plan: &mut LogicalPlan) {
    match plan {
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            push_filter_into_scan(input, predicate);
            push_filter(input);
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Scalarize { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Repeat { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => push_filter(input),
        LogicalPlan::Append { inputs, .. } => inputs.iter_mut().for_each(push_filter),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::DependentJoin { left, right, .. } => {
            push_filter(left);
            push_filter(right);
        }
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => {}
    }
}

fn push_filter_into_scan(plan: &mut LogicalPlan, predicate: &BoundExpr) {
    match plan {
        LogicalPlan::Scan { pushed_filter, .. } => *pushed_filter = Some(predicate.clone()),
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            if let Some(predicate) =
                predicate_relocation::remap_projection_columns(predicate, expressions)
            {
                push_filter_into_scan(input, &predicate);
            }
        }
        _ => {}
    }
}

fn push_limit(plan: &mut LogicalPlan) {
    match plan {
        LogicalPlan::Limit {
            input,
            offset,
            limit: Some(limit),
            ..
        } => {
            let fetch = offset.saturating_add(*limit);
            set_limit_through_projection(input, fetch);
            push_limit(input);
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Scalarize { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Repeat { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => push_limit(input),
        LogicalPlan::Append { inputs, .. } => inputs.iter_mut().for_each(push_limit),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::DependentJoin { left, right, .. } => {
            push_limit(left);
            push_limit(right);
        }
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => {}
    }
}

fn set_limit_through_projection(plan: &mut LogicalPlan, fetch: usize) {
    match plan {
        LogicalPlan::Scan { limit, .. } => *limit = Some(fetch),
        LogicalPlan::Projection { input, .. } => set_limit_through_projection(input, fetch),
        _ => {}
    }
}

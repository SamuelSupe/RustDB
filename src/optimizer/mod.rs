use crate::Result;
use crate::sql::{BoundExpr, LogicalPlan};

mod constant;
mod join_order;
mod projection;

/// Applies conservative scan pushdowns. Residual operators remain in the plan,
/// so a data source is always free to ignore a pushed predicate.
pub fn optimize(mut plan: LogicalPlan) -> Result<LogicalPlan> {
    constant::fold_plan(&mut plan);
    push_filter(&mut plan);
    join_order::choose_build_sides(&mut plan);
    projection::push_required_columns(&mut plan);
    push_limit(&mut plan);
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
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => push_filter(input),
        LogicalPlan::Join { left, right, .. } => {
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
            if let Some(predicate) = remap_projection_columns(predicate, expressions) {
                push_filter_into_scan(input, &predicate);
            }
        }
        _ => {}
    }
}

/// Alias and derived-table planning introduces projections between a filter and
/// its scan. A filter remains safe as a source hint when every referenced
/// projected expression is a direct input column.
fn remap_projection_columns(expr: &BoundExpr, projection: &[BoundExpr]) -> Option<BoundExpr> {
    use crate::sql::ExprKind;

    let mut mapped = expr.clone();
    match &mut mapped.kind {
        ExprKind::Column(index) => {
            let source = projection.get(*index)?;
            if !matches!(&source.kind, ExprKind::Column(_)) {
                return None;
            }
            return Some(source.clone());
        }
        ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            **left = remap_projection_columns(left, projection)?;
            **right = remap_projection_columns(right, projection)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            **expr = remap_projection_columns(expr, projection)?;
        }
        ExprKind::Like { expr, pattern, .. } => {
            **expr = remap_projection_columns(expr, projection)?;
            **pattern = remap_projection_columns(pattern, projection)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                *when = remap_projection_columns(when, projection)?;
                *then = remap_projection_columns(then, projection)?;
            }
            **else_expr = remap_projection_columns(else_expr, projection)?;
        }
    }
    Some(mapped)
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
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => push_limit(input),
        LogicalPlan::Join { left, right, .. } => {
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

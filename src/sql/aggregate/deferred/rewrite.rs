use crate::sql::{AggregateExpr, BoundExpr, ExprKind, LogicalPlan, PlanSchema};
use crate::{Error, Result};

pub(super) fn remap_group_columns(
    expr: &mut BoundExpr,
    mapping: &[(usize, usize)],
    current_schema: &PlanSchema,
) -> Result<()> {
    walk_leaves(expr, &mut |leaf| match &mut leaf.kind {
        ExprKind::Column(index) => {
            *index = mapping
                .iter()
                .find_map(|(input, group)| (*input == *index).then_some(*group))
                .or_else(|| {
                    current_schema
                        .arrow()
                        .fields()
                        .iter()
                        .position(|field| field.name().eq_ignore_ascii_case(&leaf.display_name))
                })
                .ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "subquery expression references outer column {}, which must appear directly in GROUP BY",
                        leaf.display_name
                    ))
                })?;
            Ok(())
        }
        ExprKind::OuterRef { .. } => Err(Error::Internal(
            "OuterRef reached deferred aggregate subquery attachment".into(),
        )),
        _ => Ok(()),
    })
}

pub(super) fn register_aggregates(expr: &BoundExpr, aggregates: &mut Vec<AggregateExpr>) {
    walk_leaves_ref(expr, &mut |leaf| {
        if let ExprKind::DeferredAggregate(aggregate) = &leaf.kind
            && !aggregates
                .iter()
                .any(|existing| existing == aggregate.as_ref())
        {
            aggregates.push(aggregate.as_ref().clone());
        }
    });
}

pub(super) fn resolve_results(
    expr: &mut BoundExpr,
    group_width: usize,
    aggregates: &[AggregateExpr],
) -> Result<()> {
    walk_leaves(expr, &mut |leaf| {
        let position = match &leaf.kind {
            ExprKind::DeferredGroup(index) => Some(*index),
            ExprKind::DeferredAggregate(aggregate) => Some(
                group_width
                    + aggregates
                        .iter()
                        .position(|existing| existing == aggregate.as_ref())
                        .ok_or_else(|| {
                            Error::Internal("deferred IN aggregate state was not registered".into())
                        })?,
            ),
            _ => None,
        };
        if let Some(position) = position {
            leaf.kind = ExprKind::Column(position);
        }
        Ok(())
    })
}

pub(super) fn remap_outer_plan(plan: &mut LogicalPlan, mapping: &[(usize, usize)]) -> Result<()> {
    match plan {
        LogicalPlan::Empty { .. } => Ok(()),
        LogicalPlan::Scan { pushed_filter, .. } => {
            if let Some(expression) = pushed_filter {
                remap_outer_expr(expression, mapping)?;
            }
            Ok(())
        }
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            remap_outer_expr(predicate, mapping)?;
            remap_outer_plan(input, mapping)
        }
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            for expression in expressions {
                remap_outer_expr(expression, mapping)?;
            }
            remap_outer_plan(input, mapping)
        }
        LogicalPlan::Scalarize { input, .. } | LogicalPlan::Limit { input, .. } => {
            remap_outer_plan(input, mapping)
        }
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            ..
        } => {
            for expression in group_exprs {
                remap_outer_expr(expression, mapping)?;
            }
            for aggregate in aggregate_exprs {
                if let Some(expression) = &mut aggregate.expr {
                    remap_outer_expr(expression, mapping)?;
                }
            }
            remap_outer_plan(input, mapping)
        }
        LogicalPlan::Append { inputs, .. } => {
            for input in inputs {
                remap_outer_plan(input, mapping)?;
            }
            Ok(())
        }
        LogicalPlan::Repeat { input, count, .. } => {
            remap_outer_expr(count, mapping)?;
            remap_outer_plan(input, mapping)
        }
        LogicalPlan::Window {
            input, expressions, ..
        } => {
            for expression in expressions {
                if let crate::sql::WindowFunction::Aggregate(aggregate) = &mut expression.function
                    && let Some(argument) = &mut aggregate.expr
                {
                    remap_outer_expr(argument, mapping)?;
                }
                for partition in &mut expression.partition_by {
                    remap_outer_expr(partition, mapping)?;
                }
                for order in &mut expression.order_by {
                    remap_outer_expr(&mut order.expr, mapping)?;
                }
            }
            remap_outer_plan(input, mapping)
        }
        LogicalPlan::Sort {
            input, expressions, ..
        } => {
            for expression in expressions {
                remap_outer_expr(&mut expression.expr, mapping)?;
            }
            remap_outer_plan(input, mapping)
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            ..
        } => {
            for (left, right) in on {
                remap_outer_expr(left, mapping)?;
                remap_outer_expr(right, mapping)?;
            }
            if let Some(expression) = residual {
                remap_outer_expr(expression, mapping)?;
            }
            if let Some((left, right)) = null_aware {
                remap_outer_expr(left, mapping)?;
                remap_outer_expr(right, mapping)?;
            }
            remap_outer_plan(left, mapping)?;
            remap_outer_plan(right, mapping)
        }
        LogicalPlan::DependentJoin { left, .. } => remap_outer_plan(left, mapping),
    }
}

fn remap_outer_expr(expr: &mut BoundExpr, mapping: &[(usize, usize)]) -> Result<()> {
    walk_leaves(expr, &mut |leaf| match &mut leaf.kind {
        ExprKind::OuterRef { depth: 1, index } => {
            *index = mapping
                .iter()
                .find_map(|(input, group)| (*input == *index).then_some(*group))
                .ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "correlated subquery outer column {} must appear directly in GROUP BY",
                        leaf.display_name
                    ))
                })?;
            Ok(())
        }
        ExprKind::OuterRef { depth, .. } => Err(Error::Unsupported(format!(
            "correlated subquery depth {depth} is not supported; maximum depth is 1"
        ))),
        ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => Err(Error::Internal(
            "deferred aggregate result leaked into correlated subquery input".into(),
        )),
        _ => Ok(()),
    })
}

fn walk_leaves(
    expr: &mut BoundExpr,
    visit: &mut impl FnMut(&mut BoundExpr) -> Result<()>,
) -> Result<()> {
    match &mut expr.kind {
        ExprKind::Column(_)
        | ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => visit(expr),
        ExprKind::Binary { left, right, .. }
        | ExprKind::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            walk_leaves(left, visit)?;
            walk_leaves(right, visit)
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            walk_leaves(expr, visit)
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                walk_leaves(when, visit)?;
                walk_leaves(then, visit)?;
            }
            walk_leaves(else_expr, visit)
        }
        ExprKind::ScalarFunction { args, .. } => {
            for argument in args {
                walk_leaves(argument, visit)?;
            }
            Ok(())
        }
    }
}

fn walk_leaves_ref(expr: &BoundExpr, visit: &mut impl FnMut(&BoundExpr)) {
    match &expr.kind {
        ExprKind::Column(_)
        | ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => visit(expr),
        ExprKind::Binary { left, right, .. }
        | ExprKind::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            walk_leaves_ref(left, visit);
            walk_leaves_ref(right, visit);
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            walk_leaves_ref(expr, visit)
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                walk_leaves_ref(when, visit);
                walk_leaves_ref(then, visit);
            }
            walk_leaves_ref(else_expr, visit);
        }
        ExprKind::ScalarFunction { args, .. } => {
            for argument in args {
                walk_leaves_ref(argument, visit);
            }
        }
    }
}

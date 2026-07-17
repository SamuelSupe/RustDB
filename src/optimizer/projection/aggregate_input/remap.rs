use crate::sql::{AggregateExpr, BoundExpr, ExprKind};

pub(super) fn required_columns(groups: &[BoundExpr], aggregates: &[AggregateExpr]) -> Vec<usize> {
    let mut columns = Vec::new();
    for expression in groups {
        expression.referenced_columns(&mut columns);
    }
    for aggregate in aggregates {
        if let Some(expression) = &aggregate.expr {
            expression.referenced_columns(&mut columns);
        }
    }
    columns.sort_unstable();
    columns.dedup();
    columns
}

pub(super) fn aggregate(
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    remap: &[Option<usize>],
) -> Option<(Vec<BoundExpr>, Vec<AggregateExpr>)> {
    let mut groups = groups.to_vec();
    let mut aggregates = aggregates.to_vec();
    for expression in &mut groups {
        expression_columns(expression, remap)?;
    }
    for aggregate in &mut aggregates {
        if let Some(expression) = &mut aggregate.expr {
            expression_columns(expression, remap)?;
        }
    }
    Some((groups, aggregates))
}

fn expression_columns(expression: &mut BoundExpr, remap: &[Option<usize>]) -> Option<()> {
    match &mut expression.kind {
        ExprKind::Column(index) => *index = remap.get(*index).copied().flatten()?,
        ExprKind::Literal(_) => {}
        ExprKind::OuterRef { .. } | ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => {
            return None;
        }
        ExprKind::Binary { left, right, .. }
        | ExprKind::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            expression_columns(left, remap)?;
            expression_columns(right, remap)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            expression_columns(expr, remap)?
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                expression_columns(when, remap)?;
                expression_columns(then, remap)?;
            }
            expression_columns(else_expr, remap)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for argument in args {
                expression_columns(argument, remap)?;
            }
        }
    }
    Some(())
}

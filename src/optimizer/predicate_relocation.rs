use crate::sql::{BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan, UnaryOp};

/// Moves structurally infallible, single-side predicates below joins that
/// preserve their semantics. This is intentionally narrower than general
/// predicate pushdown: changing the evaluation order of a cast, arithmetic
/// expression, function, or CASE can hide or expose a structured SQL error.
pub(super) fn apply(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => relocate(predicate, apply(*input), schema),
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => LogicalPlan::Projection {
            input: Box::new(apply(*input)),
            expressions,
            schema,
        },
        LogicalPlan::Scalarize { input, schema } => LogicalPlan::Scalarize {
            input: Box::new(apply(*input)),
            schema,
        },
        LogicalPlan::DependentJoin {
            left,
            right,
            kind,
            guard,
            schema,
        } => LogicalPlan::DependentJoin {
            left: Box::new(apply(*left)),
            right: Box::new(apply(*right)),
            kind,
            guard,
            schema,
        },
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(apply(*input)),
            group_exprs,
            aggregate_exprs,
            schema,
        },
        LogicalPlan::Sort {
            input,
            expressions,
            fetch,
            schema,
        } => LogicalPlan::Sort {
            input: Box::new(apply(*input)),
            expressions,
            fetch,
            schema,
        },
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            schema,
        } => LogicalPlan::Limit {
            input: Box::new(apply(*input)),
            offset,
            limit,
            schema,
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            join_type,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(apply(*left)),
            right: Box::new(apply(*right)),
            on,
            residual,
            null_aware,
            join_type,
            schema,
        },
        leaf @ (LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. }) => leaf,
    }
}

fn relocate(
    predicate: BoundExpr,
    input: LogicalPlan,
    schema: crate::sql::PlanSchema,
) -> LogicalPlan {
    if !infallible(&predicate) {
        return filter(input, predicate, schema);
    }
    match input {
        LogicalPlan::Filter {
            input,
            predicate: existing,
            ..
        } if infallible(&existing) => {
            let child_schema = input.schema().clone();
            LogicalPlan::Filter {
                input: Box::new(relocate(predicate, *input, child_schema)),
                predicate: existing,
                schema,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            join_type,
            schema: join_schema,
        } if filters_left(join_type) && references_only_left(&predicate, &left) => {
            let left_schema = left.schema().clone();
            LogicalPlan::Join {
                left: Box::new(relocate(predicate, *left, left_schema)),
                right,
                on,
                residual,
                null_aware,
                join_type,
                schema: join_schema,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            join_type: JoinType::Inner,
            schema: join_schema,
        } if inner_predicates_infallible(&on, residual.as_ref(), null_aware.as_ref())
            && references_only_left(&predicate, &left) =>
        {
            let left_schema = left.schema().clone();
            LogicalPlan::Join {
                left: Box::new(relocate(predicate, *left, left_schema)),
                right,
                on,
                residual,
                null_aware,
                join_type: JoinType::Inner,
                schema: join_schema,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            join_type: JoinType::Inner,
            schema: join_schema,
        } if inner_predicates_infallible(&on, residual.as_ref(), null_aware.as_ref())
            && references_only_right(&predicate, &left, &right) =>
        {
            let right_schema = right.schema().clone();
            let mut predicate = predicate;
            rebase_right_columns(&mut predicate, left.schema().arrow().fields().len());
            LogicalPlan::Join {
                left,
                right: Box::new(relocate(predicate, *right, right_schema)),
                on,
                residual,
                null_aware,
                join_type: JoinType::Inner,
                schema: join_schema,
            }
        }
        input => filter(input, predicate, schema),
    }
}

fn filter(input: LogicalPlan, predicate: BoundExpr, schema: crate::sql::PlanSchema) -> LogicalPlan {
    LogicalPlan::Filter {
        input: Box::new(input),
        predicate,
        schema,
    }
}

fn filters_left(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti
    )
}

fn references_only_left(predicate: &BoundExpr, left: &LogicalPlan) -> bool {
    let mut columns = Vec::new();
    predicate.referenced_columns(&mut columns);
    let width = left.schema().arrow().fields().len();
    columns.into_iter().all(|index| index < width)
}

fn references_only_right(predicate: &BoundExpr, left: &LogicalPlan, right: &LogicalPlan) -> bool {
    let mut columns = Vec::new();
    predicate.referenced_columns(&mut columns);
    let left_width = left.schema().arrow().fields().len();
    let output_width = left_width.saturating_add(right.schema().arrow().fields().len());
    !columns.is_empty()
        && columns
            .into_iter()
            .all(|index| index >= left_width && index < output_width)
}

fn inner_predicates_infallible(
    on: &[(BoundExpr, BoundExpr)],
    residual: Option<&BoundExpr>,
    null_aware: Option<&(BoundExpr, BoundExpr)>,
) -> bool {
    on.iter()
        .all(|(left, right)| infallible(left) && infallible(right))
        && residual.is_none_or(infallible)
        && null_aware.is_none_or(|(left, right)| infallible(left) && infallible(right))
}

fn rebase_right_columns(expression: &mut BoundExpr, left_width: usize) {
    match &mut expression.kind {
        ExprKind::Column(index) => *index -= left_width,
        ExprKind::Binary { left, right, .. } => {
            rebase_right_columns(left, left_width);
            rebase_right_columns(right, left_width);
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            rebase_right_columns(expr, left_width);
        }
        ExprKind::Like { expr, pattern, .. } => {
            rebase_right_columns(expr, left_width);
            rebase_right_columns(pattern, left_width);
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                rebase_right_columns(when, left_width);
                rebase_right_columns(then, left_width);
            }
            rebase_right_columns(else_expr, left_width);
        }
        ExprKind::ScalarFunction { args, .. } => {
            for argument in args {
                rebase_right_columns(argument, left_width);
            }
        }
        ExprKind::Literal(_)
        | ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_) => {}
    }
}

fn infallible(expression: &BoundExpr) -> bool {
    match &expression.kind {
        ExprKind::Column(_) | ExprKind::Literal(_) => true,
        ExprKind::Binary {
            left,
            op:
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::And
                | BinaryOp::Or,
            right,
        } => infallible(left) && infallible(right),
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        }
        | ExprKind::IsNull { expr, .. } => infallible(expr),
        ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Binary { .. }
        | ExprKind::Unary { .. }
        | ExprKind::Like { .. }
        | ExprKind::Case { .. }
        | ExprKind::Cast { .. }
        | ExprKind::ScalarFunction { .. } => false,
    }
}

#[cfg(test)]
mod tests;

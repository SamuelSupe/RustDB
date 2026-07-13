use crate::sql::{BoundExpr, ExprKind, JoinType, LogicalPlan};

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
        LogicalPlan::Append { inputs, schema } => LogicalPlan::Append {
            inputs: inputs.into_iter().map(apply).collect(),
            schema,
        },
        LogicalPlan::Repeat {
            input,
            count,
            schema,
        } => LogicalPlan::Repeat {
            input: Box::new(apply(*input)),
            count,
            schema,
        },
        LogicalPlan::Window {
            input,
            expressions,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(apply(*input)),
            expressions,
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
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(apply(*left)),
            right: Box::new(apply(*right)),
            on,
            null_equal_keys,
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
    if !predicate.is_structurally_infallible() {
        return filter(input, predicate, schema);
    }
    match input {
        LogicalPlan::Projection {
            input,
            expressions,
            schema: projection_schema,
        } if expressions
            .iter()
            .all(BoundExpr::is_structurally_infallible) =>
        {
            let Some(mapped) = remap_projection_columns(&predicate, &expressions) else {
                return filter(
                    LogicalPlan::Projection {
                        input,
                        expressions,
                        schema: projection_schema,
                    },
                    predicate,
                    schema,
                );
            };
            // Deferred aggregate planning reorders generated subquery marker
            // columns through this projection. Moving the marker predicate
            // below it would let decorrelation lower Mark to a left-only
            // Semi/Anti join while this projection still consumes the marker.
            if references_generated_attachment(&mapped, input.schema()) {
                return filter(
                    LogicalPlan::Projection {
                        input,
                        expressions,
                        schema: projection_schema,
                    },
                    predicate,
                    schema,
                );
            }
            let input_schema = input.schema().clone();
            LogicalPlan::Projection {
                input: Box::new(relocate(mapped, *input, input_schema)),
                expressions,
                schema: projection_schema,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type: JoinType::Left,
            schema: join_schema,
        } if inner_predicates_infallible(&on, residual.as_ref(), null_aware.as_ref())
            && references_only_left(&predicate, &left) =>
        {
            let left_schema = left.schema().clone();
            LogicalPlan::Join {
                left: Box::new(relocate(predicate, *left, left_schema)),
                right,
                on,
                null_equal_keys,
                residual,
                null_aware,
                join_type: JoinType::Left,
                schema: join_schema,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type: JoinType::Right,
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
                null_equal_keys,
                residual,
                null_aware,
                join_type: JoinType::Right,
                schema: join_schema,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
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
                null_equal_keys,
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
            null_equal_keys,
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
                null_equal_keys,
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
            null_equal_keys,
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
                null_equal_keys,
                residual,
                null_aware,
                join_type: JoinType::Inner,
                schema: join_schema,
            }
        }
        input => filter(input, predicate, schema),
    }
}

fn references_generated_attachment(
    expression: &BoundExpr,
    schema: &crate::sql::PlanSchema,
) -> bool {
    let mut columns = Vec::new();
    expression.referenced_columns(&mut columns);
    columns.into_iter().any(|index| {
        schema
            .arrow()
            .fields()
            .get(index)
            .is_some_and(|field| field.name().starts_with("__rustdb_scalar_subquery_"))
    })
}

/// Alias, subquery attachment, and derived-table planning introduce
/// projections between a filter and its source. A predicate can cross one
/// when every referenced output is a direct input column.
pub(super) fn remap_projection_columns(
    expression: &BoundExpr,
    projection: &[BoundExpr],
) -> Option<BoundExpr> {
    let mut mapped = expression.clone();
    match &mut mapped.kind {
        ExprKind::Column(index) => {
            let source = projection.get(*index)?;
            matches!(&source.kind, ExprKind::Column(_)).then(|| source.clone())
        }
        ExprKind::OuterRef { .. } | ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => {
            None
        }
        ExprKind::Literal(_) => Some(mapped),
        ExprKind::Binary { left, right, .. } => {
            **left = remap_projection_columns(left, projection)?;
            **right = remap_projection_columns(right, projection)?;
            Some(mapped)
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            **expr = remap_projection_columns(expr, projection)?;
            Some(mapped)
        }
        ExprKind::Like { expr, pattern, .. } => {
            **expr = remap_projection_columns(expr, projection)?;
            **pattern = remap_projection_columns(pattern, projection)?;
            Some(mapped)
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
            Some(mapped)
        }
        ExprKind::ScalarFunction { args, .. } => {
            for argument in args {
                *argument = remap_projection_columns(argument, projection)?;
            }
            Some(mapped)
        }
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
    on.iter().all(|(left, right)| {
        left.is_structurally_infallible() && right.is_structurally_infallible()
    }) && residual.is_none_or(BoundExpr::is_structurally_infallible)
        && null_aware.is_none_or(|(left, right)| {
            left.is_structurally_infallible() && right.is_structurally_infallible()
        })
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

#[cfg(test)]
mod tests;

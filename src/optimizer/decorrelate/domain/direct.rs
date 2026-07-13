use arrow::datatypes::{Field, Schema};
use std::sync::Arc;

use crate::Result;
use crate::sql::{
    AggregateExpr, AggregateFunction, BoundExpr, DependentJoinKind, ExprKind, JoinType,
    LogicalPlan, PlanSchema, ScalarValue,
};

use super::super::pull::Correlation;
use super::super::rewrite::remap_columns;
use super::compact::Compacted;

/// Eliminates the correlation-domain scan for a global scalar aggregate whose
/// only correlations are equality keys. The grouped inner side is independent
/// of the outer rows, so it can be built once and left-joined directly.
pub(super) fn rewrite(
    left: LogicalPlan,
    pulled: &Compacted,
    aggregates: &[AggregateExpr],
    projection: &[BoundExpr],
    kind: &DependentJoinKind,
    guard: Option<BoundExpr>,
    output_schema: &PlanSchema,
) -> Result<Option<LogicalPlan>> {
    if !matches!(kind, DependentJoinKind::Scalar)
        || projection.len() != 1
        || contains_subquery_attachment(&pulled.plan)
        || !plan_is_structurally_infallible(&pulled.plan)
        || aggregates.iter().any(|aggregate| {
            !aggregate_is_supported(aggregate) || !aggregate_is_structurally_infallible(aggregate)
        })
        || !projection_references_aggregates(&projection[0], aggregates.len())
        // Without an outer guard the scalar projection is evaluated exactly
        // once per real outer row in both plans, so its errors remain visible
        // in the same places. A guarded subquery must keep a fallible
        // projection on the domain path so an inactive branch stays lazy.
        || (guard.is_some() && !projection[0].is_structurally_infallible())
    {
        return Ok(None);
    }

    let mut outer_indices = Vec::with_capacity(pulled.correlations.len());
    let mut group_exprs = Vec::with_capacity(pulled.correlations.len());
    for correlation in &pulled.correlations {
        let Correlation::Key { outer, inner } = correlation else {
            return Ok(None);
        };
        let ExprKind::OuterRef { depth: 1, index } = &outer.kind else {
            return Ok(None);
        };
        if outer_indices.contains(index) {
            return Ok(None);
        }
        outer_indices.push(*index);
        group_exprs.push(inner.clone());
    }
    if group_exprs.is_empty() {
        return Ok(None);
    }

    let key_count = group_exprs.len();
    let direct_aggregates = aggregates
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, mut aggregate)| {
            aggregate.display_name = format!("__direct_correlated_{index}");
            aggregate
        })
        .collect::<Vec<_>>();
    let aggregate_schema = aggregate_schema(&group_exprs, &direct_aggregates);
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(pulled.plan.clone()),
        group_exprs,
        aggregate_exprs: direct_aggregates,
        schema: aggregate_schema.clone(),
    };

    let mut on = Vec::with_capacity(key_count);
    for (position, outer_index) in outer_indices.into_iter().enumerate() {
        let left_field = left.schema().arrow().field(outer_index);
        let right_field = aggregate.schema().arrow().field(position);
        on.push((
            BoundExpr::column(
                outer_index,
                left_field.data_type().clone(),
                left_field.name(),
            ),
            BoundExpr::column(
                position,
                right_field.data_type().clone(),
                right_field.name(),
            ),
        ));
    }

    let left_width = left.schema().arrow().fields().len();
    let join_schema = PlanSchema::left_join(left.schema(), aggregate.schema());
    let join = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(aggregate),
        on,
        null_equal_keys: false,
        residual: None,
        null_aware: None,
        join_type: JoinType::Left,
        schema: join_schema,
    };
    let aggregate_mapping = (0..aggregates.len())
        .map(|index| left_width + key_count + index)
        .collect::<Vec<_>>();
    let mut result = projection[0].clone();
    remap_columns(&mut result, &aggregate_mapping)?;
    restore_count_empty_values(&mut result, aggregates, left_width + key_count);
    result.display_name = output_schema.arrow().field(left_width).name().clone();
    if let Some(guard) = guard {
        return crate::sql::scalar_subquery::guarded::project(
            join,
            result,
            guard,
            output_schema.clone(),
        )
        .map(Some);
    }

    let mut expressions = (0..left_width)
        .map(|index| {
            let field = output_schema.arrow().field(index);
            BoundExpr::column(index, field.data_type().clone(), field.name())
        })
        .collect::<Vec<_>>();
    expressions.push(result);
    Ok(Some(LogicalPlan::Projection {
        input: Box::new(join),
        expressions,
        schema: output_schema.clone(),
    }))
}

fn plan_is_structurally_infallible(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => true,
        LogicalPlan::Filter {
            input, predicate, ..
        } => expr_is_structurally_infallible(predicate) && plan_is_structurally_infallible(input),
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            expressions.iter().all(expr_is_structurally_infallible)
                && plan_is_structurally_infallible(input)
        }
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            ..
        } => {
            group_exprs.iter().all(expr_is_structurally_infallible)
                && aggregate_exprs.iter().all(|aggregate| {
                    aggregate
                        .expr
                        .as_ref()
                        .is_none_or(expr_is_structurally_infallible)
                })
                && plan_is_structurally_infallible(input)
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            ..
        } => {
            on.iter().all(|(left, right)| {
                expr_is_structurally_infallible(left) && expr_is_structurally_infallible(right)
            }) && residual
                .as_ref()
                .is_none_or(expr_is_structurally_infallible)
                && null_aware.as_ref().is_none_or(|(left, right)| {
                    expr_is_structurally_infallible(left) && expr_is_structurally_infallible(right)
                })
                && plan_is_structurally_infallible(left)
                && plan_is_structurally_infallible(right)
        }
        LogicalPlan::Sort {
            input, expressions, ..
        } => {
            expressions
                .iter()
                .all(|expression| expr_is_structurally_infallible(&expression.expr))
                && plan_is_structurally_infallible(input)
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Scalarize { input, .. } => {
            plan_is_structurally_infallible(input)
        }
        LogicalPlan::Repeat { input, count, .. } => {
            expr_is_structurally_infallible(count) && plan_is_structurally_infallible(input)
        }
        LogicalPlan::Append { inputs, .. } => inputs.iter().all(plan_is_structurally_infallible),
        LogicalPlan::Window { .. } | LogicalPlan::DependentJoin { .. } => false,
    }
}

fn expr_is_structurally_infallible(expression: &BoundExpr) -> bool {
    expression.is_structurally_infallible()
}

fn aggregate_is_supported(aggregate: &AggregateExpr) -> bool {
    if aggregate.distinct {
        return false;
    }
    match aggregate.function {
        AggregateFunction::Count => aggregate
            .expr
            .as_ref()
            .is_none_or(|expression| !expression.contains_outer_ref()),
        AggregateFunction::Sum
        | AggregateFunction::Avg
        | AggregateFunction::Min
        | AggregateFunction::Max => aggregate
            .expr
            .as_ref()
            .is_some_and(|expression| !expression.contains_outer_ref()),
    }
}

fn aggregate_is_structurally_infallible(aggregate: &AggregateExpr) -> bool {
    aggregate
        .expr
        .as_ref()
        .is_none_or(BoundExpr::is_structurally_infallible)
}

fn projection_references_aggregates(expression: &BoundExpr, aggregate_count: usize) -> bool {
    let mut columns = Vec::new();
    expression.referenced_columns(&mut columns);
    columns.into_iter().all(|index| index < aggregate_count)
}

fn restore_count_empty_values(
    expression: &mut BoundExpr,
    aggregates: &[AggregateExpr],
    first_aggregate_column: usize,
) {
    match &mut expression.kind {
        ExprKind::Column(index) => {
            let Some(aggregate_index) = index.checked_sub(first_aggregate_column) else {
                return;
            };
            if aggregates
                .get(aggregate_index)
                .is_some_and(|aggregate| aggregate.function == AggregateFunction::Count)
            {
                let value = expression.clone();
                let is_null = BoundExpr {
                    kind: ExprKind::IsNull {
                        expr: Box::new(value.clone()),
                        negated: false,
                    },
                    data_type: arrow::datatypes::DataType::Boolean,
                    display_name: format!("{} IS NULL", value.display_name),
                };
                let mut zero = BoundExpr::literal(ScalarValue::Int64(0));
                if zero.data_type != expression.data_type {
                    zero = BoundExpr {
                        kind: ExprKind::Cast {
                            expr: Box::new(zero),
                        },
                        data_type: expression.data_type.clone(),
                        display_name: "0".into(),
                    };
                }
                expression.kind = ExprKind::Case {
                    when_then: vec![(is_null, zero)],
                    else_expr: Box::new(value),
                };
            }
        }
        ExprKind::Binary { left, right, .. } => {
            restore_count_empty_values(left, aggregates, first_aggregate_column);
            restore_count_empty_values(right, aggregates, first_aggregate_column);
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            restore_count_empty_values(expr, aggregates, first_aggregate_column);
        }
        ExprKind::Like { expr, pattern, .. } => {
            restore_count_empty_values(expr, aggregates, first_aggregate_column);
            restore_count_empty_values(pattern, aggregates, first_aggregate_column);
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                restore_count_empty_values(when, aggregates, first_aggregate_column);
                restore_count_empty_values(then, aggregates, first_aggregate_column);
            }
            restore_count_empty_values(else_expr, aggregates, first_aggregate_column);
        }
        ExprKind::ScalarFunction { args, .. } => {
            for argument in args {
                restore_count_empty_values(argument, aggregates, first_aggregate_column);
            }
        }
        ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => {}
    }
}

fn contains_subquery_attachment(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::DependentJoin { .. } => true,
        LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } => {
            !matches!(
                join_type,
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
            ) || contains_subquery_attachment(left)
                || contains_subquery_attachment(right)
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Scalarize { input, .. }
        | LogicalPlan::Repeat { input, .. } => contains_subquery_attachment(input),
        LogicalPlan::Append { inputs, .. } => inputs.iter().any(contains_subquery_attachment),
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => false,
    }
}

fn aggregate_schema(groups: &[BoundExpr], aggregates: &[AggregateExpr]) -> PlanSchema {
    let fields = groups
        .iter()
        .map(|expression| {
            Field::new(
                expression.display_name.clone(),
                expression.data_type.clone(),
                true,
            )
        })
        .chain(aggregates.iter().map(|aggregate| {
            Field::new(
                aggregate.display_name.clone(),
                aggregate.data_type.clone(),
                true,
            )
        }))
        .collect::<Vec<_>>();
    PlanSchema::unqualified(Arc::new(Schema::new(fields)))
}

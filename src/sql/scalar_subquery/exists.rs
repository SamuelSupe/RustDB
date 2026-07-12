use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use crate::{Error, Result};

use crate::sql::{AggregateExpr, AggregateFunction};

use super::{BoundExpr, ExprKind, LogicalPlan, PlanSchema, one_field_schema};

/// EXISTS observes only whether its input produces a row. SQL expressions in
/// the visible SELECT list must therefore not be evaluated.
pub(super) fn erase_output(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Projection { input, .. } => {
            let input = prune_projection_input(*input)?;
            let mut expression = BoundExpr::literal(crate::sql::ScalarValue::Int64(1));
            expression.display_name = "__rustdb_exists_row".into();
            let schema = one_field_schema(
                "__rustdb_exists_row",
                arrow::datatypes::DataType::Int64,
                false,
            );
            LogicalPlan::Projection {
                input: Box::new(input),
                expressions: vec![expression],
                schema,
            }
        }
        // SELECT DISTINCT is represented as a grouping Aggregate over the
        // visible projection. Without OFFSET, duplicate elimination cannot
        // change whether any row exists, so both it and the visible
        // expressions can be removed.
        LogicalPlan::Aggregate {
            input,
            aggregate_exprs,
            ..
        } if aggregate_exprs.is_empty() => erase_output(*input)?,
        LogicalPlan::Sort { input, .. } => erase_output(*input)?,
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            ..
        } => {
            if offset > 0 && projection_controls_offset_cardinality(&input) {
                let schema = input.schema().clone();
                return Ok(LogicalPlan::Limit {
                    input,
                    offset,
                    limit,
                    schema,
                });
            }
            let input = erase_output(*input)?;
            let schema = input.schema().clone();
            LogicalPlan::Limit {
                input: Box::new(input),
                offset,
                limit,
                schema,
            }
        }
        other => other,
    })
}

fn projection_controls_offset_cardinality(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Sort { input, .. } => projection_controls_offset_cardinality(input),
        LogicalPlan::Aggregate {
            aggregate_exprs, ..
        } => aggregate_exprs.is_empty(),
        _ => false,
    }
}

fn prune_projection_input(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            schema,
            ..
        } => {
            let group_width = group_exprs.len();
            let selected = (0..group_width).collect::<Vec<_>>();
            let (aggregate_exprs, schema) = if group_width == 0 {
                (vec![synthetic_count()], synthetic_count_schema())
            } else {
                (Vec::new(), select_schema(&schema, &selected))
            };
            Ok(LogicalPlan::Aggregate {
                input,
                group_exprs,
                aggregate_exprs,
                schema,
            })
        }
        LogicalPlan::Filter {
            input,
            mut predicate,
            ..
        } => {
            let LogicalPlan::Aggregate {
                input,
                group_exprs,
                aggregate_exprs,
                schema,
            } = *input
            else {
                let schema = input.schema().clone();
                return Ok(LogicalPlan::Filter {
                    input,
                    predicate,
                    schema,
                });
            };
            let group_width = group_exprs.len();
            let mut required = Vec::new();
            predicate.referenced_columns(&mut required);
            required.sort_unstable();
            required.dedup();

            let mut selected = (0..group_width).collect::<Vec<_>>();
            selected.extend((0..aggregate_exprs.len()).filter_map(|index| {
                required
                    .contains(&(group_width + index))
                    .then_some(group_width + index)
            }));
            let mut mapping = vec![usize::MAX; group_width + aggregate_exprs.len()];
            for (new, old) in selected.iter().copied().enumerate() {
                mapping[old] = new;
            }
            remap_columns(&mut predicate, &mapping)?;

            let mut aggregate_exprs: Vec<AggregateExpr> = aggregate_exprs
                .into_iter()
                .enumerate()
                .filter_map(|(index, expression)| {
                    required
                        .contains(&(group_width + index))
                        .then_some(expression)
                })
                .collect();
            let schema = if group_width == 0 && aggregate_exprs.is_empty() {
                aggregate_exprs.push(synthetic_count());
                synthetic_count_schema()
            } else {
                select_schema(&schema, &selected)
            };
            let aggregate = LogicalPlan::Aggregate {
                input,
                group_exprs,
                aggregate_exprs,
                schema: schema.clone(),
            };
            Ok(LogicalPlan::Filter {
                input: Box::new(aggregate),
                predicate,
                schema,
            })
        }
        other => Ok(other),
    }
}

fn synthetic_count() -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct: false,
        data_type: DataType::Int64,
        display_name: "__rustdb_exists_count".into(),
    }
}

fn synthetic_count_schema() -> PlanSchema {
    PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
        "__rustdb_exists_count",
        DataType::Int64,
        false,
    )])))
}

fn select_schema(schema: &PlanSchema, indices: &[usize]) -> PlanSchema {
    let fields = indices
        .iter()
        .map(|index| Arc::clone(&schema.arrow().fields()[*index]))
        .collect::<Vec<_>>();
    let qualifiers = indices
        .iter()
        .map(|index| schema.qualifier(*index).map(str::to_owned))
        .collect();
    PlanSchema::new(Arc::new(Schema::new(fields)), qualifiers)
}

fn remap_columns(expr: &mut BoundExpr, mapping: &[usize]) -> Result<()> {
    match &mut expr.kind {
        ExprKind::Column(index) => {
            *index = mapping
                .get(*index)
                .copied()
                .filter(|mapped| *mapped != usize::MAX)
                .ok_or_else(|| {
                    Error::Internal(format!(
                        "EXISTS HAVING expression references removed aggregate column {index}"
                    ))
                })?;
        }
        ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. }
        | ExprKind::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            remap_columns(left, mapping)?;
            remap_columns(right, mapping)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            remap_columns(expr, mapping)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                remap_columns(when, mapping)?;
                remap_columns(then, mapping)?;
            }
            remap_columns(else_expr, mapping)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for argument in args {
                remap_columns(argument, mapping)?;
            }
        }
    }
    Ok(())
}

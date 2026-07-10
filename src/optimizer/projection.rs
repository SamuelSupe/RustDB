use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::sql::{BoundExpr, ExprKind, JoinType, LogicalPlan, PlanSchema};

pub(super) fn push_required_columns(plan: &mut LogicalPlan) {
    let required = (0..plan.schema().arrow().fields().len()).collect::<Vec<_>>();
    push(plan, &required);
}

fn push(plan: &mut LogicalPlan, required: &[usize]) {
    match plan {
        LogicalPlan::Empty { .. } => {}
        LogicalPlan::Scan {
            projection,
            pushed_filter,
            schema,
            ..
        } => {
            let mut columns = required.to_vec();
            if let Some(predicate) = pushed_filter {
                predicate.referenced_columns(&mut columns);
            }
            set_projection(projection, columns);
            *schema = nullable_unrequired(schema, required);
        }
        LogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => {
            let mut columns = required.to_vec();
            predicate.referenced_columns(&mut columns);
            *schema = nullable_unrequired(schema, required);
            push(input, &columns);
        }
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => {
            let mut columns = Vec::new();
            let selected: Box<dyn Iterator<Item = usize>> = if transparent(expressions) {
                Box::new(required.iter().copied())
            } else {
                Box::new(0..expressions.len())
            };
            for index in selected {
                if let Some(expression) = expressions.get(index) {
                    expression.referenced_columns(&mut columns);
                }
            }
            *schema = nullable_unrequired(schema, required);
            push(input, &columns);
        }
        LogicalPlan::Scalarize { input, .. } => push(input, &[0]),
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            ..
        } => {
            // The current aggregate executor materializes every configured
            // state. Keep every state input until aggregate-output pruning is
            // implemented, while still pushing the complete requirement down.
            let mut columns = referenced_columns(group_exprs);
            for aggregate in aggregate_exprs {
                if let Some(expr) = &aggregate.expr {
                    expr.referenced_columns(&mut columns);
                }
            }
            push(input, &columns);
        }
        LogicalPlan::Sort {
            input,
            expressions,
            schema,
            ..
        } => {
            let mut columns = required.to_vec();
            for expression in expressions {
                expression.expr.referenced_columns(&mut columns);
            }
            *schema = nullable_unrequired(schema, required);
            push(input, &columns);
        }
        LogicalPlan::Limit { input, schema, .. } => {
            *schema = nullable_unrequired(schema, required);
            push(input, required);
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            schema,
            ..
        } => {
            let left_width = left.schema().arrow().fields().len();
            let mut left_columns = Vec::new();
            let mut right_columns = Vec::new();
            for index in required {
                if *index < left_width {
                    left_columns.push(*index);
                } else if matches!(join_type, JoinType::Inner | JoinType::Left) {
                    right_columns.push(*index - left_width);
                }
            }
            for (left, right) in on {
                left.referenced_columns(&mut left_columns);
                right.referenced_columns(&mut right_columns);
            }
            *schema = nullable_unrequired(schema, required);
            push(left, &left_columns);
            push(right, &right_columns);
        }
    }
}

fn transparent(expressions: &[BoundExpr]) -> bool {
    expressions
        .iter()
        .all(|expression| matches!(&expression.kind, ExprKind::Column(_)))
}

fn nullable_unrequired(schema: &PlanSchema, required: &[usize]) -> PlanSchema {
    let fields = schema
        .arrow()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if required.contains(&index) || field.is_nullable() {
                Arc::clone(field)
            } else {
                Arc::new(field.as_ref().clone().with_nullable(true))
            }
        })
        .collect::<Vec<_>>();
    let qualifiers = (0..fields.len())
        .map(|index| schema.qualifier(index).map(str::to_owned))
        .collect();
    PlanSchema::new(Arc::new(Schema::new(fields)), qualifiers)
}

fn referenced_columns(expressions: &[BoundExpr]) -> Vec<usize> {
    let mut columns = Vec::new();
    for expression in expressions {
        expression.referenced_columns(&mut columns);
    }
    columns
}

fn set_projection(target: &mut Option<Vec<usize>>, mut columns: Vec<usize>) {
    columns.sort_unstable();
    columns.dedup();
    if !columns.is_empty() {
        *target = Some(columns);
    }
}

#[cfg(test)]
mod tests {
    use super::set_projection;

    #[test]
    fn projections_are_sorted_and_deduplicated() {
        let mut target = None;
        set_projection(&mut target, vec![3, 1, 3, 2]);
        assert_eq!(target, Some(vec![1, 2, 3]));
    }
}

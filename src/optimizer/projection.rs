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
            for index in 0..expressions.len() {
                // Preserve evaluation of every non-trivial expression even
                // when its output is unused: casts, arithmetic and scalar
                // functions may return a structured runtime error. Direct
                // columns and literals are side-effect free, so only retain
                // those when a parent consumes their output position.
                if !required.contains(&index) && cheaply_prunable(&expressions[index]) {
                    continue;
                }
                if let Some(expression) = expressions.get(index) {
                    expression.referenced_columns(&mut columns);
                }
            }
            // A zero-column scan deliberately returns a compact batch.  A
            // projection that still materializes direct input columns needs
            // one physical anchor so the scan expands back to its logical
            // width; the remaining, unconsumed columns stay NULL placeholders.
            if columns.is_empty()
                && !input.schema().arrow().fields().is_empty()
                && let Some(anchor) = first_referenced_column(expressions)
            {
                columns.push(anchor);
            }
            *schema = nullable_unrequired(schema, required);
            push(input, &columns);
        }
        LogicalPlan::Scalarize { input, .. } => push(input, &[0]),
        LogicalPlan::DependentJoin {
            left,
            right,
            kind,
            guard,
            ..
        } => {
            let mut left_columns = required
                .iter()
                .copied()
                .filter(|index| *index < left.schema().arrow().fields().len())
                .collect::<Vec<_>>();
            if let crate::sql::DependentJoinKind::In { needle }
            | crate::sql::DependentJoinKind::InFilter { needle, .. } = kind
            {
                needle.referenced_columns(&mut left_columns);
            }
            if let Some(guard) = guard {
                guard.referenced_columns(&mut left_columns);
            }
            let right_columns = (0..right.schema().arrow().fields().len()).collect::<Vec<_>>();
            push(left, &left_columns);
            push(right, &right_columns);
        }
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
            residual,
            null_aware,
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
                } else if matches!(
                    join_type,
                    JoinType::Inner | JoinType::Left | JoinType::LeftSingle
                ) {
                    right_columns.push(*index - left_width);
                }
            }
            for (left, right) in on.iter() {
                left.referenced_columns(&mut left_columns);
                right.referenced_columns(&mut right_columns);
            }
            if let Some(residual) = residual {
                let mut columns = Vec::new();
                residual.referenced_columns(&mut columns);
                for index in columns {
                    if index < left_width {
                        left_columns.push(index);
                    } else {
                        right_columns.push(index - left_width);
                    }
                }
            }
            if let Some((left, right)) = null_aware {
                left.referenced_columns(&mut left_columns);
                right.referenced_columns(&mut right_columns);
            }
            // Join output preserves the logical child widths.  Explicit
            // zero-column scans are compact batches, so retain one physical
            // anchor only when a materialized side would otherwise have no
            // columns.  This avoids the former all-column attachment fallback
            // without reintroducing an Arrow schema-width mismatch.
            if left_columns.is_empty() && left_width != 0 {
                left_columns.push(0);
            }
            if right_columns.is_empty()
                && !right.schema().arrow().fields().is_empty()
                && matches!(
                    join_type,
                    JoinType::Inner | JoinType::Left | JoinType::LeftSingle
                )
            {
                right_columns.push(0);
            }
            *schema = nullable_unrequired(schema, required);
            push(left, &left_columns);
            push(right, &right_columns);
        }
    }
}

fn cheaply_prunable(expression: &BoundExpr) -> bool {
    matches!(&expression.kind, ExprKind::Column(_) | ExprKind::Literal(_))
}

fn first_referenced_column(expressions: &[BoundExpr]) -> Option<usize> {
    expressions.iter().find_map(|expression| {
        let mut columns = Vec::new();
        expression.referenced_columns(&mut columns);
        columns.into_iter().next()
    })
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
    // `None` means the provider must read every column.  Preserve an explicit
    // empty projection so metadata-only plans such as Parquet COUNT(*) do not
    // accidentally decode the full physical schema.
    *target = Some(columns);
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

    #[test]
    fn preserves_an_explicit_zero_column_projection() {
        let mut target = None;
        set_projection(&mut target, Vec::new());
        assert_eq!(target, Some(Vec::new()));
    }
}

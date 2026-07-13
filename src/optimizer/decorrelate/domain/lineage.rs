use crate::sql::{ExprKind, JoinType, LogicalPlan};

pub(super) struct DomainSource {
    pub(super) plan: LogicalPlan,
    pub(super) parameters: Vec<usize>,
}

/// Removes only operators that cannot change the distinct set of requested
/// parameter values. Filters, limits, inner/semi/anti joins, and grouping stay
/// in the lineage because they can remove values from the outer query block.
pub(super) fn minimum(plan: &LogicalPlan, parameters: &[usize]) -> DomainSource {
    match plan {
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            let mapped = parameters
                .iter()
                .map(|index| match &expressions.get(*index)?.kind {
                    ExprKind::Column(source) => Some(*source),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();
            mapped.map_or_else(
                || retained(plan, parameters),
                |mapped| minimum(input, &mapped),
            )
        }
        LogicalPlan::Sort {
            input,
            expressions,
            fetch: None,
            ..
        } if expressions
            .iter()
            .all(|expression| expression.expr.is_structurally_infallible()) =>
        {
            minimum(input, parameters)
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            join_type,
            ..
        } => {
            let left_width = left.schema().arrow().fields().len();
            let right_width = right.schema().arrow().fields().len();
            if preserves_left(*join_type)
                && parameters.iter().all(|index| *index < left_width)
                && conditions_are_infallible(on, residual.as_ref(), null_aware.as_ref())
                && plan_is_infallible(right)
            {
                minimum(left, parameters)
            } else if *join_type == JoinType::Right
                && parameters
                    .iter()
                    .all(|index| *index >= left_width && *index < left_width + right_width)
                && conditions_are_infallible(on, residual.as_ref(), null_aware.as_ref())
                && plan_is_infallible(left)
            {
                let rebased = parameters
                    .iter()
                    .map(|index| index - left_width)
                    .collect::<Vec<_>>();
                minimum(right, &rebased)
            } else {
                retained(plan, parameters)
            }
        }
        _ => retained(plan, parameters),
    }
}

fn preserves_left(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Left | JoinType::Mark)
}

fn conditions_are_infallible(
    on: &[(crate::sql::BoundExpr, crate::sql::BoundExpr)],
    residual: Option<&crate::sql::BoundExpr>,
    null_aware: Option<&(crate::sql::BoundExpr, crate::sql::BoundExpr)>,
) -> bool {
    on.iter().all(|(left, right)| {
        left.is_structurally_infallible() && right.is_structurally_infallible()
    }) && residual.is_none_or(crate::sql::BoundExpr::is_structurally_infallible)
        && null_aware.is_none_or(|(left, right)| {
            left.is_structurally_infallible() && right.is_structurally_infallible()
        })
}

fn plan_is_infallible(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => true,
        LogicalPlan::Filter {
            input, predicate, ..
        } => predicate.is_structurally_infallible() && plan_is_infallible(input),
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            expressions
                .iter()
                .all(crate::sql::BoundExpr::is_structurally_infallible)
                && plan_is_infallible(input)
        }
        LogicalPlan::Sort {
            input, expressions, ..
        } => {
            expressions
                .iter()
                .all(|expression| expression.expr.is_structurally_infallible())
                && plan_is_infallible(input)
        }
        LogicalPlan::Limit { input, .. } => plan_is_infallible(input),
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            join_type,
            ..
        } => {
            *join_type != JoinType::LeftSingle
                && conditions_are_infallible(on, residual.as_ref(), null_aware.as_ref())
                && plan_is_infallible(left)
                && plan_is_infallible(right)
        }
        LogicalPlan::Append { inputs, .. } => inputs.iter().all(plan_is_infallible),
        LogicalPlan::Aggregate { .. }
        | LogicalPlan::Repeat { .. }
        | LogicalPlan::Window { .. }
        | LogicalPlan::Scalarize { .. }
        | LogicalPlan::DependentJoin { .. } => false,
    }
}

fn retained(plan: &LogicalPlan, parameters: &[usize]) -> DomainSource {
    DomainSource {
        plan: plan.clone(),
        parameters: parameters.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::minimum;
    use crate::sql::{BoundExpr, JoinType, LogicalPlan, PlanSchema};

    #[test]
    fn removes_only_value_preserving_join_branches() {
        let left = empty("left");
        let right = empty("right");
        let left_join = join(left.clone(), right.clone(), JoinType::Left);
        let source = minimum(&left_join, &[0]);
        assert!(matches!(source.plan, LogicalPlan::Empty { .. }));
        assert_eq!(source.parameters, [0]);

        let right_join = join(left.clone(), right.clone(), JoinType::Right);
        let source = minimum(&right_join, &[1]);
        assert!(matches!(source.plan, LogicalPlan::Empty { .. }));
        assert_eq!(source.parameters, [0]);

        let inner = join(left, right, JoinType::Inner);
        assert!(matches!(
            minimum(&inner, &[0]).plan,
            LogicalPlan::Join { .. }
        ));
    }

    #[test]
    fn maps_direct_projection_columns_to_their_source() {
        let input = empty("source");
        let expression = BoundExpr::column(0, DataType::Int64, "alias");
        let projection = LogicalPlan::Projection {
            input: Box::new(input),
            expressions: vec![expression],
            schema: schema("alias"),
        };
        let source = minimum(&projection, &[0]);
        assert!(matches!(source.plan, LogicalPlan::Empty { .. }));
        assert_eq!(source.parameters, [0]);
    }

    #[test]
    fn retains_cardinality_and_expression_error_boundaries() {
        let left = empty("left");
        let right = empty("right");
        let single = join(left.clone(), right.clone(), JoinType::LeftSingle);
        assert!(matches!(
            minimum(&single, &[0]).plan,
            LogicalPlan::Join { .. }
        ));

        let fallible = LogicalPlan::Projection {
            input: Box::new(right),
            expressions: vec![BoundExpr {
                kind: crate::sql::ExprKind::Binary {
                    left: Box::new(BoundExpr::column(0, DataType::Int64, "right")),
                    op: crate::sql::BinaryOp::Divide,
                    right: Box::new(BoundExpr::literal(crate::sql::ScalarValue::Int64(0))),
                },
                data_type: DataType::Int64,
                display_name: "right / 0".into(),
            }],
            schema: schema("right"),
        };
        let outer = join(left, fallible, JoinType::Left);
        assert!(matches!(
            minimum(&outer, &[0]).plan,
            LogicalPlan::Join { .. }
        ));
    }

    fn join(left: LogicalPlan, right: LogicalPlan, join_type: JoinType) -> LogicalPlan {
        let schema = match join_type {
            JoinType::Left | JoinType::LeftSingle => {
                PlanSchema::left_join(left.schema(), right.schema())
            }
            JoinType::Right => PlanSchema::right_join(left.schema(), right.schema()),
            _ => PlanSchema::join(left.schema(), right.schema()),
        };
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            on: Vec::new(),
            null_equal_keys: false,
            residual: None,
            null_aware: None,
            join_type,
            schema,
        }
    }

    fn empty(name: &str) -> LogicalPlan {
        LogicalPlan::Empty {
            produce_one_row: false,
            schema: schema(name),
        }
    }

    fn schema(name: &str) -> PlanSchema {
        PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Int64,
            false,
        )])))
    }
}

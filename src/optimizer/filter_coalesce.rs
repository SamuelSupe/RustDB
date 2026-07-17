use arrow::datatypes::DataType;

use crate::sql::{BinaryOp, BoundExpr, ExprKind, LogicalPlan};

/// Coalesces only adjacent filters whose eager conjunction cannot expose a
/// structured SQL error. The inner predicate remains the left operand so the
/// existing evaluation order is preserved.
pub(super) fn apply(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => filter(apply(*input), predicate, schema),
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

fn filter(input: LogicalPlan, outer: BoundExpr, schema: crate::sql::PlanSchema) -> LogicalPlan {
    match input {
        LogicalPlan::Filter {
            input,
            predicate: inner,
            ..
        } if inner.is_structurally_infallible() && outer.is_structurally_infallible() => {
            LogicalPlan::Filter {
                input,
                predicate: conjunction(inner, outer),
                schema,
            }
        }
        input => LogicalPlan::Filter {
            input: Box::new(input),
            predicate: outer,
            schema,
        },
    }
}

fn conjunction(inner: BoundExpr, outer: BoundExpr) -> BoundExpr {
    BoundExpr {
        display_name: format!("{} AND {}", inner.display_name, outer.display_name),
        kind: ExprKind::Binary {
            left: Box::new(inner),
            op: BinaryOp::And,
            right: Box::new(outer),
        },
        data_type: DataType::Boolean,
    }
}

#[cfg(test)]
#[path = "filter_coalesce/tests.rs"]
mod tests;

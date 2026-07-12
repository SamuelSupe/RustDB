use crate::sql::{BoundExpr, LogicalPlan};
use crate::{Error, Result};

pub(super) fn executable(plan: &LogicalPlan) -> Result<()> {
    match plan {
        LogicalPlan::DependentJoin { .. } => {
            return Err(Error::Internal(
                "DependentJoin remained after decorrelation; physical execution is forbidden"
                    .into(),
            ));
        }
        LogicalPlan::Empty { .. } => {}
        LogicalPlan::Scan { pushed_filter, .. } => check_optional(pushed_filter)?,
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            check(predicate)?;
            executable(input)?;
        }
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            check_many(expressions)?;
            executable(input)?;
        }
        LogicalPlan::Scalarize { input, .. } | LogicalPlan::Limit { input, .. } => {
            executable(input)?;
        }
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            ..
        } => {
            check_many(group_exprs)?;
            for aggregate in aggregate_exprs {
                check_optional(&aggregate.expr)?;
            }
            executable(input)?;
        }
        LogicalPlan::Append { inputs, .. } => {
            for input in inputs {
                executable(input)?;
            }
        }
        LogicalPlan::Window {
            input, expressions, ..
        } => {
            for expression in expressions {
                if let crate::sql::WindowFunction::Aggregate(aggregate) = &expression.function {
                    check_optional(&aggregate.expr)?;
                }
                check_many(&expression.partition_by)?;
                for order in &expression.order_by {
                    check(&order.expr)?;
                }
            }
            executable(input)?;
        }
        LogicalPlan::Sort {
            input, expressions, ..
        } => {
            for expression in expressions {
                check(&expression.expr)?;
            }
            executable(input)?;
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
                check(left)?;
                check(right)?;
            }
            check_optional(residual)?;
            if let Some((left, right)) = null_aware {
                check(left)?;
                check(right)?;
            }
            executable(left)?;
            executable(right)?;
        }
    }
    Ok(())
}

fn check_many(expressions: &[BoundExpr]) -> Result<()> {
    for expression in expressions {
        check(expression)?;
    }
    Ok(())
}

fn check_optional(expression: &Option<BoundExpr>) -> Result<()> {
    if let Some(expression) = expression {
        check(expression)?;
    }
    Ok(())
}

fn check(expression: &BoundExpr) -> Result<()> {
    if expression.contains_deferred_aggregate() {
        Err(Error::Internal(format!(
            "deferred aggregate result remained after attachment planning in '{}'; physical execution is forbidden",
            expression.display_name
        )))
    } else if expression.contains_outer_ref() {
        Err(Error::Internal(
            "OuterRef remained after decorrelation; physical execution is forbidden".into(),
        ))
    } else {
        Ok(())
    }
}

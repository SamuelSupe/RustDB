use arrow::datatypes::DataType;
use sqlparser::ast::{BinaryOperator, Expr, Query};

use crate::{Error, Result};

use super::binder::{bind_expr, ensure_boolean, make_binary};
use super::coercion::cast_if_needed;
use super::{BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan};

pub(super) fn apply_where<F>(
    plan: LogicalPlan,
    predicate: &Expr,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
{
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = predicate
    {
        let plan = apply_where(plan, left, plan_subquery)?;
        return apply_where(plan, right, plan_subquery);
    }
    match predicate {
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => apply_in(plan, expr, subquery, plan_subquery),
        Expr::InSubquery { negated: true, .. } => Err(Error::Unsupported(
            "NOT IN subqueries require null-aware anti join semantics and are not supported".into(),
        )),
        Expr::Exists { subquery, negated } => apply_exists(plan, subquery, *negated, plan_subquery),
        _ if contains_subquery(predicate) => Err(Error::Unsupported(
            "subquery predicates are supported only as top-level AND conjuncts".into(),
        )),
        _ => apply_filter(plan, predicate),
    }
}

fn apply_in<F>(
    left_plan: LogicalPlan,
    expr: &Expr,
    subquery: &Query,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
{
    let left_expr = bind_expr(expr, left_plan.schema())?;
    let right_plan = plan_uncorrelated(subquery, plan_subquery)?;
    if right_plan.schema().arrow().fields().len() != 1 {
        return Err(Error::InvalidArgument(format!(
            "IN subquery must return exactly one column, got {}",
            right_plan.schema().arrow().fields().len()
        )));
    }
    let field = right_plan.schema().arrow().field(0);
    let equality = make_binary(
        left_expr,
        BinaryOp::Eq,
        BoundExpr::column(0, field.data_type().clone(), field.name()),
    )?;
    let ExprKind::Binary { left, right, .. } = equality.kind else {
        unreachable!("equality binding always creates a binary expression")
    };
    let schema = left_plan.schema().clone();
    Ok(LogicalPlan::Join {
        left: Box::new(left_plan),
        right: Box::new(right_plan),
        on: vec![(*left, *right)],
        join_type: JoinType::Semi,
        schema,
    })
}

fn apply_exists<F>(
    left_plan: LogicalPlan,
    subquery: &Query,
    negated: bool,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
{
    let right_plan = plan_uncorrelated(subquery, plan_subquery)?;
    let right_schema = right_plan.schema().clone();
    let right_plan = LogicalPlan::Limit {
        input: Box::new(right_plan),
        offset: 0,
        limit: Some(1),
        schema: right_schema,
    };
    let schema = left_plan.schema().clone();
    Ok(LogicalPlan::Join {
        left: Box::new(left_plan),
        right: Box::new(right_plan),
        on: Vec::new(),
        join_type: if negated {
            JoinType::Anti
        } else {
            JoinType::Semi
        },
        schema,
    })
}

fn plan_uncorrelated<F>(subquery: &Query, plan_subquery: &mut F) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
{
    match plan_subquery(subquery) {
        Err(Error::Catalog(message)) => Err(Error::Unsupported(format!(
            "correlated subqueries are not supported ({message})"
        ))),
        result => result,
    }
}

fn apply_filter(input: LogicalPlan, predicate: &Expr) -> Result<LogicalPlan> {
    let predicate = bind_expr(predicate, input.schema())?;
    ensure_boolean(&predicate).map_err(|_| {
        Error::InvalidArgument(format!(
            "WHERE requires BOOLEAN, got {}",
            predicate.data_type
        ))
    })?;
    let predicate = cast_if_needed(predicate, &DataType::Boolean);
    let schema = input.schema().clone();
    Ok(LogicalPlan::Filter {
        input: Box::new(input),
        predicate,
        schema,
    })
}

fn contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. } => true,
        Expr::BinaryOp { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => contains_subquery(left) || contains_subquery(right),
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Cast { expr, .. } => contains_subquery(expr),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(contains_subquery)
                || conditions.iter().any(|branch| {
                    contains_subquery(&branch.condition) || contains_subquery(&branch.result)
                })
                || else_result.as_deref().is_some_and(contains_subquery)
        }
        _ => false,
    }
}

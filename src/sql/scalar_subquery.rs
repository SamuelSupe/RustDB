use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use sqlparser::ast::{Expr, Ident, Query};

use crate::{Error, Result};

use super::{JoinType, LogicalPlan, PlanSchema};

pub(super) fn extract<F, N>(
    plan: LogicalPlan,
    expr: &mut Expr,
    hidden_groups: Option<&mut Vec<Expr>>,
    next_name: &mut N,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
    N: FnMut() -> String,
{
    let mut hidden_groups = hidden_groups;
    rewrite(plan, expr, &mut hidden_groups, next_name, plan_subquery)
}

fn rewrite<F, N>(
    plan: LogicalPlan,
    expr: &mut Expr,
    hidden_groups: &mut Option<&mut Vec<Expr>>,
    next_name: &mut N,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
    N: FnMut() -> String,
{
    if let Expr::Subquery(query) = expr {
        let subquery = plan_uncorrelated(query, plan_subquery)?;
        let width = subquery.schema().arrow().fields().len();
        if width != 1 {
            return Err(Error::InvalidArgument(format!(
                "scalar subquery must return exactly one column, got {width}"
            )));
        }
        let name = next_name();
        let field = subquery.schema().arrow().field(0);
        let scalar_schema = PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
            name.clone(),
            field.data_type().clone(),
            true,
        )])));
        let scalar = LogicalPlan::Scalarize {
            input: Box::new(subquery),
            schema: scalar_schema,
        };
        let schema = PlanSchema::join(plan.schema(), scalar.schema());
        let joined = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(scalar),
            on: Vec::new(),
            join_type: JoinType::Inner,
            schema,
        };
        let replacement = Expr::Identifier(Ident::new(name));
        if let Some(groups) = hidden_groups.as_deref_mut() {
            groups.push(replacement.clone());
        }
        *expr = replacement;
        return Ok(joined);
    }

    match expr {
        Expr::BinaryOp { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::ILike {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::SimilarTo {
            expr: left,
            pattern: right,
            ..
        } => {
            let plan = rewrite(plan, left, hidden_groups, next_name, plan_subquery)?;
            rewrite(plan, right, hidden_groups, next_name, plan_subquery)
        }
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
        | Expr::Cast { expr, .. } => rewrite(plan, expr, hidden_groups, next_name, plan_subquery),
        Expr::Between {
            expr, low, high, ..
        } => {
            let plan = rewrite(plan, expr, hidden_groups, next_name, plan_subquery)?;
            let plan = rewrite(plan, low, hidden_groups, next_name, plan_subquery)?;
            rewrite(plan, high, hidden_groups, next_name, plan_subquery)
        }
        Expr::InList { expr, list, .. } => {
            let mut plan = rewrite(plan, expr, hidden_groups, next_name, plan_subquery)?;
            for candidate in list {
                plan = rewrite(plan, candidate, hidden_groups, next_name, plan_subquery)?;
            }
            Ok(plan)
        }
        Expr::InSubquery { expr, .. } => {
            rewrite(plan, expr, hidden_groups, next_name, plan_subquery)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut plan = plan;
            if let Some(operand) = operand {
                plan = rewrite(plan, operand, hidden_groups, next_name, plan_subquery)?;
            }
            for branch in conditions {
                plan = rewrite(
                    plan,
                    &mut branch.condition,
                    hidden_groups,
                    next_name,
                    plan_subquery,
                )?;
                plan = rewrite(
                    plan,
                    &mut branch.result,
                    hidden_groups,
                    next_name,
                    plan_subquery,
                )?;
            }
            if let Some(else_result) = else_result {
                plan = rewrite(plan, else_result, hidden_groups, next_name, plan_subquery)?;
            }
            Ok(plan)
        }
        _ => Ok(plan),
    }
}

fn plan_uncorrelated<F>(subquery: &Query, plan_subquery: &mut F) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
{
    match plan_subquery(subquery) {
        Err(Error::Catalog(message)) => Err(Error::Unsupported(format!(
            "correlated scalar subqueries are not supported ({message})"
        ))),
        result => result,
    }
}

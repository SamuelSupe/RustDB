use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    Ident, Query, UnaryOperator,
};

use crate::{Error, Result};

use super::binder::{bind_expr, make_binary};
use super::{BinaryOp, BoundExpr, DependentJoinKind, ExprKind, JoinType, LogicalPlan, PlanSchema};

mod correlation;
mod exists;
pub(crate) mod guarded;

pub(super) use correlation::validate_outer_grouping;

pub(super) fn extract<F, N>(
    plan: LogicalPlan,
    expr: &mut Expr,
    hidden_groups: Option<&mut Vec<Expr>>,
    aggregate_groups: Option<&[Expr]>,
    next_name: &mut N,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
    N: FnMut() -> String,
{
    let mut hidden_groups = hidden_groups;
    rewrite(
        plan,
        expr,
        &mut hidden_groups,
        aggregate_groups,
        None,
        next_name,
        plan_subquery,
    )
}

fn rewrite<F, N>(
    plan: LogicalPlan,
    expr: &mut Expr,
    hidden_groups: &mut Option<&mut Vec<Expr>>,
    aggregate_groups: Option<&[Expr]>,
    guard: Option<Expr>,
    next_name: &mut N,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
    N: FnMut() -> String,
{
    match expr {
        Expr::Subquery(query) => {
            let query = query.as_ref().clone();
            return attach_scalar(
                plan,
                &query,
                expr,
                hidden_groups,
                aggregate_groups,
                guard.as_ref(),
                next_name,
                plan_subquery,
            );
        }
        Expr::Exists { subquery, negated } => {
            let query = subquery.as_ref().clone();
            let negated = *negated;
            return attach_exists(
                plan,
                &query,
                negated,
                expr,
                hidden_groups,
                aggregate_groups,
                guard.as_ref(),
                next_name,
                plan_subquery,
            );
        }
        Expr::InSubquery {
            expr: needle,
            subquery,
            negated,
        } => {
            let plan = rewrite(
                plan,
                needle,
                hidden_groups,
                aggregate_groups,
                guard.clone(),
                next_name,
                plan_subquery,
            )?;
            let needle = needle.as_ref().clone();
            let query = subquery.as_ref().clone();
            let negated = *negated;
            return attach_in(
                plan,
                &needle,
                &query,
                negated,
                expr,
                hidden_groups,
                aggregate_groups,
                guard.as_ref(),
                next_name,
                plan_subquery,
            );
        }
        _ => {}
    }

    match expr {
        Expr::BinaryOp { left, op, right } => {
            let plan = rewrite(
                plan,
                left,
                hidden_groups,
                aggregate_groups,
                guard.clone(),
                next_name,
                plan_subquery,
            )?;
            let right_guard = match op {
                SqlBinaryOperator::And => {
                    guarded::and(guard, Expr::IsNotFalse(Box::new(left.as_ref().clone())))
                }
                SqlBinaryOperator::Or => {
                    guarded::and(guard, Expr::IsNotTrue(Box::new(left.as_ref().clone())))
                }
                _ => guard,
            };
            rewrite(
                plan,
                right,
                hidden_groups,
                aggregate_groups,
                right_guard,
                next_name,
                plan_subquery,
            )
        }
        Expr::Like {
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
            let plan = rewrite(
                plan,
                left,
                hidden_groups,
                aggregate_groups,
                guard.clone(),
                next_name,
                plan_subquery,
            )?;
            rewrite(
                plan,
                right,
                hidden_groups,
                aggregate_groups,
                guard,
                next_name,
                plan_subquery,
            )
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
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. } => rewrite(
            plan,
            expr,
            hidden_groups,
            aggregate_groups,
            guard,
            next_name,
            plan_subquery,
        ),
        Expr::Between {
            expr, low, high, ..
        } => {
            let plan = rewrite(
                plan,
                expr,
                hidden_groups,
                aggregate_groups,
                guard.clone(),
                next_name,
                plan_subquery,
            )?;
            let plan = rewrite(
                plan,
                low,
                hidden_groups,
                aggregate_groups,
                guard.clone(),
                next_name,
                plan_subquery,
            )?;
            rewrite(
                plan,
                high,
                hidden_groups,
                aggregate_groups,
                guard,
                next_name,
                plan_subquery,
            )
        }
        Expr::InList { expr, list, .. } => {
            let mut plan = rewrite(
                plan,
                expr,
                hidden_groups,
                aggregate_groups,
                guard.clone(),
                next_name,
                plan_subquery,
            )?;
            for candidate in list {
                plan = rewrite(
                    plan,
                    candidate,
                    hidden_groups,
                    aggregate_groups,
                    guard.clone(),
                    next_name,
                    plan_subquery,
                )?;
            }
            Ok(plan)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut plan = plan;
            if let Some(operand) = operand {
                plan = rewrite(
                    plan,
                    operand,
                    hidden_groups,
                    aggregate_groups,
                    guard.clone(),
                    next_name,
                    plan_subquery,
                )?;
            }
            let operand = operand.as_deref().cloned();
            let mut prefix = guard;
            for branch in conditions {
                plan = rewrite(
                    plan,
                    &mut branch.condition,
                    hidden_groups,
                    aggregate_groups,
                    prefix.clone(),
                    next_name,
                    plan_subquery,
                )?;
                let selected = guarded::case_branch_selected(operand.as_ref(), &branch.condition);
                plan = rewrite(
                    plan,
                    &mut branch.result,
                    hidden_groups,
                    aggregate_groups,
                    guarded::and(prefix.clone(), Expr::IsTrue(Box::new(selected.clone()))),
                    next_name,
                    plan_subquery,
                )?;
                prefix = guarded::and(prefix, Expr::IsNotTrue(Box::new(selected)));
            }
            if let Some(else_result) = else_result {
                plan = rewrite(
                    plan,
                    else_result,
                    hidden_groups,
                    aggregate_groups,
                    prefix,
                    next_name,
                    plan_subquery,
                )?;
            }
            Ok(plan)
        }
        Expr::Function(function) => {
            let nullif = function.name.to_string().eq_ignore_ascii_case("nullif");
            let FunctionArguments::List(arguments) = &mut function.args else {
                return Ok(plan);
            };
            let mut plan = plan;
            let mut first: Option<Expr> = None;
            for (index, argument) in arguments.args.iter_mut().enumerate() {
                let argument = match argument {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    }
                    | FunctionArg::ExprNamed {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    } => expr,
                    _ => continue,
                };
                let argument_guard = if nullif && index == 1 {
                    first
                        .as_ref()
                        .map(|first| {
                            guarded::and(guard.clone(), Expr::IsNotNull(Box::new(first.clone())))
                        })
                        .unwrap_or_else(|| guard.clone())
                } else {
                    guard.clone()
                };
                plan = rewrite(
                    plan,
                    argument,
                    hidden_groups,
                    aggregate_groups,
                    argument_guard,
                    next_name,
                    plan_subquery,
                )?;
                if index == 0 {
                    first = Some(argument.clone());
                }
            }
            Ok(plan)
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut plan = rewrite(
                plan,
                expr,
                hidden_groups,
                aggregate_groups,
                guard.clone(),
                next_name,
                plan_subquery,
            )?;
            if let Some(from) = substring_from {
                plan = rewrite(
                    plan,
                    from,
                    hidden_groups,
                    aggregate_groups,
                    guard.clone(),
                    next_name,
                    plan_subquery,
                )?;
            }
            if let Some(length) = substring_for {
                plan = rewrite(
                    plan,
                    length,
                    hidden_groups,
                    aggregate_groups,
                    guard.clone(),
                    next_name,
                    plan_subquery,
                )?;
            }
            Ok(plan)
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            let mut plan = rewrite(
                plan,
                expr,
                hidden_groups,
                aggregate_groups,
                guard.clone(),
                next_name,
                plan_subquery,
            )?;
            if let Some(what) = trim_what {
                plan = rewrite(
                    plan,
                    what,
                    hidden_groups,
                    aggregate_groups,
                    guard.clone(),
                    next_name,
                    plan_subquery,
                )?;
            }
            if let Some(characters) = trim_characters {
                for character in characters {
                    plan = rewrite(
                        plan,
                        character,
                        hidden_groups,
                        aggregate_groups,
                        guard.clone(),
                        next_name,
                        plan_subquery,
                    )?;
                }
            }
            Ok(plan)
        }
        _ => Ok(plan),
    }
}

#[allow(clippy::too_many_arguments)]
fn attach_scalar<F, N>(
    plan: LogicalPlan,
    query: &Query,
    target: &mut Expr,
    hidden_groups: &mut Option<&mut Vec<Expr>>,
    aggregate_groups: Option<&[Expr]>,
    guard: Option<&Expr>,
    next_name: &mut N,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
    N: FnMut() -> String,
{
    let subquery = plan_subquery(query)?;
    let width = subquery.schema().arrow().fields().len();
    if width != 1 {
        return Err(Error::InvalidArgument(format!(
            "scalar subquery must return exactly one column, got {width}"
        )));
    }
    let name = unique_name(plan.schema(), next_name);
    let field = subquery.schema().arrow().field(0);
    let output = one_field_schema(&name, field.data_type().clone(), true);
    let guard = guarded::bind(guard, plan.schema(), aggregate_groups)?;
    let correlated = correlation::has_outer_refs(&subquery);
    let (subquery, delayed) = if guard.is_some() && (correlated || aggregate_groups.is_none()) {
        guarded::split_projection(subquery)
    } else {
        (subquery, None)
    };
    let joined = if let (Some(expression), Some(guard)) = (delayed, guard.clone()) {
        if correlated {
            let schema = PlanSchema::join(&plan.schema().clone(), &output);
            LogicalPlan::DependentJoin {
                left: Box::new(plan),
                right: Box::new(subquery),
                kind: DependentJoinKind::GuardedScalar { expression },
                guard: Some(guard),
                schema,
            }
        } else {
            let schema = PlanSchema::join(plan.schema(), &output);
            guarded::join(plan, subquery, expression, guard, schema)?
        }
    } else if correlated {
        let schema = PlanSchema::join(&plan.schema().clone(), &output);
        LogicalPlan::DependentJoin {
            left: Box::new(plan),
            right: Box::new(subquery),
            kind: DependentJoinKind::Scalar,
            guard,
            schema,
        }
    } else if guard.is_some() {
        let schema = PlanSchema::join(plan.schema(), &output);
        LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(subquery),
            on: Vec::new(),
            null_equal_keys: false,
            residual: guard,
            null_aware: None,
            join_type: JoinType::LeftSingle,
            schema,
        }
    } else {
        let scalar = LogicalPlan::Scalarize {
            input: Box::new(subquery),
            schema: output,
        };
        let schema = PlanSchema::join(plan.schema(), scalar.schema());
        LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(scalar),
            on: Vec::new(),
            null_equal_keys: false,
            residual: None,
            null_aware: None,
            join_type: JoinType::Inner,
            schema,
        }
    };
    replace_with_output(target, hidden_groups, name, false);
    Ok(joined)
}

#[allow(clippy::too_many_arguments)]
fn attach_exists<F, N>(
    plan: LogicalPlan,
    query: &Query,
    negated: bool,
    target: &mut Expr,
    hidden_groups: &mut Option<&mut Vec<Expr>>,
    aggregate_groups: Option<&[Expr]>,
    guard: Option<&Expr>,
    next_name: &mut N,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
    N: FnMut() -> String,
{
    let subquery = exists::erase_output(plan_subquery(query)?)?;
    let name = unique_name(plan.schema(), next_name);
    let marker = one_field_schema(&name, DataType::Boolean, false);
    let schema = PlanSchema::join(plan.schema(), &marker);
    let guard = guarded::bind(guard, plan.schema(), aggregate_groups)?;
    let joined = if correlation::has_outer_refs(&subquery) {
        LogicalPlan::DependentJoin {
            left: Box::new(plan),
            right: Box::new(subquery),
            kind: DependentJoinKind::Exists,
            guard,
            schema,
        }
    } else {
        let right_schema = subquery.schema().clone();
        let subquery = LogicalPlan::Limit {
            input: Box::new(subquery),
            offset: 0,
            limit: Some(1),
            schema: right_schema,
        };
        LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(subquery),
            on: Vec::new(),
            null_equal_keys: false,
            residual: guard,
            null_aware: None,
            join_type: JoinType::Mark,
            schema,
        }
    };
    replace_with_output(target, hidden_groups, name, negated);
    Ok(joined)
}

#[allow(clippy::too_many_arguments)]
fn attach_in<F, N>(
    plan: LogicalPlan,
    needle: &Expr,
    query: &Query,
    negated: bool,
    target: &mut Expr,
    hidden_groups: &mut Option<&mut Vec<Expr>>,
    aggregate_groups: Option<&[Expr]>,
    guard: Option<&Expr>,
    next_name: &mut N,
    plan_subquery: &mut F,
) -> Result<LogicalPlan>
where
    F: FnMut(&Query) -> Result<LogicalPlan>,
    N: FnMut() -> String,
{
    let needle = match bind_expr(needle, plan.schema()) {
        Ok(needle) => needle,
        Err(_) if super::aggregate::contains_aggregate(needle) => {
            let groups = aggregate_groups.ok_or_else(|| {
                Error::Internal("aggregate IN needle was found outside an aggregate query".into())
            })?;
            super::aggregate::bind_deferred_result(needle, plan.schema(), groups)?
        }
        Err(error) => return Err(error),
    };
    let subquery = plan_subquery(query)?;
    let width = subquery.schema().arrow().fields().len();
    if width != 1 {
        return Err(Error::InvalidArgument(format!(
            "IN subquery must return exactly one column, got {width}"
        )));
    }
    let right_field = subquery.schema().arrow().field(0);
    let equality = make_binary(
        needle,
        BinaryOp::Eq,
        BoundExpr::column(
            0,
            right_field.data_type().clone(),
            right_field.name().clone(),
        ),
    )?;
    let ExprKind::Binary { left, right, .. } = equality.kind else {
        unreachable!("equality binding creates a binary expression")
    };
    let left = *left;
    let right = *right;
    let name = unique_name(plan.schema(), next_name);
    let marker = one_field_schema(&name, DataType::Boolean, true);
    let schema = PlanSchema::join(plan.schema(), &marker);
    let guard = guarded::bind(guard, plan.schema(), aggregate_groups)?;
    let joined = if correlation::has_outer_refs(&subquery) {
        LogicalPlan::DependentJoin {
            left: Box::new(plan),
            right: Box::new(subquery),
            kind: DependentJoinKind::In { needle: left },
            guard,
            schema,
        }
    } else {
        LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(subquery),
            on: Vec::new(),
            null_equal_keys: false,
            residual: guard,
            null_aware: Some((left, right)),
            join_type: JoinType::Mark,
            schema,
        }
    };
    replace_with_output(target, hidden_groups, name, negated);
    Ok(joined)
}

fn replace_with_output(
    target: &mut Expr,
    hidden_groups: &mut Option<&mut Vec<Expr>>,
    name: String,
    negated: bool,
) {
    let column = Expr::Identifier(Ident::new(name));
    if let Some(groups) = hidden_groups.as_deref_mut() {
        groups.push(column.clone());
    }
    *target = if negated {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(column),
        }
    } else {
        column
    };
}

fn one_field_schema(name: &str, data_type: DataType, nullable: bool) -> PlanSchema {
    PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
        name, data_type, nullable,
    )])))
}

fn unique_name<N>(schema: &PlanSchema, next_name: &mut N) -> String
where
    N: FnMut() -> String,
{
    loop {
        let candidate = next_name();
        if !schema
            .arrow()
            .fields()
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(&candidate))
        {
            return candidate;
        }
    }
}

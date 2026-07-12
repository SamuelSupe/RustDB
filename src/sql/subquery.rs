use arrow::datatypes::DataType;
use sqlparser::ast::{BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::{Error, Result};

use super::binder::{bind_expr_scoped, ensure_boolean};
use super::coercion::cast_if_needed;
use super::{BoundExpr, DependentJoinKind, ExprKind, JoinType, LogicalPlan, PlanSchema, UnaryOp};

mod staging;

pub(super) use staging::{is_direct_mark_attachment, stageable_direct_mark_term};

pub(super) fn apply_where(
    mut plan: LogicalPlan,
    predicate: &Expr,
    outer: Option<&PlanSchema>,
) -> Result<LogicalPlan> {
    let mut terms = Vec::new();
    split_and(predicate, &mut terms);
    // Subquery extraction nests Mark/DependentJoin nodes in discovery order.
    // Apply generated marker predicates in reverse order so every direct
    // WHERE IN/EXISTS can be lowered without moving unrelated filters.
    terms.sort_by_key(|term| std::cmp::Reverse(marker_rank(term).unwrap_or(0)));
    for term in terms {
        plan = apply_filter(plan, term, outer)?;
    }
    Ok(plan)
}

fn apply_filter(
    plan: LogicalPlan,
    predicate: &Expr,
    outer: Option<&PlanSchema>,
) -> Result<LogicalPlan> {
    if contains_subquery(predicate) {
        return Err(Error::Internal(
            "subquery expression reached WHERE after extraction".into(),
        ));
    }
    let predicate = bind_expr_scoped(predicate, plan.schema(), outer)?;
    ensure_boolean(&predicate).map_err(|_| {
        Error::InvalidArgument(format!(
            "WHERE requires BOOLEAN, got {}",
            predicate.data_type
        ))
    })?;
    let predicate = cast_if_needed(predicate, &DataType::Boolean);
    if let Some(negated) = direct_marker_predicate(&predicate, &plan) {
        return lower_direct_marker(plan, negated);
    }
    let schema = plan.schema().clone();
    Ok(LogicalPlan::Filter {
        input: Box::new(plan),
        predicate,
        schema,
    })
}

fn direct_marker_predicate(predicate: &BoundExpr, input: &LogicalPlan) -> Option<bool> {
    let marker = match input {
        LogicalPlan::Join {
            left,
            join_type: JoinType::Mark,
            ..
        } => left.schema().arrow().fields().len(),
        LogicalPlan::DependentJoin {
            left,
            kind: DependentJoinKind::Exists | DependentJoinKind::In { .. },
            guard: None,
            ..
        } => left.schema().arrow().fields().len(),
        _ => return None,
    };
    match &predicate.kind {
        ExprKind::Column(index) if *index == marker => Some(false),
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } if matches!(expr.kind, ExprKind::Column(index) if index == marker) => Some(true),
        _ => None,
    }
}

fn lower_direct_marker(plan: LogicalPlan, negated: bool) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            mut on,
            residual,
            null_aware,
            join_type: JoinType::Mark,
            ..
        } => {
            let schema = left.schema().clone();
            let (join_type, null_aware) = match (negated, null_aware) {
                (false, Some(pair)) => {
                    on.push(pair);
                    (JoinType::Semi, None)
                }
                (false, None) => (JoinType::Semi, None),
                (true, Some(pair)) => (JoinType::NullAwareAnti, Some(pair)),
                (true, None) => (JoinType::Anti, None),
            };
            Ok(LogicalPlan::Join {
                left,
                right,
                on,
                null_equal_keys: false,
                residual,
                null_aware,
                join_type,
                schema,
            })
        }
        LogicalPlan::DependentJoin {
            left,
            right,
            kind,
            guard: None,
            ..
        } => {
            let schema = left.schema().clone();
            let kind = match kind {
                DependentJoinKind::Exists => DependentJoinKind::ExistsFilter { negated },
                DependentJoinKind::In { needle } => DependentJoinKind::InFilter { needle, negated },
                _ => {
                    return Err(Error::Internal(
                        "direct marker lowering received a non-marker dependent join".into(),
                    ));
                }
            };
            Ok(LogicalPlan::DependentJoin {
                left,
                right,
                kind,
                guard: None,
                schema,
            })
        }
        _ => Err(Error::Internal(
            "direct marker lowering received a non-Mark join".into(),
        )),
    }
}

fn split_and<'a>(expr: &'a Expr, output: &mut Vec<&'a Expr>) {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        split_and(left, output);
        split_and(right, output);
    } else {
        output.push(expr);
    }
}

fn marker_rank(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Identifier(ident) => ident
            .value
            .strip_prefix("__rustdb_scalar_subquery_")
            .and_then(|index| index.parse().ok())
            .map(|index: usize| index.saturating_add(1)),
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => marker_rank(expr),
        _ => None,
    }
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
        Expr::Between {
            expr, low, high, ..
        } => contains_subquery(expr) || contains_subquery(low) || contains_subquery(high),
        Expr::InList { expr, list, .. } => {
            contains_subquery(expr) || list.iter().any(contains_subquery)
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
        | Expr::Floor { expr, .. } => contains_subquery(expr),
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
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(arguments) => arguments.args.iter().any(|argument| {
                matches!(
                    argument,
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        }
                        if contains_subquery(expr)
                )
            }),
            FunctionArguments::Subquery(_) => true,
            FunctionArguments::None => false,
        },
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            contains_subquery(expr)
                || substring_from.as_deref().is_some_and(contains_subquery)
                || substring_for.as_deref().is_some_and(contains_subquery)
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            contains_subquery(expr)
                || trim_what.as_deref().is_some_and(contains_subquery)
                || trim_characters
                    .as_deref()
                    .is_some_and(|values| values.iter().any(contains_subquery))
        }
        _ => false,
    }
}

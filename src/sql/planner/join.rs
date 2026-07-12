use sqlparser::ast::{BinaryOperator, Expr, JoinConstraint};

use crate::{Error, Result};

use super::super::{
    BinaryOp, BoundExpr, ExprKind, PlanSchema,
    binder::{bind_expr, ensure_boolean},
};

pub(super) struct JoinBinding {
    pub(super) keys: Vec<(BoundExpr, BoundExpr)>,
    pub(super) right_filters: Vec<BoundExpr>,
}

pub(super) fn bind_join_constraint(
    constraint: &JoinConstraint,
    left: &PlanSchema,
    right: &PlanSchema,
) -> Result<JoinBinding> {
    match constraint {
        JoinConstraint::On(expr) => {
            let combined = PlanSchema::join(left, right);
            let mut binding = JoinBinding {
                keys: Vec::new(),
                right_filters: Vec::new(),
            };
            bind_join_on(
                expr,
                &combined,
                right,
                left.arrow().fields().len(),
                &mut binding,
            )?;
            Ok(binding)
        }
        JoinConstraint::Using(_) => Err(Error::Unsupported(
            "JOIN ... USING is not supported because v0.1 does not yet coalesce its output key; use JOIN ... ON".into(),
        )),
        _ => Err(Error::Unsupported(
            "only JOIN ... ON equality expressions and JOIN ... USING are supported".into(),
        )),
    }
}

fn bind_join_on(
    expr: &Expr,
    schema: &PlanSchema,
    right_schema: &PlanSchema,
    left_width: usize,
    binding: &mut JoinBinding,
) -> Result<()> {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        bind_join_on(left, schema, right_schema, left_width, binding)?;
        return bind_join_on(right, schema, right_schema, left_width, binding);
    }
    let bound = bind_expr(expr, schema)?;
    if let ExprKind::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = bound.kind
        && let (Ok(left_side), Ok(right_side)) = (
            expression_side(&left, left_width),
            expression_side(&right, left_width),
        )
    {
        match (left_side, right_side) {
            (JoinExprSide::Left, JoinExprSide::Right) => {
                binding
                    .keys
                    .push((*left, rebase_right(*right, left_width)?));
                return Ok(());
            }
            (JoinExprSide::Right, JoinExprSide::Left) => {
                binding
                    .keys
                    .push((*right, rebase_right(*left, left_width)?));
                return Ok(());
            }
            _ => {}
        }
    }
    let predicate = bind_expr(expr, right_schema).map_err(|_| {
        Error::Unsupported(format!(
            "join condition `{expr}` must be an equality key or a right-only predicate"
        ))
    })?;
    ensure_boolean(&predicate).map_err(|_| {
        Error::InvalidArgument(format!(
            "join predicate `{expr}` requires BOOLEAN, got {}",
            predicate.data_type
        ))
    })?;
    binding.right_filters.push(predicate);
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum JoinExprSide {
    Left,
    Right,
}

fn expression_side(expr: &BoundExpr, left_width: usize) -> Result<JoinExprSide> {
    let mut columns = Vec::new();
    expr.referenced_columns(&mut columns);
    if columns.is_empty() {
        return Err(Error::Unsupported(
            "constant join keys are not supported".into(),
        ));
    }
    if columns.iter().all(|index| *index < left_width) {
        Ok(JoinExprSide::Left)
    } else if columns.iter().all(|index| *index >= left_width) {
        Ok(JoinExprSide::Right)
    } else {
        Err(Error::Unsupported(
            "a join key cannot reference both inputs".into(),
        ))
    }
}

fn rebase_right(mut expr: BoundExpr, left_width: usize) -> Result<BoundExpr> {
    match &mut expr.kind {
        ExprKind::Column(index) => {
            *index = index.checked_sub(left_width).ok_or_else(|| {
                Error::Internal("right join expression used a left column".into())
            })?;
        }
        ExprKind::OuterRef { .. } => {
            return Err(Error::Internal(
                "OuterRef reached parser-visible JOIN rebasing".into(),
            ));
        }
        ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => {
            return Err(Error::Internal(
                "deferred aggregate result reached parser-visible JOIN rebasing".into(),
            ));
        }
        ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            **left = rebase_right((**left).clone(), left_width)?;
            **right = rebase_right((**right).clone(), left_width)?;
        }
        ExprKind::Like { expr, pattern, .. } => {
            **expr = rebase_right((**expr).clone(), left_width)?;
            **pattern = rebase_right((**pattern).clone(), left_width)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                *when = rebase_right(when.clone(), left_width)?;
                *then = rebase_right(then.clone(), left_width)?;
            }
            **else_expr = rebase_right((**else_expr).clone(), left_width)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            **expr = rebase_right((**expr).clone(), left_width)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for arg in args {
                *arg = rebase_right(arg.clone(), left_width)?;
            }
        }
    }
    Ok(expr)
}

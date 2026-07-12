use crate::sql::{BinaryOp, BoundExpr, ExprKind};
use crate::{Error, Result};

pub(super) fn remap_columns(expr: &mut BoundExpr, mapping: &[usize]) -> Result<()> {
    rewrite(expr, &mut |index| {
        mapping
            .get(index)
            .copied()
            .filter(|mapped| *mapped != usize::MAX)
            .ok_or_else(|| {
                Error::Internal(format!(
                    "decorrelation column {index} is outside a {}-column input",
                    mapping.len()
                ))
            })
    })
}

pub(super) fn shift_columns(expr: &mut BoundExpr, offset: usize) -> Result<()> {
    rewrite(expr, &mut |index| {
        index
            .checked_add(offset)
            .ok_or_else(|| Error::Internal("decorrelation column offset overflow".into()))
    })
}

pub(super) fn outer_to_domain_columns(
    expr: &mut BoundExpr,
    parameters: &[(usize, usize)],
) -> Result<()> {
    match &mut expr.kind {
        ExprKind::OuterRef { depth: 1, index } => {
            let position = parameters
                .iter()
                .find_map(|(outer, position)| (*outer == *index).then_some(*position))
                .ok_or_else(|| {
                    Error::Internal(format!(
                        "outer column {index} is missing from the correlation domain"
                    ))
                })?;
            expr.kind = ExprKind::Column(position);
        }
        ExprKind::OuterRef { depth, .. } => {
            return Err(Error::Unsupported(format!(
                "correlated subquery depth {depth} is not supported; maximum depth is 1"
            )));
        }
        ExprKind::Column(_)
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            outer_to_domain_columns(left, parameters)?;
            outer_to_domain_columns(right, parameters)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            outer_to_domain_columns(expr, parameters)?
        }
        ExprKind::Like { expr, pattern, .. } => {
            outer_to_domain_columns(expr, parameters)?;
            outer_to_domain_columns(pattern, parameters)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                outer_to_domain_columns(when, parameters)?;
                outer_to_domain_columns(then, parameters)?;
            }
            outer_to_domain_columns(else_expr, parameters)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for arg in args {
                outer_to_domain_columns(arg, parameters)?;
            }
        }
    }
    Ok(())
}

pub(super) fn residual_to_domain_join(
    expr: &mut BoundExpr,
    domain_width: usize,
    parameters: &[(usize, usize)],
) -> Result<()> {
    shift_columns(expr, domain_width)?;
    outer_to_domain_columns(expr, parameters)
}

pub(super) fn outer_to_columns(expr: &mut BoundExpr) -> Result<()> {
    match &mut expr.kind {
        ExprKind::OuterRef { depth: 1, index } => {
            expr.kind = ExprKind::Column(*index);
        }
        ExprKind::OuterRef { depth, .. } => {
            return Err(Error::Unsupported(format!(
                "correlated subquery depth {depth} is not supported; maximum depth is 1"
            )));
        }
        ExprKind::Column(_)
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            outer_to_columns(left)?;
            outer_to_columns(right)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            outer_to_columns(expr)?
        }
        ExprKind::Like { expr, pattern, .. } => {
            outer_to_columns(expr)?;
            outer_to_columns(pattern)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                outer_to_columns(when)?;
                outer_to_columns(then)?;
            }
            outer_to_columns(else_expr)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for arg in args {
                outer_to_columns(arg)?;
            }
        }
    }
    Ok(())
}

pub(super) fn right_residual_to_join(expr: &mut BoundExpr, left_width: usize) -> Result<()> {
    match &mut expr.kind {
        ExprKind::Column(index) => {
            *index = index.checked_add(left_width).ok_or_else(|| {
                Error::Internal("decorrelation residual column offset overflow".into())
            })?;
        }
        ExprKind::OuterRef { depth: 1, index } => {
            expr.kind = ExprKind::Column(*index);
        }
        ExprKind::OuterRef { depth, .. } => {
            return Err(Error::Unsupported(format!(
                "correlated subquery depth {depth} is not supported; maximum depth is 1"
            )));
        }
        ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) | ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            right_residual_to_join(left, left_width)?;
            right_residual_to_join(right, left_width)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            right_residual_to_join(expr, left_width)?
        }
        ExprKind::Like { expr, pattern, .. } => {
            right_residual_to_join(expr, left_width)?;
            right_residual_to_join(pattern, left_width)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                right_residual_to_join(when, left_width)?;
                right_residual_to_join(then, left_width)?;
            }
            right_residual_to_join(else_expr, left_width)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for arg in args {
                right_residual_to_join(arg, left_width)?;
            }
        }
    }
    Ok(())
}

pub(super) fn combine_and(mut terms: Vec<BoundExpr>) -> Result<Option<BoundExpr>> {
    if terms.iter().any(|term| {
        matches!(
            &term.kind,
            ExprKind::Literal(crate::sql::ScalarValue::Boolean(false))
        )
    }) {
        return Ok(Some(BoundExpr::literal(crate::sql::ScalarValue::Boolean(
            false,
        ))));
    }
    let Some(mut output) = terms.pop() else {
        return Ok(None);
    };
    while let Some(term) = terms.pop() {
        let display_name = format!("{} AND {}", term.display_name, output.display_name);
        output = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(term),
                op: BinaryOp::And,
                right: Box::new(output),
            },
            data_type: arrow::datatypes::DataType::Boolean,
            display_name,
        };
    }
    Ok(Some(output))
}

pub(super) fn split_and(expr: BoundExpr, output: &mut Vec<BoundExpr>) {
    if let ExprKind::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = expr.kind
    {
        split_and(*left, output);
        split_and(*right, output);
    } else {
        output.push(expr);
    }
}

fn rewrite<F>(expr: &mut BoundExpr, map: &mut F) -> Result<()>
where
    F: FnMut(usize) -> Result<usize>,
{
    match &mut expr.kind {
        ExprKind::Column(index) => *index = map(*index)?,
        ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            rewrite(left, map)?;
            rewrite(right, map)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            rewrite(expr, map)?
        }
        ExprKind::Like { expr, pattern, .. } => {
            rewrite(expr, map)?;
            rewrite(pattern, map)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                rewrite(when, map)?;
                rewrite(then, map)?;
            }
            rewrite(else_expr, map)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for arg in args {
                rewrite(arg, map)?;
            }
        }
    }
    Ok(())
}

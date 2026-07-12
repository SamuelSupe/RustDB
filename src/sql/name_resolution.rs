use sqlparser::{
    ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, Spanned, UnaryOperator,
        Value,
    },
    tokenizer::Span,
};

use crate::{Error, Result};

use super::PlanSchema;

pub(super) fn resolve_group_by(
    groups: &mut [Expr],
    projection: &[SelectItem],
    input: &PlanSchema,
) -> Result<()> {
    for group in groups {
        let span = group.span();
        if let Some(position) = ordinal(group, "GROUP BY")? {
            *group = projection_expr(projection, position, "GROUP BY", span)?.clone();
            continue;
        }
        let Expr::Identifier(ident) = group else {
            continue;
        };
        if input
            .arrow()
            .fields()
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(&ident.value))
        {
            continue;
        }
        if let Some(alias) = projection_alias(projection, &ident.value, "GROUP BY", span)? {
            *group = alias.clone();
        }
    }
    Ok(())
}

pub(super) fn rewrite_projection_aliases(
    expr: &mut Expr,
    projection: &[SelectItem],
    clause: &str,
) -> Result<()> {
    let span = expr.span();
    if let Expr::Identifier(ident) = expr
        && let Some(alias) = projection_alias(projection, &ident.value, clause, span)?
    {
        *expr = alias.clone();
        return Ok(());
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
            rewrite_projection_aliases(left, projection, clause)?;
            rewrite_projection_aliases(right, projection, clause)
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
        | Expr::Cast { expr, .. } => rewrite_projection_aliases(expr, projection, clause),
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_projection_aliases(expr, projection, clause)?;
            rewrite_projection_aliases(low, projection, clause)?;
            rewrite_projection_aliases(high, projection, clause)
        }
        Expr::InList { expr, list, .. } => {
            rewrite_projection_aliases(expr, projection, clause)?;
            for candidate in list {
                rewrite_projection_aliases(candidate, projection, clause)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                rewrite_projection_aliases(operand, projection, clause)?;
            }
            for branch in conditions {
                rewrite_projection_aliases(&mut branch.condition, projection, clause)?;
                rewrite_projection_aliases(&mut branch.result, projection, clause)?;
            }
            if let Some(else_result) = else_result {
                rewrite_projection_aliases(else_result, projection, clause)?;
            }
            Ok(())
        }
        Expr::Function(function) => {
            let FunctionArguments::List(arguments) = &mut function.args else {
                return Ok(());
            };
            for argument in &mut arguments.args {
                let expression = match argument {
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
                rewrite_projection_aliases(expression, projection, clause)?;
            }
            Ok(())
        }
        Expr::Extract { expr, .. } | Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } => {
            rewrite_projection_aliases(expr, projection, clause)
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            rewrite_projection_aliases(expr, projection, clause)?;
            if let Some(value) = substring_from {
                rewrite_projection_aliases(value, projection, clause)?;
            }
            if let Some(value) = substring_for {
                rewrite_projection_aliases(value, projection, clause)?;
            }
            Ok(())
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            rewrite_projection_aliases(expr, projection, clause)?;
            if let Some(value) = trim_what {
                rewrite_projection_aliases(value, projection, clause)?;
            }
            if let Some(values) = trim_characters {
                for value in values {
                    rewrite_projection_aliases(value, projection, clause)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) fn ordinal(expr: &Expr, clause: &str) -> Result<Option<usize>> {
    let span = expr.span();
    let Expr::Value(value) = expr else {
        if let Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } = expr
            && is_integer_literal(expr)
        {
            return Err(Error::InvalidArgument(format!(
                "{clause} position must be at least 1{}",
                source_location_suffix(span)
            )));
        }
        return Ok(None);
    };
    let Value::Number(number, _) = &value.value else {
        return Ok(None);
    };
    if !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(None);
    }
    let position = number.parse::<usize>().map_err(|_| {
        Error::InvalidArgument(format!(
            "{clause} position '{number}' is out of range{}",
            source_location_suffix(span)
        ))
    })?;
    if position == 0 {
        return Err(Error::InvalidArgument(format!(
            "{clause} position must be at least 1{}",
            source_location_suffix(span)
        )));
    }
    Ok(Some(position))
}

pub(super) fn explicit_projection_index(
    projection: &[SelectItem],
    target: &Expr,
    _clause: &str,
) -> Result<Option<usize>> {
    let mut found = None;
    for (index, item) in projection.iter().enumerate() {
        let expression = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return Ok(None),
        };
        if expression != target {
            continue;
        }
        // DuckDB resolves duplicate output expressions to the final select
        // item. Identical expressions have identical values, while using the
        // same rule as aliases keeps query-block resolution predictable.
        found = Some(index);
    }
    Ok(found)
}

fn projection_expr<'a>(
    projection: &'a [SelectItem],
    position: usize,
    clause: &str,
    span: Span,
) -> Result<&'a Expr> {
    let item = projection.get(position - 1).ok_or_else(|| {
        Error::InvalidArgument(format!(
            "{clause} position {position} is out of range (select list has {} items){}",
            projection.len(),
            source_location_suffix(span)
        ))
    })?;
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => Ok(expr),
        _ => Err(Error::Unsupported(format!(
            "{clause} ordinal references to wildcard select items are not supported{}",
            source_location_suffix(span)
        ))),
    }
}

fn projection_alias<'a>(
    projection: &'a [SelectItem],
    name: &str,
    _clause: &str,
    _span: Span,
) -> Result<Option<&'a Expr>> {
    let mut found = None;
    for item in projection {
        let SelectItem::ExprWithAlias { expr, alias } = item else {
            continue;
        };
        if !alias.value.eq_ignore_ascii_case(name) {
            continue;
        }
        // DuckDB resolves duplicate aliases to the final select item.
        found = Some(expr);
    }
    Ok(found)
}

pub(super) fn source_location_suffix(span: Span) -> String {
    if span == Span::empty() {
        String::new()
    } else {
        format!(" at line {}, column {}", span.start.line, span.start.column)
    }
}

fn is_integer_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(value)
            if matches!(&value.value, Value::Number(number, _) if number.bytes().all(|byte| byte.is_ascii_digit()))
    )
}

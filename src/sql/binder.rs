use arrow::datatypes::DataType;
use sqlparser::ast::{BinaryOperator, CastKind, Expr, Ident, UnaryOperator};

use crate::{Error, Result};

use super::{BinaryOp, BoundExpr, ExprKind, PlanSchema, ScalarValue, UnaryOp};
use super::{
    coercion::{
        cast, cast_if_needed, coerce_arithmetic, coerce_comparison, common_case_type, is_numeric,
        is_string,
    },
    literal::{bind_interval, bind_typed_string, bind_value, string_value},
};

pub(super) fn bind_expr(expr: &Expr, schema: &PlanSchema) -> Result<BoundExpr> {
    match expr {
        Expr::Identifier(ident) => bind_column(None, ident, schema),
        Expr::CompoundIdentifier(idents) if idents.len() >= 2 => bind_column(
            Some(&idents[idents.len() - 2].value),
            &idents[idents.len() - 1],
            schema,
        ),
        Expr::Value(value) => bind_value(&value.value),
        Expr::TypedString(value) => bind_typed_string(value),
        Expr::Interval(interval) => bind_interval(interval),
        Expr::Nested(expr) => bind_expr(expr, schema),
        Expr::BinaryOp { left, op, right } => make_binary(
            bind_expr(left, schema)?,
            map_binary(op.clone())?,
            bind_expr(right, schema)?,
        ),
        Expr::UnaryOp { op, expr } => {
            let expr = bind_expr(expr, schema)?;
            match op {
                UnaryOperator::Plus => Ok(expr),
                UnaryOperator::Minus => make_unary(UnaryOp::Negate, expr),
                UnaryOperator::Not => make_unary(UnaryOp::Not, expr),
                other => Err(Error::Unsupported(format!(
                    "unary operator {other} is not supported"
                ))),
            }
        }
        Expr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => {
            if *array || format.is_some() {
                return Err(Error::Unsupported(
                    "CAST ARRAY and CAST FORMAT are not supported".into(),
                ));
            }
            if !matches!(kind, CastKind::Cast | CastKind::DoubleColon) {
                return Err(Error::Unsupported(
                    "TRY_CAST and SAFE_CAST are not supported".into(),
                ));
            }
            cast(bind_expr(expr, schema)?, data_type)
        }
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                return Err(Error::Unsupported("LIKE ANY is not supported".into()));
            }
            let escape = escape_char
                .as_ref()
                .map(|value| parse_escape(&value.value))
                .transpose()?;
            make_like(
                bind_expr(expr, schema)?,
                bind_expr(pattern, schema)?,
                *negated,
                escape,
            )
        }
        Expr::ILike { .. } => Err(Error::Unsupported(
            "ILIKE is not supported; use LIKE for case-sensitive matching".into(),
        )),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let operand = operand
                .as_ref()
                .map(|expr| bind_expr(expr, schema))
                .transpose()?;
            let when_then = conditions
                .iter()
                .map(|branch| {
                    Ok((
                        bind_expr(&branch.condition, schema)?,
                        bind_expr(&branch.result, schema)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_expr = else_result
                .as_ref()
                .map(|expr| bind_expr(expr, schema))
                .transpose()?;
            make_case(operand, when_then, else_expr)
        }
        Expr::IsNull(expr) => make_is_null(bind_expr(expr, schema)?, false),
        Expr::IsNotNull(expr) => make_is_null(bind_expr(expr, schema)?, true),
        Expr::IsTrue(expr) => make_is_truth(bind_expr(expr, schema)?, TruthValue::True, false),
        Expr::IsNotTrue(expr) => make_is_truth(bind_expr(expr, schema)?, TruthValue::True, true),
        Expr::IsFalse(expr) => make_is_truth(bind_expr(expr, schema)?, TruthValue::False, false),
        Expr::IsNotFalse(expr) => make_is_truth(bind_expr(expr, schema)?, TruthValue::False, true),
        Expr::IsUnknown(expr) => {
            make_is_truth(bind_expr(expr, schema)?, TruthValue::Unknown, false)
        }
        Expr::IsNotUnknown(expr) => {
            make_is_truth(bind_expr(expr, schema)?, TruthValue::Unknown, true)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => bind_in_list(expr, list, *negated, schema),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => bind_between(expr, low, high, *negated, schema),
        Expr::Function(function) if function.over.is_some() => Err(Error::Unsupported(
            "window functions are not supported".into(),
        )),
        Expr::Function(function) => Err(Error::Unsupported(format!(
            "scalar function {} is not supported",
            function.name
        ))),
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. } => Err(Error::Unsupported(
            "subquery expressions, including correlated subqueries, are not supported".into(),
        )),
        other => Err(Error::Unsupported(format!(
            "expression `{other}` is not supported"
        ))),
    }
}

fn bind_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &PlanSchema,
) -> Result<BoundExpr> {
    if list.is_empty() {
        return Ok(BoundExpr::literal(ScalarValue::Boolean(negated)));
    }
    let needle = bind_expr(expr, schema)?;
    let comparison = if negated {
        BinaryOp::NotEq
    } else {
        BinaryOp::Eq
    };
    let connective = if negated { BinaryOp::And } else { BinaryOp::Or };
    let mut comparisons = list
        .iter()
        .map(|candidate| make_binary(needle.clone(), comparison, bind_expr(candidate, schema)?));
    let mut output = comparisons
        .next()
        .expect("non-empty IN list established above")?;
    for comparison in comparisons {
        output = make_binary(output, connective, comparison?)?;
    }
    Ok(output)
}

fn bind_between(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    schema: &PlanSchema,
) -> Result<BoundExpr> {
    let value = bind_expr(expr, schema)?;
    let lower = make_binary(value.clone(), BinaryOp::GtEq, bind_expr(low, schema)?)?;
    let upper = make_binary(value, BinaryOp::LtEq, bind_expr(high, schema)?)?;
    let between = make_binary(lower, BinaryOp::And, upper)?;
    if negated {
        make_unary(UnaryOp::Not, between)
    } else {
        Ok(between)
    }
}

pub(super) fn bind_column(
    qualifier: Option<&str>,
    ident: &Ident,
    schema: &PlanSchema,
) -> Result<BoundExpr> {
    let mut matches = Vec::new();
    for (index, field) in schema.arrow().fields().iter().enumerate() {
        if !field.name().eq_ignore_ascii_case(&ident.value) {
            continue;
        }
        if let Some(qualifier) = qualifier
            && !schema
                .qualifier(index)
                .is_some_and(|value| value.eq_ignore_ascii_case(qualifier))
        {
            continue;
        }
        matches.push(index);
    }

    match matches.as_slice() {
        [index] => {
            let field = schema.arrow().field(*index);
            Ok(BoundExpr::column(
                *index,
                field.data_type().clone(),
                ident.value.clone(),
            ))
        }
        [] => Err(Error::Catalog(format!(
            "column {}{} does not exist",
            qualifier
                .map(|value| format!("{value}."))
                .unwrap_or_default(),
            ident.value
        ))),
        _ => Err(Error::Catalog(format!(
            "column '{}' is ambiguous",
            ident.value
        ))),
    }
}

pub(super) fn make_binary(left: BoundExpr, op: BinaryOp, right: BoundExpr) -> Result<BoundExpr> {
    let (left, right, data_type) = match op {
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => {
            let (left, right) = coerce_comparison(left, right)?;
            (left, right, DataType::Boolean)
        }
        BinaryOp::And | BinaryOp::Or => {
            ensure_boolean(&left)?;
            ensure_boolean(&right)?;
            (
                cast_if_needed(left, &DataType::Boolean),
                cast_if_needed(right, &DataType::Boolean),
                DataType::Boolean,
            )
        }
        BinaryOp::Add
        | BinaryOp::Subtract
        | BinaryOp::Multiply
        | BinaryOp::Divide
        | BinaryOp::Modulo => coerce_arithmetic(left, right, op)?,
    };
    let display_name = format!("{} {op} {}", left.display_name, right.display_name);
    Ok(BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type,
        display_name,
    })
}

pub(super) fn make_unary(op: UnaryOp, expr: BoundExpr) -> Result<BoundExpr> {
    let expr = match op {
        UnaryOp::Not => {
            ensure_boolean(&expr)?;
            cast_if_needed(expr, &DataType::Boolean)
        }
        UnaryOp::Negate if super::coercion::is_numeric(&expr.data_type) => expr,
        UnaryOp::Negate => {
            return Err(Error::Unsupported(format!(
                "cannot negate {}",
                expr.data_type
            )));
        }
    };
    let data_type = expr.data_type.clone();
    let display_name = match op {
        UnaryOp::Not => format!("NOT {}", expr.display_name),
        UnaryOp::Negate => format!("-{}", expr.display_name),
    };
    Ok(BoundExpr {
        kind: ExprKind::Unary {
            op,
            expr: Box::new(expr),
        },
        data_type,
        display_name,
    })
}

pub(super) fn make_is_null(expr: BoundExpr, negated: bool) -> Result<BoundExpr> {
    let display_name = format!(
        "{} IS {}NULL",
        expr.display_name,
        if negated { "NOT " } else { "" }
    );
    Ok(BoundExpr {
        kind: ExprKind::IsNull {
            expr: Box::new(expr),
            negated,
        },
        data_type: DataType::Boolean,
        display_name,
    })
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum TruthValue {
    True,
    False,
    Unknown,
}

pub(super) fn make_is_truth(
    expr: BoundExpr,
    truth: TruthValue,
    negated: bool,
) -> Result<BoundExpr> {
    if truth == TruthValue::Unknown {
        let original_name = expr.display_name.clone();
        let mut output = make_is_null(expr, negated)?;
        output.display_name = truth_display(&original_name, truth, negated);
        return Ok(output);
    }
    if !matches!(expr.data_type, DataType::Boolean | DataType::Null)
        && !is_numeric(&expr.data_type)
        && !is_string(&expr.data_type)
    {
        return Err(Error::InvalidArgument(format!(
            "IS TRUE/FALSE does not support {}",
            expr.data_type
        )));
    }
    let original_name = expr.display_name.clone();
    let expr = cast_if_needed(expr, &DataType::Boolean);
    let expected = BoundExpr::literal(ScalarValue::Boolean(truth == TruthValue::True));
    let null_test = make_is_null(expr.clone(), !negated)?;
    let comparison = make_binary(
        expr,
        if negated {
            BinaryOp::NotEq
        } else {
            BinaryOp::Eq
        },
        expected,
    )?;
    let mut output = make_binary(
        null_test,
        if negated { BinaryOp::Or } else { BinaryOp::And },
        comparison,
    )?;
    output.display_name = truth_display(&original_name, truth, negated);
    Ok(output)
}

fn truth_display(expr_name: &str, truth: TruthValue, negated: bool) -> String {
    let truth = match truth {
        TruthValue::True => "TRUE",
        TruthValue::False => "FALSE",
        TruthValue::Unknown => "UNKNOWN",
    };
    format!(
        "{expr_name} IS {}{truth}",
        if negated { "NOT " } else { "" }
    )
}

pub(super) fn make_like(
    expr: BoundExpr,
    pattern: BoundExpr,
    negated: bool,
    escape: Option<char>,
) -> Result<BoundExpr> {
    if !matches!(expr.data_type, DataType::Null) && !is_string(&expr.data_type) {
        return Err(Error::InvalidArgument(format!(
            "LIKE requires a string input, got {}",
            expr.data_type
        )));
    }
    if !matches!(pattern.data_type, DataType::Null) && !is_string(&pattern.data_type) {
        return Err(Error::InvalidArgument(format!(
            "LIKE requires a string pattern, got {}",
            pattern.data_type
        )));
    }
    let expr = cast_if_needed(expr, &DataType::Utf8);
    let pattern = cast_if_needed(pattern, &DataType::Utf8);
    let display_name = format!(
        "{} {}LIKE {}",
        expr.display_name,
        if negated { "NOT " } else { "" },
        pattern.display_name
    );
    Ok(BoundExpr {
        kind: ExprKind::Like {
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            negated,
            escape,
        },
        data_type: DataType::Boolean,
        display_name,
    })
}

pub(super) fn make_case(
    operand: Option<BoundExpr>,
    when_then: Vec<(BoundExpr, BoundExpr)>,
    else_expr: Option<BoundExpr>,
) -> Result<BoundExpr> {
    if when_then.is_empty() {
        return Err(Error::InvalidArgument(
            "CASE requires at least one WHEN branch".into(),
        ));
    }
    let mut branches = Vec::with_capacity(when_then.len());
    for (condition, result) in when_then {
        let condition = if let Some(operand) = &operand {
            make_binary(operand.clone(), BinaryOp::Eq, condition)?
        } else {
            ensure_boolean(&condition)?;
            cast_if_needed(condition, &DataType::Boolean)
        };
        branches.push((condition, result));
    }
    let else_expr = else_expr.unwrap_or_else(|| BoundExpr::literal(ScalarValue::Null));
    let mut data_type = else_expr.data_type.clone();
    for (_, result) in &branches {
        data_type = common_case_type(&data_type, &result.data_type)?;
    }
    let branches = branches
        .into_iter()
        .map(|(condition, result)| (condition, cast_if_needed(result, &data_type)))
        .collect();
    Ok(BoundExpr {
        kind: ExprKind::Case {
            when_then: branches,
            else_expr: Box::new(cast_if_needed(else_expr, &data_type)),
        },
        data_type,
        display_name: "CASE".into(),
    })
}

pub(super) fn ensure_boolean(expr: &BoundExpr) -> Result<()> {
    if matches!(expr.data_type, DataType::Boolean | DataType::Null) {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "expected BOOLEAN expression, got {}",
            expr.data_type
        )))
    }
}

pub(super) fn parse_escape(value: &sqlparser::ast::Value) -> Result<char> {
    let value = string_value(value)
        .ok_or_else(|| Error::InvalidArgument("LIKE ESCAPE must be a string literal".into()))?;
    let mut characters = value.chars();
    let escape = characters.next().ok_or_else(|| {
        Error::InvalidArgument("LIKE ESCAPE must contain exactly one character".into())
    })?;
    if characters.next().is_some() {
        return Err(Error::InvalidArgument(
            "LIKE ESCAPE must contain exactly one character".into(),
        ));
    }
    Ok(escape)
}

pub(super) fn map_binary(op: BinaryOperator) -> Result<BinaryOp> {
    match op {
        BinaryOperator::Eq => Ok(BinaryOp::Eq),
        BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
        BinaryOperator::Lt => Ok(BinaryOp::Lt),
        BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
        BinaryOperator::Gt => Ok(BinaryOp::Gt),
        BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
        BinaryOperator::And => Ok(BinaryOp::And),
        BinaryOperator::Or => Ok(BinaryOp::Or),
        BinaryOperator::Plus => Ok(BinaryOp::Add),
        BinaryOperator::Minus => Ok(BinaryOp::Subtract),
        BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
        BinaryOperator::Divide => Ok(BinaryOp::Divide),
        BinaryOperator::Modulo => Ok(BinaryOp::Modulo),
        other => Err(Error::Unsupported(format!(
            "binary operator {other} is not supported"
        ))),
    }
}

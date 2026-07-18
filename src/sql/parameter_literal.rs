use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use sqlparser::ast::{DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::{Error, Result};

use super::{BoundExpr, ExprKind, ScalarValue, coercion::cast_if_needed};

const VALUE_FUNCTION: &str = "__rustdb_internal_parameter_timestamp";
const NULL_FUNCTION: &str = "__rustdb_internal_parameter_timestamp_null";

pub(super) fn bind_with<F>(expression: &Expr, bind: &mut F) -> Option<Result<BoundExpr>>
where
    F: FnMut(&Expr) -> Result<BoundExpr>,
{
    let Expr::Function(function) = expression else {
        return None;
    };
    let name = function.name.to_string().to_ascii_lowercase();
    if !matches!(name.as_str(), VALUE_FUNCTION | NULL_FUNCTION) {
        return None;
    }
    Some((|| {
        if function.over.is_some()
            || function.filter.is_some()
            || function.null_treatment.is_some()
            || !function.within_group.is_empty()
            || !matches!(function.parameters, FunctionArguments::None)
        {
            return Err(Error::InvalidArgument(
                "internal timestamp parameter has invalid modifiers".into(),
            ));
        }
        let FunctionArguments::List(arguments) = &function.args else {
            return Err(Error::InvalidArgument(
                "internal timestamp parameter requires arguments".into(),
            ));
        };
        if arguments.duplicate_treatment == Some(DuplicateTreatment::Distinct)
            || !arguments.clauses.is_empty()
        {
            return Err(Error::InvalidArgument(
                "internal timestamp parameter has invalid argument clauses".into(),
            ));
        }
        let args = arguments
            .args
            .iter()
            .map(|argument| match argument {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => bind(expression),
                _ => Err(Error::InvalidArgument(
                    "internal timestamp parameter accepts positional values only".into(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        bind_timestamp(&name, &args)
    })())
}

fn bind_timestamp(name: &str, args: &[BoundExpr]) -> Result<BoundExpr> {
    let (value, unit, timezone) = match (name, args) {
        (VALUE_FUNCTION, [value, unit, timezone]) => (
            Some(literal_i64(value)?),
            literal_utf8(unit)?,
            literal_utf8(timezone)?,
        ),
        (NULL_FUNCTION, [unit, timezone]) => (None, literal_utf8(unit)?, literal_utf8(timezone)?),
        _ => {
            return Err(Error::InvalidArgument(format!(
                "internal timestamp parameter received {} arguments",
                args.len()
            )));
        }
    };
    let unit = parse_unit(unit)?;
    let timezone = timezone.parse::<chrono_tz::Tz>().map_err(|_| {
        Error::InvalidArgument(format!(
            "timestamp parameter has unknown IANA timezone '{timezone}'"
        ))
    })?;
    let timezone = timezone.to_string();
    Ok(match value {
        Some(value) => BoundExpr::literal(ScalarValue::Timestamp {
            value,
            unit,
            timezone: Some(timezone),
        }),
        None => cast_if_needed(
            BoundExpr::literal(ScalarValue::Null),
            &DataType::Timestamp(unit, Some(Arc::from(timezone))),
        ),
    })
}

fn literal_i64(expression: &BoundExpr) -> Result<i64> {
    let value = literal_utf8(expression)?;
    value
        .parse::<i64>()
        .map_err(|_| Error::InvalidArgument("internal timestamp parameter value is invalid".into()))
}

fn literal_utf8(expression: &BoundExpr) -> Result<&str> {
    match &expression.kind {
        ExprKind::Literal(ScalarValue::Utf8(value)) => Ok(value),
        _ => Err(Error::InvalidArgument(
            "internal timestamp parameter argument is invalid".into(),
        )),
    }
}

fn parse_unit(value: &str) -> Result<TimeUnit> {
    match value {
        "second" => Ok(TimeUnit::Second),
        "millisecond" => Ok(TimeUnit::Millisecond),
        "microsecond" => Ok(TimeUnit::Microsecond),
        "nanosecond" => Ok(TimeUnit::Nanosecond),
        _ => Err(Error::InvalidArgument(
            "internal timestamp parameter unit is invalid".into(),
        )),
    }
}

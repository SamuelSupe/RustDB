use arrow::datatypes::DataType;
use sqlparser::ast::{
    CeilFloorKind, DateTimeField, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, TrimWhereField,
};

use crate::{Error, Result};

use super::coercion::{
    cast_if_needed, coerce_comparison, common_case_type, is_integer, is_numeric, is_string,
};
use super::{BinaryOp, BoundExpr, DateTimePart, ExprKind, ScalarFunction, ScalarValue, UnaryOp};

#[derive(Clone, Copy)]
struct Signature {
    names: &'static [&'static str],
    function: ScalarFunction,
    min_args: usize,
    max_args: usize,
}

const VARIADIC: usize = usize::MAX;

const SIGNATURES: &[Signature] = &[
    Signature::new(&["substring", "substr"], ScalarFunction::Substring, 2, 3),
    Signature::new(&["length", "char_length"], ScalarFunction::Length, 1, 1),
    Signature::new(&["lower"], ScalarFunction::Lower, 1, 1),
    Signature::new(&["upper"], ScalarFunction::Upper, 1, 1),
    Signature::new(&["trim"], ScalarFunction::Trim, 1, 2),
    Signature::new(&["ltrim"], ScalarFunction::LTrim, 1, 2),
    Signature::new(&["rtrim"], ScalarFunction::RTrim, 1, 2),
    Signature::new(&["concat"], ScalarFunction::Concat, 1, VARIADIC),
    Signature::new(&["replace"], ScalarFunction::Replace, 3, 3),
    Signature::new(&["starts_with"], ScalarFunction::StartsWith, 2, 2),
    Signature::new(&["ends_with"], ScalarFunction::EndsWith, 2, 2),
    Signature::new(&["contains"], ScalarFunction::Contains, 2, 2),
    Signature::new(&["coalesce"], ScalarFunction::Coalesce, 1, VARIADIC),
    Signature::new(&["nullif"], ScalarFunction::NullIf, 2, 2),
    Signature::new(&["abs"], ScalarFunction::Abs, 1, 1),
    Signature::new(&["ceil", "ceiling"], ScalarFunction::Ceil, 1, 1),
    Signature::new(&["floor"], ScalarFunction::Floor, 1, 1),
    Signature::new(&["round"], ScalarFunction::Round, 1, 2),
];

impl Signature {
    const fn new(
        names: &'static [&'static str],
        function: ScalarFunction,
        min_args: usize,
        max_args: usize,
    ) -> Self {
        Self {
            names,
            function,
            min_args,
            max_args,
        }
    }
}

pub(super) fn bind_scalar_expr_with<F>(expr: &Expr, bind_arg: &mut F) -> Option<Result<BoundExpr>>
where
    F: FnMut(&Expr) -> Result<BoundExpr>,
{
    match expr {
        Expr::Function(function) if lookup(&function.name.to_string()).is_some() => {
            Some(bind_function_with(function, bind_arg))
        }
        Expr::Extract { field, expr, .. } => Some(
            bind_arg(expr).and_then(|expr| bind_date_part(date_part_field(field), expr, "extract")),
        ),
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => Some(bind_substring_with(
            expr,
            substring_from.as_deref(),
            substring_for.as_deref(),
            bind_arg,
        )),
        Expr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } => Some(bind_trim_with(
            *trim_where,
            trim_what.as_deref(),
            expr,
            trim_characters.as_deref(),
            bind_arg,
        )),
        Expr::Ceil { expr, field } => Some(bind_ceil_floor_with(
            ScalarFunction::Ceil,
            expr,
            field,
            bind_arg,
        )),
        Expr::Floor { expr, field } => Some(bind_ceil_floor_with(
            ScalarFunction::Floor,
            expr,
            field,
            bind_arg,
        )),
        _ => None,
    }
}

fn bind_function_with<F>(function: &Function, bind_arg: &mut F) -> Result<BoundExpr>
where
    F: FnMut(&Expr) -> Result<BoundExpr>,
{
    if function.over.is_some()
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return Err(Error::Unsupported(format!(
            "scalar function {} does not support parameters, FILTER, NULL treatment, WITHIN GROUP, or OVER",
            function.name
        )));
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(Error::InvalidArgument(format!(
            "scalar function {} requires parentheses",
            function.name
        )));
    };
    if arguments.duplicate_treatment == Some(DuplicateTreatment::Distinct)
        || !arguments.clauses.is_empty()
    {
        return Err(Error::Unsupported(format!(
            "scalar function {} does not support DISTINCT or argument clauses",
            function.name
        )));
    }
    let mut args = Vec::with_capacity(arguments.args.len());
    for arg in &arguments.args {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return Err(Error::InvalidArgument(format!(
                "scalar function {} accepts positional expressions only",
                function.name
            )));
        };
        args.push(bind_arg(expr)?);
    }
    bind_named(&function.name.to_string(), args, function.to_string())
}

fn bind_named(name: &str, args: Vec<BoundExpr>, display_name: String) -> Result<BoundExpr> {
    let normalized = name.to_ascii_lowercase();
    if matches!(normalized.as_str(), "date_part" | "date_trunc") {
        return bind_dynamic_date_function(&normalized, args, display_name);
    }
    if matches!(normalized.as_str(), "year" | "month" | "day") {
        let part = parse_date_part(&normalized)?;
        let [expr] = args.as_slice() else {
            return arity_error(name, args.len(), 1, 1);
        };
        return bind_date_part(Ok(part), expr.clone(), display_name);
    }
    let signature = lookup(&normalized)
        .ok_or_else(|| Error::Unsupported(format!("scalar function {name} is not supported")))?;
    if args.len() < signature.min_args || args.len() > signature.max_args {
        return arity_error(name, args.len(), signature.min_args, signature.max_args);
    }
    make_function(signature.function, args, display_name)
}

fn make_function(
    function: ScalarFunction,
    mut args: Vec<BoundExpr>,
    display_name: String,
) -> Result<BoundExpr> {
    let data_type = match function {
        ScalarFunction::Substring => {
            coerce_strings(&mut args[..1], function)?;
            coerce_integer_args(&mut args, 1)?;
            DataType::Utf8
        }
        ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::Trim
        | ScalarFunction::LTrim
        | ScalarFunction::RTrim
        | ScalarFunction::Concat
        | ScalarFunction::Replace => {
            coerce_strings(&mut args, function)?;
            DataType::Utf8
        }
        ScalarFunction::Length => {
            coerce_strings(&mut args, function)?;
            DataType::Int64
        }
        ScalarFunction::StartsWith | ScalarFunction::EndsWith | ScalarFunction::Contains => {
            coerce_strings(&mut args, function)?;
            DataType::Boolean
        }
        ScalarFunction::Coalesce => {
            let mut common = DataType::Null;
            for arg in &args {
                common = common_case_type(&common, &arg.data_type)?;
            }
            for arg in &mut args {
                *arg = cast_if_needed(arg.clone(), &common);
            }
            common
        }
        ScalarFunction::NullIf => {
            let right = args.pop().expect("arity checked");
            let left = args.pop().expect("arity checked");
            let output = left.data_type.clone();
            let original_left = left.clone();
            let (comparison_left, comparison_right) = coerce_comparison(left, right)?;
            // Keep the original value as the function result. Comparison may
            // widen it (for example Int64 to Float64), and casting that value
            // back could lose integer precision even though NULLIF must
            // return its first argument's type and exact value.
            args.extend([original_left, comparison_left, comparison_right]);
            output
        }
        ScalarFunction::Abs
        | ScalarFunction::Ceil
        | ScalarFunction::Floor
        | ScalarFunction::Round => {
            if !is_numeric(&args[0].data_type) {
                return Err(Error::InvalidArgument(format!(
                    "{function} requires a numeric first argument, got {}",
                    args[0].data_type
                )));
            }
            if args.len() == 2 {
                coerce_integer_args(&mut args, 1)?;
            }
            let mut target = canonical_numeric_type(&args[0].data_type);
            if matches!(function, ScalarFunction::Ceil | ScalarFunction::Floor)
                && is_integer(&target)
            {
                target = DataType::Float64;
            }
            args[0] = cast_if_needed(args[0].clone(), &target);
            decimal_numeric_output(function, &target, args.get(1))?
        }
        ScalarFunction::DatePart(_) | ScalarFunction::DateTrunc(_) => {
            return Err(Error::Internal(
                "date functions must be bound through their specialized signature".into(),
            ));
        }
    };
    Ok(BoundExpr {
        kind: ExprKind::ScalarFunction { function, args },
        data_type,
        display_name,
    })
}

fn bind_dynamic_date_function(
    name: &str,
    mut args: Vec<BoundExpr>,
    display_name: String,
) -> Result<BoundExpr> {
    if args.len() != 2 {
        return arity_error(name, args.len(), 2, 2);
    }
    let unit = args.remove(0);
    let ExprKind::Literal(ScalarValue::Utf8(unit)) = unit.kind else {
        return Err(Error::InvalidArgument(format!(
            "{name} unit must be a constant string"
        )));
    };
    let part = parse_date_part(&unit)?;
    let expr = args.remove(0);
    if name == "date_part" {
        bind_date_part(Ok(part), expr, display_name)
    } else {
        bind_date_trunc(part, expr, display_name)
    }
}

fn bind_date_part(
    part: Result<DateTimePart>,
    expr: BoundExpr,
    display_name: impl Into<String>,
) -> Result<BoundExpr> {
    let part = part?;
    ensure_temporal(&expr, "date_part")?;
    Ok(BoundExpr {
        kind: ExprKind::ScalarFunction {
            function: ScalarFunction::DatePart(part),
            args: vec![expr],
        },
        data_type: DataType::Int64,
        display_name: display_name.into(),
    })
}

fn bind_date_trunc(
    part: DateTimePart,
    expr: BoundExpr,
    display_name: impl Into<String>,
) -> Result<BoundExpr> {
    ensure_temporal(&expr, "date_trunc")?;
    if matches!(&expr.data_type, DataType::Timestamp(_, Some(_))) {
        return Err(Error::Unsupported(
            "date_trunc on timezone-aware TIMESTAMP is not supported without a session timezone"
                .into(),
        ));
    }
    // DuckDB returns a calendar DATE for year/month/day truncation and a
    // TIMESTAMP for sub-day truncation, independently of whether the input is
    // DATE or TIMESTAMP.
    let data_type = match part {
        DateTimePart::Year | DateTimePart::Month | DateTimePart::Day => DataType::Date32,
        DateTimePart::Hour | DateTimePart::Minute | DateTimePart::Second => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
    };
    Ok(BoundExpr {
        kind: ExprKind::ScalarFunction {
            function: ScalarFunction::DateTrunc(part),
            args: vec![expr],
        },
        data_type,
        display_name: display_name.into(),
    })
}

fn bind_substring_with<F>(
    expr: &Expr,
    from: Option<&Expr>,
    length: Option<&Expr>,
    bind_arg: &mut F,
) -> Result<BoundExpr>
where
    F: FnMut(&Expr) -> Result<BoundExpr>,
{
    let mut args = vec![bind_arg(expr)?];
    args.push(match from {
        Some(from) => bind_arg(from)?,
        None => BoundExpr::literal(ScalarValue::Int64(1)),
    });
    if let Some(length) = length {
        args.push(bind_arg(length)?);
    }
    make_function(ScalarFunction::Substring, args, "substring".into())
}

fn bind_trim_with<F>(
    trim_where: Option<TrimWhereField>,
    trim_what: Option<&Expr>,
    expr: &Expr,
    trim_characters: Option<&[Expr]>,
    bind_arg: &mut F,
) -> Result<BoundExpr>
where
    F: FnMut(&Expr) -> Result<BoundExpr>,
{
    let function = match trim_where.unwrap_or(TrimWhereField::Both) {
        TrimWhereField::Both => ScalarFunction::Trim,
        TrimWhereField::Leading => ScalarFunction::LTrim,
        TrimWhereField::Trailing => ScalarFunction::RTrim,
    };
    let custom = match (trim_what, trim_characters) {
        (Some(value), None | Some([])) => Some(value),
        (None, Some([value])) => Some(value),
        (None, None | Some([])) => None,
        _ => {
            return Err(Error::InvalidArgument(
                "TRIM accepts one custom character-set expression".into(),
            ));
        }
    };
    let mut args = vec![bind_arg(expr)?];
    if let Some(custom) = custom {
        args.push(bind_arg(custom)?);
    }
    make_function(function, args, "trim".into())
}

fn bind_ceil_floor_with<F>(
    function: ScalarFunction,
    expr: &Expr,
    field: &CeilFloorKind,
    bind_arg: &mut F,
) -> Result<BoundExpr>
where
    F: FnMut(&Expr) -> Result<BoundExpr>,
{
    let args = vec![bind_arg(expr)?];
    match field {
        CeilFloorKind::DateTimeField(DateTimeField::NoDateTime) => {}
        CeilFloorKind::Scale(_) => {
            return Err(Error::Unsupported(format!(
                "{function} with a scale argument is not supported"
            )));
        }
        CeilFloorKind::DateTimeField(field) => {
            return Err(Error::Unsupported(format!(
                "{function} TO {field} is not supported"
            )));
        }
    }
    make_function(function, args, function.to_string())
}

fn lookup(name: &str) -> Option<&'static Signature> {
    let name = name.to_ascii_lowercase();
    SIGNATURES
        .iter()
        .find(|signature| signature.names.contains(&name.as_str()))
        .or_else(|| {
            matches!(
                name.as_str(),
                "date_part" | "date_trunc" | "year" | "month" | "day"
            )
            .then(|| &SIGNATURES[0])
        })
}

fn coerce_strings(args: &mut [BoundExpr], function: ScalarFunction) -> Result<()> {
    for arg in args {
        if arg.data_type != DataType::Null && !is_string(&arg.data_type) {
            return Err(Error::InvalidArgument(format!(
                "{function} requires string arguments, got {}",
                arg.data_type
            )));
        }
        *arg = cast_if_needed(arg.clone(), &DataType::Utf8);
    }
    Ok(())
}

fn coerce_integer_args(args: &mut [BoundExpr], start: usize) -> Result<()> {
    for arg in &mut args[start..] {
        if arg.data_type != DataType::Null && !is_integer(&arg.data_type) {
            return Err(Error::InvalidArgument(format!(
                "function argument must be an integer, got {}",
                arg.data_type
            )));
        }
        *arg = cast_if_needed(arg.clone(), &DataType::Int64);
    }
    Ok(())
}

fn ensure_temporal(expr: &BoundExpr, function: &str) -> Result<()> {
    if matches!(expr.data_type, DataType::Date32 | DataType::Timestamp(_, _)) {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "{function} requires DATE or TIMESTAMP, got {}",
            expr.data_type
        )))
    }
}

fn canonical_numeric_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            DataType::UInt64
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::Decimal128(_, _) => data_type.clone(),
        _ => DataType::Int64,
    }
}

fn decimal_numeric_output(
    function: ScalarFunction,
    input: &DataType,
    scale: Option<&BoundExpr>,
) -> Result<DataType> {
    let DataType::Decimal128(precision, input_scale) = input else {
        return Ok(input.clone());
    };
    let output_scale = match function {
        ScalarFunction::Abs => *input_scale,
        ScalarFunction::Ceil | ScalarFunction::Floor => 0,
        ScalarFunction::Round => {
            let requested = match scale {
                None => 0,
                Some(scale) => match constant_integer(scale)? {
                    Some(value) => value,
                    None => {
                        return Err(Error::InvalidArgument(
                            "ROUND scale for DECIMAL input must be a constant integer".into(),
                        ));
                    }
                },
            };
            i64::from(*input_scale).min(requested.max(0)) as i8
        }
        _ => unreachable!("caller passes only numeric scalar functions"),
    };
    Ok(DataType::Decimal128(*precision, output_scale))
}

fn constant_integer(expression: &BoundExpr) -> Result<Option<i64>> {
    let value = match &expression.kind {
        ExprKind::Literal(ScalarValue::Int64(value)) => Some(*value),
        ExprKind::Literal(ScalarValue::UInt64(value)) => i64::try_from(*value).ok(),
        ExprKind::Cast { expr } => constant_integer(expr)?,
        ExprKind::Unary {
            op: UnaryOp::Negate,
            expr,
        } => constant_integer(expr)?
            .map(|value| {
                value.checked_neg().ok_or_else(|| {
                    Error::InvalidArgument("ROUND scale constant overflows Int64".into())
                })
            })
            .transpose()?,
        ExprKind::Binary { left, op, right } => {
            let (Some(left), Some(right)) = (constant_integer(left)?, constant_integer(right)?)
            else {
                return Ok(None);
            };
            let value = match op {
                BinaryOp::Add => left.checked_add(right),
                BinaryOp::Subtract => left.checked_sub(right),
                BinaryOp::Multiply => left.checked_mul(right),
                BinaryOp::Modulo if right != 0 => left.checked_rem(right),
                _ => return Ok(None),
            }
            .ok_or_else(|| Error::InvalidArgument("ROUND scale constant overflows Int64".into()))?;
            Some(value)
        }
        _ => None,
    };
    Ok(value)
}

fn parse_date_part(value: &str) -> Result<DateTimePart> {
    match value.trim().to_ascii_lowercase().as_str() {
        "year" | "years" => Ok(DateTimePart::Year),
        "month" | "months" => Ok(DateTimePart::Month),
        "day" | "days" => Ok(DateTimePart::Day),
        "hour" | "hours" => Ok(DateTimePart::Hour),
        "minute" | "minutes" => Ok(DateTimePart::Minute),
        "second" | "seconds" => Ok(DateTimePart::Second),
        other => Err(Error::Unsupported(format!(
            "date part '{other}' is not supported"
        ))),
    }
}

fn date_part_field(field: &DateTimeField) -> Result<DateTimePart> {
    parse_date_part(&field.to_string())
}

fn arity_error<T>(name: &str, actual: usize, min: usize, max: usize) -> Result<T> {
    let expected = if max == VARIADIC {
        format!("at least {min}")
    } else if min == max {
        min.to_string()
    } else {
        format!("{min} to {max}")
    };
    Err(Error::InvalidArgument(format!(
        "scalar function {name} expects {expected} arguments, got {actual}"
    )))
}

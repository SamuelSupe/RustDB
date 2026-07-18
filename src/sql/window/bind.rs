use std::collections::HashMap;

use arrow::datatypes::DataType;
use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArguments, NamedWindowDefinition,
    NamedWindowExpr, WindowFrameBound as AstBound, WindowFrameUnits as AstUnits, WindowSpec,
    WindowType,
};

use crate::{Error, Result};

use super::ast::WindowCall;
use crate::sql::coercion::{cast_if_needed, common_case_type};
use crate::sql::{
    BoundExpr, ExprKind, ScalarValue, SortExpr, WindowExpr, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunction, aggregate::bind_window_aggregate,
};

pub(super) struct WindowBinding {
    pub(super) expression: sqlparser::ast::Expr,
    pub(super) hidden_name: String,
    pub(super) bound: WindowExpr,
}

pub(super) fn bind_windows(
    calls: &[WindowCall],
    named: &[NamedWindowDefinition],
    mut bind: impl FnMut(&sqlparser::ast::Expr) -> Result<BoundExpr>,
) -> Result<Vec<WindowBinding>> {
    let named = named_windows(named)?;
    calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            let spec = resolve_spec(&call.function, &named)?;
            let partition_by = spec
                .partition_by
                .iter()
                .map(&mut bind)
                .collect::<Result<Vec<_>>>()?;
            let order_by = spec
                .order_by
                .iter()
                .map(|order| {
                    if order.with_fill.is_some() {
                        return Err(Error::Unsupported(
                            "window ORDER BY WITH FILL is not supported".into(),
                        ));
                    }
                    Ok(SortExpr {
                        expr: bind(&order.expr)?,
                        descending: order.options.asc == Some(false),
                        nulls_first: order.options.nulls_first.unwrap_or(false),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let function = bind_function(&call.function, &mut bind)?;
            let frame = bind_frame(spec.window_frame.as_ref(), &order_by, &mut bind)?;
            validate_function_frame(&function, frame)?;
            let data_type = match &function {
                WindowFunction::RowNumber
                | WindowFunction::Rank
                | WindowFunction::DenseRank
                | WindowFunction::Ntile(_) => DataType::Int64,
                WindowFunction::PercentRank | WindowFunction::CumeDist => DataType::Float64,
                WindowFunction::Lead { expr, .. }
                | WindowFunction::Lag { expr, .. }
                | WindowFunction::FirstValue(expr)
                | WindowFunction::LastValue(expr) => expr.data_type.clone(),
                WindowFunction::Aggregate(aggregate) => aggregate.data_type.clone(),
            };
            Ok(WindowBinding {
                expression: call.expression.clone(),
                hidden_name: format!("__rustdb_window_{index}"),
                bound: WindowExpr {
                    function,
                    partition_by,
                    order_by,
                    frame,
                    data_type,
                    display_name: call.expression.to_string(),
                },
            })
        })
        .collect()
}

fn named_windows(named: &[NamedWindowDefinition]) -> Result<HashMap<String, WindowSpec>> {
    let mut output = HashMap::new();
    for NamedWindowDefinition(name, expression) in named {
        let key = name.value.to_ascii_lowercase();
        if output.contains_key(&key) {
            return Err(Error::InvalidArgument(format!(
                "window '{}' is defined more than once",
                name.value
            )));
        }
        let NamedWindowExpr::WindowSpec(spec) = expression else {
            return Err(Error::Unsupported(
                "named window inheritance is not supported".into(),
            ));
        };
        if spec.window_name.is_some() {
            return Err(Error::Unsupported(
                "named window inheritance and overrides are not supported".into(),
            ));
        }
        output.insert(key, spec.clone());
    }
    Ok(output)
}

fn resolve_spec(function: &Function, named: &HashMap<String, WindowSpec>) -> Result<WindowSpec> {
    let over = function
        .over
        .as_ref()
        .ok_or_else(|| Error::Internal("window call has no OVER clause".into()))?;
    match over {
        WindowType::NamedWindow(name) => named
            .get(&name.value.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| Error::Catalog(format!("window '{}' does not exist", name.value))),
        WindowType::WindowSpec(spec) if spec.window_name.is_some() => Err(Error::Unsupported(
            "named window inheritance and overrides are not supported".into(),
        )),
        WindowType::WindowSpec(spec) => Ok(spec.clone()),
    }
}

fn bind_function(
    function: &Function,
    bind: &mut impl FnMut(&sqlparser::ast::Expr) -> Result<BoundExpr>,
) -> Result<WindowFunction> {
    if function.filter.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return Err(Error::Unsupported(
            "window FILTER, ordered arguments, parameters, and NULL treatment are not supported"
                .into(),
        ));
    }
    let name = function.name.to_string().to_ascii_lowercase();
    if matches!(name.as_str(), "lead" | "lag") {
        return bind_offset_function(function, name == "lead", bind);
    }
    if matches!(name.as_str(), "first_value" | "last_value") {
        return bind_value_function(function, name == "first_value", bind);
    }
    if matches!(
        name.as_str(),
        "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist"
    ) {
        let FunctionArguments::List(arguments) = &function.args else {
            return Err(Error::InvalidArgument(format!(
                "window function {} requires parentheses",
                function.name
            )));
        };
        if !arguments.args.is_empty()
            || !arguments.clauses.is_empty()
            || arguments.duplicate_treatment.is_some()
        {
            return Err(Error::InvalidArgument(format!(
                "window function {} does not accept arguments",
                function.name
            )));
        }
        return Ok(match name.as_str() {
            "row_number" => WindowFunction::RowNumber,
            "rank" => WindowFunction::Rank,
            "dense_rank" => WindowFunction::DenseRank,
            "percent_rank" => WindowFunction::PercentRank,
            _ => WindowFunction::CumeDist,
        });
    }
    if name == "ntile" {
        let FunctionArguments::List(arguments) = &function.args else {
            return Err(Error::InvalidArgument(
                "window function ntile requires one argument".into(),
            ));
        };
        if !arguments.clauses.is_empty() || arguments.duplicate_treatment.is_some() {
            return Err(Error::InvalidArgument(
                "window function ntile accepts one positive integer argument".into(),
            ));
        }
        let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice()
        else {
            return Err(Error::InvalidArgument(
                "window function ntile accepts one positive integer argument".into(),
            ));
        };
        let argument = bind(argument)?;
        let buckets = match argument.kind {
            ExprKind::Literal(ScalarValue::Int64(value)) if value > 0 => value as u64,
            ExprKind::Literal(ScalarValue::UInt64(value)) if value > 0 => value,
            _ => {
                return Err(Error::InvalidArgument(
                    "window function ntile requires a positive integer constant".into(),
                ));
            }
        };
        return Ok(WindowFunction::Ntile(buckets));
    }
    Ok(WindowFunction::Aggregate(bind_window_aggregate(
        function, bind,
    )?))
}

fn bind_frame(
    frame: Option<&sqlparser::ast::WindowFrame>,
    order_by: &[SortExpr],
    bind: &mut impl FnMut(&sqlparser::ast::Expr) -> Result<BoundExpr>,
) -> Result<WindowFrame> {
    let Some(frame) = frame else {
        return Ok(if !order_by.is_empty() {
            WindowFrame {
                units: WindowFrameUnits::Range,
                start: WindowFrameBound::UnboundedPreceding,
                end: WindowFrameBound::CurrentRow,
            }
        } else {
            WindowFrame::whole_partition()
        });
    };
    let units = match frame.units {
        AstUnits::Rows => WindowFrameUnits::Rows,
        AstUnits::Range => WindowFrameUnits::Range,
        AstUnits::Groups => WindowFrameUnits::Groups,
    };
    let start = bind_bound(&frame.start_bound, bind)?;
    let end = frame
        .end_bound
        .as_ref()
        .map(|bound| bind_bound(bound, bind))
        .transpose()?
        .unwrap_or(WindowFrameBound::CurrentRow);
    let bounded = matches!(
        start,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    ) || matches!(
        end,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    );
    if units == WindowFrameUnits::Groups && order_by.is_empty() {
        return Err(Error::InvalidArgument(
            "GROUPS window frames require ORDER BY".into(),
        ));
    }
    if units == WindowFrameUnits::Range && bounded {
        if order_by.len() != 1 {
            return Err(Error::InvalidArgument(
                "bounded RANGE window frames require exactly one ORDER BY expression".into(),
            ));
        }
        if !crate::sql::coercion::is_numeric(&order_by[0].expr.data_type) {
            return Err(Error::InvalidArgument(format!(
                "bounded RANGE offset requires a numeric ORDER BY expression, got {}",
                order_by[0].expr.data_type
            )));
        }
        if matches!(order_by[0].expr.data_type, DataType::Decimal128(_, scale) if scale < 0) {
            return Err(Error::InvalidArgument(
                "bounded RANGE does not support negative-scale DECIMAL ordering".into(),
            ));
        }
    }
    validate_frame(start, end)?;
    Ok(WindowFrame { units, start, end })
}

fn bind_bound(
    bound: &AstBound,
    bind: &mut impl FnMut(&sqlparser::ast::Expr) -> Result<BoundExpr>,
) -> Result<WindowFrameBound> {
    match bound {
        AstBound::Preceding(None) => Ok(WindowFrameBound::UnboundedPreceding),
        AstBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        AstBound::Following(None) => Ok(WindowFrameBound::UnboundedFollowing),
        AstBound::Preceding(Some(value)) => Ok(WindowFrameBound::Preceding(non_negative_constant(
            bind(value)?,
            "window frame offset",
        )?)),
        AstBound::Following(Some(value)) => Ok(WindowFrameBound::Following(non_negative_constant(
            bind(value)?,
            "window frame offset",
        )?)),
    }
}

fn bind_offset_function(
    function: &Function,
    lead: bool,
    bind: &mut impl FnMut(&sqlparser::ast::Expr) -> Result<BoundExpr>,
) -> Result<WindowFunction> {
    reject_navigation_modifiers(function)?;
    let arguments = plain_arguments(function)?;
    if !(1..=3).contains(&arguments.len()) {
        return Err(Error::InvalidArgument(format!(
            "window function {} expects value[, offset[, default]]",
            function.name
        )));
    }
    let mut value = bind(unnamed_expr(&arguments[0], &function.name.to_string())?)?;
    let offset = if let Some(argument) = arguments.get(1) {
        non_negative_constant(
            bind(unnamed_expr(argument, &function.name.to_string())?)?,
            "window offset",
        )?
    } else {
        1
    };
    let mut default = if let Some(argument) = arguments.get(2) {
        bind(unnamed_expr(argument, &function.name.to_string())?)?
    } else {
        BoundExpr::literal(ScalarValue::Null)
    };
    let data_type = common_case_type(&value.data_type, &default.data_type).map_err(|_| {
        Error::InvalidArgument(format!(
            "{} default type {} is not compatible with value type {}",
            function.name, default.data_type, value.data_type
        ))
    })?;
    value = cast_if_needed(value, &data_type);
    default = cast_if_needed(default, &data_type);
    Ok(if lead {
        WindowFunction::Lead {
            expr: value,
            offset,
            default,
        }
    } else {
        WindowFunction::Lag {
            expr: value,
            offset,
            default,
        }
    })
}

fn bind_value_function(
    function: &Function,
    first: bool,
    bind: &mut impl FnMut(&sqlparser::ast::Expr) -> Result<BoundExpr>,
) -> Result<WindowFunction> {
    reject_navigation_modifiers(function)?;
    let arguments = plain_arguments(function)?;
    let [argument] = arguments else {
        return Err(Error::InvalidArgument(format!(
            "window function {} expects one expression",
            function.name
        )));
    };
    let expression = bind(unnamed_expr(argument, &function.name.to_string())?)?;
    Ok(if first {
        WindowFunction::FirstValue(expression)
    } else {
        WindowFunction::LastValue(expression)
    })
}

fn reject_navigation_modifiers(function: &Function) -> Result<()> {
    if function.null_treatment.is_some() {
        return Err(Error::Unsupported(
            "IGNORE/RESPECT NULLS is not supported yet".into(),
        ));
    }
    Ok(())
}

fn plain_arguments(function: &Function) -> Result<&[FunctionArg]> {
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(Error::InvalidArgument(format!(
            "window function {} requires parentheses",
            function.name
        )));
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(Error::Unsupported(format!(
            "window function {} does not support DISTINCT or ordered arguments",
            function.name
        )));
    }
    Ok(&arguments.args)
}

fn unnamed_expr<'a>(argument: &'a FunctionArg, name: &str) -> Result<&'a sqlparser::ast::Expr> {
    match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => Ok(expression),
        _ => Err(Error::InvalidArgument(format!(
            "window function {name} requires expression arguments"
        ))),
    }
}

fn non_negative_constant(expression: BoundExpr, label: &str) -> Result<u64> {
    match expression.kind {
        ExprKind::Literal(ScalarValue::UInt64(value)) => Ok(value),
        ExprKind::Literal(ScalarValue::Int64(value)) if value >= 0 => Ok(value as u64),
        _ => Err(Error::InvalidArgument(format!(
            "{label} must be a non-negative integer constant"
        ))),
    }
}

fn validate_frame(start: WindowFrameBound, end: WindowFrameBound) -> Result<()> {
    if start == WindowFrameBound::UnboundedFollowing {
        return Err(Error::InvalidArgument(
            "window frame cannot start with UNBOUNDED FOLLOWING".into(),
        ));
    }
    if end == WindowFrameBound::UnboundedPreceding {
        return Err(Error::InvalidArgument(
            "window frame cannot end with UNBOUNDED PRECEDING".into(),
        ));
    }
    let after = match (start, end) {
        (WindowFrameBound::Following(left), WindowFrameBound::Following(right)) => left > right,
        (
            WindowFrameBound::Following(_),
            WindowFrameBound::CurrentRow | WindowFrameBound::Preceding(_),
        ) => true,
        (WindowFrameBound::CurrentRow, WindowFrameBound::Preceding(_)) => true,
        (WindowFrameBound::Preceding(left), WindowFrameBound::Preceding(right)) => left < right,
        _ => false,
    };
    if after {
        return Err(Error::InvalidArgument(
            "window frame start is after its end".into(),
        ));
    }
    Ok(())
}

fn validate_function_frame(function: &WindowFunction, frame: WindowFrame) -> Result<()> {
    let _ = (function, frame);
    Ok(())
}

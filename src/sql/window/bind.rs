use std::collections::HashMap;

use arrow::datatypes::DataType;
use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArguments, NamedWindowDefinition,
    NamedWindowExpr, WindowFrameBound as AstBound, WindowFrameUnits as AstUnits, WindowSpec,
    WindowType,
};

use crate::{Error, Result};

use super::ast::WindowCall;
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
            let frame = bind_frame(spec.window_frame.as_ref(), !order_by.is_empty())?;
            let function = bind_function(&call.function, &mut bind)?;
            let data_type = match &function {
                WindowFunction::RowNumber
                | WindowFunction::Rank
                | WindowFunction::DenseRank
                | WindowFunction::Ntile(_) => DataType::Int64,
                WindowFunction::PercentRank | WindowFunction::CumeDist => DataType::Float64,
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
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return Err(Error::Unsupported(
            "window FILTER, ordered arguments, parameters, and NULL treatment are not supported"
                .into(),
        ));
    }
    let name = function.name.to_string().to_ascii_lowercase();
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

fn bind_frame(frame: Option<&sqlparser::ast::WindowFrame>, ordered: bool) -> Result<WindowFrame> {
    let Some(frame) = frame else {
        return Ok(if ordered {
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
        AstUnits::Groups => {
            return Err(Error::Unsupported(
                "GROUPS window frames are not supported".into(),
            ));
        }
    };
    let start = bind_bound(&frame.start_bound)?;
    let end = frame
        .end_bound
        .as_ref()
        .map(bind_bound)
        .transpose()?
        .unwrap_or(WindowFrameBound::CurrentRow);
    if start != WindowFrameBound::UnboundedPreceding
        || !matches!(
            end,
            WindowFrameBound::CurrentRow | WindowFrameBound::UnboundedFollowing
        )
    {
        return Err(Error::Unsupported(
            "only UNBOUNDED PRECEDING to CURRENT ROW or UNBOUNDED FOLLOWING frames are supported"
                .into(),
        ));
    }
    Ok(WindowFrame { units, start, end })
}

fn bind_bound(bound: &AstBound) -> Result<WindowFrameBound> {
    match bound {
        AstBound::Preceding(None) => Ok(WindowFrameBound::UnboundedPreceding),
        AstBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        AstBound::Following(None) => Ok(WindowFrameBound::UnboundedFollowing),
        AstBound::Preceding(Some(_)) | AstBound::Following(Some(_)) => Err(Error::Unsupported(
            "bounded window frames are not supported".into(),
        )),
    }
}

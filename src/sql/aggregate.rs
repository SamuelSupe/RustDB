use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use sqlparser::ast::{
    CastKind, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    Ident, SelectItem, UnaryOperator,
};

use crate::{Error, Result};

use super::{
    AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, LogicalPlan, PlanSchema, ScalarValue,
    UnaryOp,
    binder::{
        bind_expr, ensure_boolean, make_binary, make_case, make_is_null, make_like, make_unary,
        map_binary, parse_escape,
    },
    coercion::{cast, is_numeric},
};

pub(super) fn is_aggregate_query(
    group_exprs: &[Expr],
    projection: &[SelectItem],
    having: Option<&Expr>,
) -> bool {
    !group_exprs.is_empty() || having.is_some() || projection.iter().any(select_item_has_aggregate)
}

pub(super) fn plan_aggregate_projection(
    input: LogicalPlan,
    group_ast: &[Expr],
    items: &[SelectItem],
    having: Option<&Expr>,
) -> Result<LogicalPlan> {
    if items.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    }) {
        return Err(Error::Unsupported(
            "wildcards are not supported in aggregate queries".into(),
        ));
    }
    let group_exprs = group_ast
        .iter()
        .map(|expr| bind_expr(expr, input.schema()))
        .collect::<Result<Vec<_>>>()?;
    let mut aggregate_exprs = Vec::new();
    let mut output_exprs = Vec::with_capacity(items.len());
    for item in items {
        let (expr, alias) = select_expr(item)?;
        let mut bound = bind_after_aggregate(
            expr,
            input.schema(),
            group_ast,
            &group_exprs,
            &mut aggregate_exprs,
        )?;
        if let Some(alias) = alias {
            bound.display_name = alias.value.clone();
        }
        output_exprs.push(bound);
    }
    let having = having
        .map(|expr| {
            bind_after_aggregate(
                expr,
                input.schema(),
                group_ast,
                &group_exprs,
                &mut aggregate_exprs,
            )
        })
        .transpose()?;
    if let Some(having) = &having {
        ensure_boolean(having).map_err(|_| {
            Error::InvalidArgument(format!("HAVING requires BOOLEAN, got {}", having.data_type))
        })?;
    }

    let aggregate_schema = aggregate_schema(&group_exprs, &aggregate_exprs);
    let mut plan = LogicalPlan::Aggregate {
        input: Box::new(input),
        group_exprs,
        aggregate_exprs,
        schema: aggregate_schema.clone(),
    };
    if let Some(predicate) = having {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
            schema: aggregate_schema,
        };
    }
    let schema = expression_schema(&output_exprs);
    Ok(LogicalPlan::Projection {
        input: Box::new(plan),
        expressions: output_exprs,
        schema,
    })
}

fn bind_after_aggregate(
    expr: &Expr,
    input_schema: &PlanSchema,
    group_ast: &[Expr],
    group_exprs: &[BoundExpr],
    aggregates: &mut Vec<AggregateExpr>,
) -> Result<BoundExpr> {
    if let Some(index) = group_ast.iter().position(|group| group == expr) {
        let group = &group_exprs[index];
        return Ok(BoundExpr::column(
            index,
            group.data_type.clone(),
            group.display_name.clone(),
        ));
    }
    match expr {
        Expr::Function(function) if aggregate_function(function).is_some() => {
            let aggregate = bind_aggregate(function, input_schema)?;
            let aggregate_index = if let Some(index) = aggregates
                .iter()
                .position(|existing| existing == &aggregate)
            {
                index
            } else {
                aggregates.push(aggregate.clone());
                aggregates.len() - 1
            };
            Ok(BoundExpr::column(
                group_exprs.len() + aggregate_index,
                aggregate.data_type,
                aggregate.display_name,
            ))
        }
        Expr::Value(_) | Expr::TypedString(_) | Expr::Interval(_) => bind_expr(expr, input_schema),
        Expr::Nested(expr) => {
            bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)
        }
        Expr::BinaryOp { left, op, right } => make_binary(
            bind_after_aggregate(left, input_schema, group_ast, group_exprs, aggregates)?,
            map_binary(op.clone())?,
            bind_after_aggregate(right, input_schema, group_ast, group_exprs, aggregates)?,
        ),
        Expr::UnaryOp { op, expr } => {
            let bound =
                bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?;
            match op {
                UnaryOperator::Plus => Ok(bound),
                UnaryOperator::Minus => make_unary(UnaryOp::Negate, bound),
                UnaryOperator::Not => make_unary(UnaryOp::Not, bound),
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
            if *array || format.is_some() || !matches!(kind, CastKind::Cast | CastKind::DoubleColon)
            {
                return Err(Error::Unsupported(
                    "TRY_CAST, SAFE_CAST, CAST ARRAY, and CAST FORMAT are not supported".into(),
                ));
            }
            cast(
                bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?,
                data_type,
            )
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
            make_like(
                bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?,
                bind_after_aggregate(pattern, input_schema, group_ast, group_exprs, aggregates)?,
                *negated,
                escape_char
                    .as_ref()
                    .map(|value| parse_escape(&value.value))
                    .transpose()?,
            )
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let operand = operand
                .as_ref()
                .map(|expr| {
                    bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)
                })
                .transpose()?;
            let branches = conditions
                .iter()
                .map(|branch| {
                    Ok((
                        bind_after_aggregate(
                            &branch.condition,
                            input_schema,
                            group_ast,
                            group_exprs,
                            aggregates,
                        )?,
                        bind_after_aggregate(
                            &branch.result,
                            input_schema,
                            group_ast,
                            group_exprs,
                            aggregates,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_expr = else_result
                .as_ref()
                .map(|expr| {
                    bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)
                })
                .transpose()?;
            make_case(operand, branches, else_expr)
        }
        Expr::IsNull(expr) => make_is_null(
            bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?,
            false,
        ),
        Expr::IsNotNull(expr) => make_is_null(
            bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?,
            true,
        ),
        Expr::IsTrue(expr) => make_binary(
            bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?,
            BinaryOp::Eq,
            BoundExpr::literal(ScalarValue::Boolean(true)),
        ),
        Expr::IsFalse(expr) => make_binary(
            bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?,
            BinaryOp::Eq,
            BoundExpr::literal(ScalarValue::Boolean(false)),
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if list.is_empty() {
                return Ok(BoundExpr::literal(ScalarValue::Boolean(*negated)));
            }
            let needle =
                bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?;
            let comparison = if *negated {
                BinaryOp::NotEq
            } else {
                BinaryOp::Eq
            };
            let connective = if *negated {
                BinaryOp::And
            } else {
                BinaryOp::Or
            };
            let mut output: Option<BoundExpr> = None;
            for candidate in list {
                let comparison = make_binary(
                    needle.clone(),
                    comparison,
                    bind_after_aggregate(
                        candidate,
                        input_schema,
                        group_ast,
                        group_exprs,
                        aggregates,
                    )?,
                )?;
                output = Some(match output {
                    Some(output) => make_binary(output, connective, comparison)?,
                    None => comparison,
                });
            }
            Ok(output.expect("non-empty IN list established above"))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value =
                bind_after_aggregate(expr, input_schema, group_ast, group_exprs, aggregates)?;
            let lower = make_binary(
                value.clone(),
                BinaryOp::GtEq,
                bind_after_aggregate(low, input_schema, group_ast, group_exprs, aggregates)?,
            )?;
            let upper = make_binary(
                value,
                BinaryOp::LtEq,
                bind_after_aggregate(high, input_schema, group_ast, group_exprs, aggregates)?,
            )?;
            let between = make_binary(lower, BinaryOp::And, upper)?;
            if *negated {
                make_unary(UnaryOp::Not, between)
            } else {
                Ok(between)
            }
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Err(Error::InvalidArgument(format!(
            "expression `{expr}` must appear in GROUP BY or be used by an aggregate"
        ))),
        Expr::Function(function) if function.over.is_some() => Err(Error::Unsupported(
            "window functions are not supported".into(),
        )),
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. } => Err(Error::Unsupported(
            "subquery expressions, including correlated subqueries, are not supported".into(),
        )),
        other => Err(Error::Unsupported(format!(
            "aggregate expression `{other}` is not supported"
        ))),
    }
}

fn bind_aggregate(function: &Function, schema: &PlanSchema) -> Result<AggregateExpr> {
    if function.over.is_some()
        || function.filter.is_some()
        || !function.within_group.is_empty()
        || !matches!(&function.parameters, FunctionArguments::None)
    {
        return Err(Error::Unsupported(
            "aggregate modifiers, FILTER, and window OVER clauses are not supported".into(),
        ));
    }
    let aggregate = aggregate_function(function).ok_or_else(|| {
        Error::Unsupported(format!("function {} is not supported", function.name))
    })?;
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(Error::InvalidArgument(format!(
            "aggregate {} requires parentheses",
            function.name
        )));
    };
    if arguments.duplicate_treatment == Some(DuplicateTreatment::Distinct)
        || !arguments.clauses.is_empty()
    {
        return Err(Error::Unsupported(
            "DISTINCT and ordered aggregate arguments are not supported".into(),
        ));
    }
    let expr = match arguments.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))] => Some(bind_expr(expr, schema)?),
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
            if aggregate == AggregateFunction::Count =>
        {
            None
        }
        [] if aggregate == AggregateFunction::Count => None,
        _ => {
            return Err(Error::InvalidArgument(format!(
                "aggregate {} expects one expression (or * for count)",
                function.name
            )));
        }
    };
    if matches!(aggregate, AggregateFunction::Sum | AggregateFunction::Avg)
        && !expr
            .as_ref()
            .is_some_and(|expr| is_numeric(&expr.data_type))
    {
        return Err(Error::InvalidArgument(format!(
            "{} requires a numeric argument",
            function.name
        )));
    }
    let data_type = aggregate_type(aggregate, expr.as_ref(), function)?;
    Ok(AggregateExpr {
        function: aggregate,
        expr,
        data_type,
        display_name: function.to_string(),
    })
}

fn aggregate_type(
    aggregate: AggregateFunction,
    expr: Option<&BoundExpr>,
    function: &Function,
) -> Result<DataType> {
    let input = expr.map(|expr| &expr.data_type);
    Ok(match aggregate {
        AggregateFunction::Count => DataType::Int64,
        AggregateFunction::Avg => DataType::Float64,
        AggregateFunction::Sum => match input {
            Some(DataType::Decimal128(precision, scale)) => {
                DataType::Decimal128(*precision, *scale)
            }
            Some(DataType::Float16 | DataType::Float32 | DataType::Float64) => DataType::Float64,
            Some(DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64) => {
                DataType::UInt64
            }
            _ => DataType::Int64,
        },
        AggregateFunction::Min | AggregateFunction::Max => input.cloned().ok_or_else(|| {
            Error::InvalidArgument(format!("{} requires an argument", function.name))
        })?,
    })
}

fn aggregate_schema(groups: &[BoundExpr], aggregates: &[AggregateExpr]) -> PlanSchema {
    let fields = groups
        .iter()
        .map(|expr| Field::new(expr.display_name.clone(), expr.data_type.clone(), true))
        .chain(
            aggregates
                .iter()
                .map(|expr| Field::new(expr.display_name.clone(), expr.data_type.clone(), true)),
        )
        .collect::<Vec<_>>();
    PlanSchema::unqualified(Arc::new(Schema::new(fields)))
}

fn expression_schema(expressions: &[BoundExpr]) -> PlanSchema {
    PlanSchema::unqualified(Arc::new(Schema::new(
        expressions
            .iter()
            .map(|expr| Field::new(expr.display_name.clone(), expr.data_type.clone(), true))
            .collect::<Vec<_>>(),
    )))
}

fn select_expr(item: &SelectItem) -> Result<(&Expr, Option<&Ident>)> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok((expr, None)),
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias))),
        other => Err(Error::Unsupported(format!(
            "select item `{other}` is not supported in aggregate queries"
        ))),
    }
}

fn select_item_has_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            contains_aggregate(expr)
        }
        _ => false,
    }
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(function) => aggregate_function(function).is_some(),
        Expr::BinaryOp { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || conditions.iter().any(|branch| {
                    contains_aggregate(&branch.condition) || contains_aggregate(&branch.result)
                })
                || else_result.as_deref().is_some_and(contains_aggregate)
        }
        _ => false,
    }
}

fn aggregate_function(function: &Function) -> Option<AggregateFunction> {
    match function.name.to_string().to_ascii_lowercase().as_str() {
        "count" => Some(AggregateFunction::Count),
        "sum" => Some(AggregateFunction::Sum),
        "min" => Some(AggregateFunction::Min),
        "max" => Some(AggregateFunction::Max),
        "avg" => Some(AggregateFunction::Avg),
        _ => None,
    }
}

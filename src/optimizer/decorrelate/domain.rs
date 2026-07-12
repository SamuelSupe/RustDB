use std::sync::Arc;

use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};

use crate::sql::{
    AggregateExpr, AggregateFunction, BoundExpr, DependentJoinKind, ExprKind, JoinType,
    LogicalPlan, PlanSchema, ScalarValue,
};
use crate::{Error, Result};

use super::pull::{Correlation, pull};
use super::rewrite::{
    combine_and, outer_to_domain_columns, remap_columns, residual_to_domain_join, shift_columns,
};

pub(super) fn is_aggregate(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Projection { input, .. } = plan else {
        return false;
    };
    aggregate_input(input).is_some()
}

pub(super) fn rewrite(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: DependentJoinKind,
    guard: Option<BoundExpr>,
    output_schema: PlanSchema,
) -> Result<LogicalPlan> {
    let Parts {
        input,
        mut groups,
        mut aggregates,
        having,
        mut projection,
    } = decompose(right)?;
    let original_group_count = groups.len();
    let aggregate_count = aggregates.len();
    let pulled = pull(input)?;
    let parameters = correlation_parameters(&pulled.correlations)?;
    if parameters.is_empty() {
        return Err(Error::Internal(
            "correlated global aggregate has no outer parameters".into(),
        ));
    }
    let parameter_positions = parameters
        .iter()
        .enumerate()
        .map(|(position, index)| (*index, position))
        .collect::<Vec<_>>();
    let domain = build_domain(left.clone(), &parameters)?;
    let domain_width = parameters.len();

    let mut on = Vec::new();
    let mut residuals = Vec::new();
    for correlation in pulled.correlations {
        match correlation {
            Correlation::Key { mut outer, inner } => {
                outer_to_domain_columns(&mut outer, &parameter_positions)?;
                on.push((outer, inner));
            }
            Correlation::Residual(mut residual) => {
                residual_to_domain_join(&mut residual, domain_width, &parameter_positions)?;
                residuals.push(residual);
            }
        }
    }
    if on.is_empty() {
        return Err(Error::Unsupported(
            "correlated subqueries require at least one outer-to-inner equality key".into(),
        ));
    }

    let (inner, sentinel_index) = append_match_sentinel(pulled.plan);
    let join_type = if original_group_count == 0 {
        JoinType::Left
    } else {
        JoinType::Inner
    };
    let joined_schema = if join_type == JoinType::Left {
        PlanSchema::left_join(domain.schema(), inner.schema())
    } else {
        PlanSchema::join(domain.schema(), inner.schema())
    };
    let joined = LogicalPlan::Join {
        left: Box::new(domain),
        right: Box::new(inner),
        on,
        residual: combine_and(residuals)?,
        null_aware: None,
        join_type,
        schema: joined_schema,
    };

    for group in &mut groups {
        remap_columns(group, &pulled.old_to_new)?;
        shift_columns(group, domain_width)?;
    }
    for aggregate in &mut aggregates {
        if let Some(expression) = &mut aggregate.expr {
            remap_columns(expression, &pulled.old_to_new)?;
            shift_columns(expression, domain_width)?;
            *expression =
                gate_aggregate_argument(expression.clone(), domain_width + sentinel_index);
        } else if aggregate.function == AggregateFunction::Count {
            aggregate.expr = Some(BoundExpr::column(
                domain_width + sentinel_index,
                DataType::Int64,
                "__rustdb_inner_match",
            ));
        }
    }
    let mut domain_groups = parameter_group_exprs(joined.schema(), domain_width);
    domain_groups.extend(groups);
    let aggregate_schema = aggregate_schema(&domain_groups, &aggregates);
    let mut result = LogicalPlan::Aggregate {
        input: Box::new(joined),
        group_exprs: domain_groups,
        aggregate_exprs: aggregates,
        schema: aggregate_schema,
    };

    let aggregate_mapping = (0..original_group_count + aggregate_count)
        .map(|index| domain_width + index)
        .collect::<Vec<_>>();
    if let Some(mut predicate) = having {
        remap_columns(&mut predicate, &aggregate_mapping)?;
        let schema = result.schema().clone();
        result = LogicalPlan::Filter {
            input: Box::new(result),
            predicate,
            schema,
        };
    }
    for expression in &mut projection {
        remap_columns(expression, &aggregate_mapping)?;
    }
    let visible_width = projection.len();
    append_domain_keys(&mut projection, result.schema(), domain_width)?;
    let result_schema = expression_schema(&projection);
    result = LogicalPlan::Projection {
        input: Box::new(result),
        expressions: projection,
        schema: result_schema,
    };

    join_domain_result(
        left,
        result,
        kind,
        guard,
        output_schema,
        &parameters,
        visible_width,
        original_group_count != 0,
    )
}

struct Parts {
    input: LogicalPlan,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<BoundExpr>,
    projection: Vec<BoundExpr>,
}

fn decompose(plan: LogicalPlan) -> Result<Parts> {
    let LogicalPlan::Projection {
        input, expressions, ..
    } = plan
    else {
        return Err(Error::Internal(
            "global aggregate subquery is missing its output projection".into(),
        ));
    };
    let (aggregate, having) = match *input {
        aggregate @ LogicalPlan::Aggregate { .. } => (aggregate, None),
        LogicalPlan::Filter {
            input, predicate, ..
        } => (*input, Some(predicate)),
        _ => {
            return Err(Error::Internal(
                "global aggregate subquery has an unexpected plan shape".into(),
            ));
        }
    };
    let LogicalPlan::Aggregate {
        input,
        group_exprs,
        aggregate_exprs,
        ..
    } = aggregate
    else {
        return Err(Error::Internal(
            "HAVING input is not a global aggregate".into(),
        ));
    };
    Ok(Parts {
        input: *input,
        groups: group_exprs,
        aggregates: aggregate_exprs,
        having,
        projection: expressions,
    })
}

fn aggregate_input(plan: &LogicalPlan) -> Option<(&LogicalPlan, usize)> {
    match plan {
        LogicalPlan::Aggregate {
            input, group_exprs, ..
        } => Some((input, group_exprs.len())),
        LogicalPlan::Filter { input, .. } => match input.as_ref() {
            LogicalPlan::Aggregate {
                input, group_exprs, ..
            } => Some((input, group_exprs.len())),
            _ => None,
        },
        _ => None,
    }
}

fn correlation_parameters(correlations: &[Correlation]) -> Result<Vec<usize>> {
    let mut references = Vec::new();
    for correlation in correlations {
        match correlation {
            Correlation::Key { outer, .. } => outer.outer_references(&mut references),
            Correlation::Residual(expression) => expression.outer_references(&mut references),
        }
    }
    if references.iter().any(|(depth, _)| *depth != 1) {
        return Err(Error::Unsupported(
            "correlated subquery depth greater than 1 is not supported".into(),
        ));
    }
    let mut parameters = references
        .into_iter()
        .map(|(_, index)| index)
        .collect::<Vec<_>>();
    parameters.sort_unstable();
    parameters.dedup();
    Ok(parameters)
}

fn build_domain(left: LogicalPlan, parameters: &[usize]) -> Result<LogicalPlan> {
    let expressions = parameters
        .iter()
        .map(|index| {
            let field = left.schema().arrow().field(*index);
            BoundExpr::column(*index, field.data_type().clone(), field.name().clone())
        })
        .collect::<Vec<_>>();
    let schema = expression_schema(&expressions);
    let projection = LogicalPlan::Projection {
        input: Box::new(left),
        expressions,
        schema: schema.clone(),
    };
    let groups = parameter_group_exprs(&schema, parameters.len());
    Ok(LogicalPlan::Aggregate {
        input: Box::new(projection),
        group_exprs: groups,
        aggregate_exprs: Vec::new(),
        schema,
    })
}

fn append_match_sentinel(plan: LogicalPlan) -> (LogicalPlan, usize) {
    let width = plan.schema().arrow().fields().len();
    let mut expressions = (0..width)
        .map(|index| {
            let field = plan.schema().arrow().field(index);
            BoundExpr::column(index, field.data_type().clone(), field.name().clone())
        })
        .collect::<Vec<_>>();
    let mut sentinel = BoundExpr::literal(ScalarValue::Int64(1));
    sentinel.display_name = "__rustdb_inner_match".into();
    expressions.push(sentinel);
    let schema = expression_schema(&expressions);
    (
        LogicalPlan::Projection {
            input: Box::new(plan),
            expressions,
            schema,
        },
        width,
    )
}

fn parameter_group_exprs(schema: &PlanSchema, width: usize) -> Vec<BoundExpr> {
    (0..width)
        .map(|index| {
            let field = schema.arrow().field(index);
            BoundExpr::column(index, field.data_type().clone(), field.name().clone())
        })
        .collect()
}

fn append_domain_keys(
    expressions: &mut Vec<BoundExpr>,
    schema: &PlanSchema,
    domain_width: usize,
) -> Result<()> {
    for index in 0..domain_width {
        let field = schema.arrow().field(index);
        let raw = BoundExpr::column(index, field.data_type().clone(), field.name().clone());
        let (is_null, value) = canonical_pair(raw)?;
        expressions.push(is_null);
        expressions.push(value);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn join_domain_result(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: DependentJoinKind,
    guard: Option<BoundExpr>,
    output_schema: PlanSchema,
    parameters: &[usize],
    visible_width: usize,
    scalar_single: bool,
) -> Result<LogicalPlan> {
    let mut on = Vec::with_capacity(parameters.len() * 2);
    for (position, outer_index) in parameters.iter().enumerate() {
        let field = left.schema().arrow().field(*outer_index);
        let raw = BoundExpr::column(
            *outer_index,
            field.data_type().clone(),
            field.name().clone(),
        );
        let (outer_null, outer_value) = canonical_pair(raw)?;
        let null_field = right.schema().arrow().field(visible_width + position * 2);
        let value_field = right
            .schema()
            .arrow()
            .field(visible_width + position * 2 + 1);
        on.push((
            outer_null,
            BoundExpr::column(
                visible_width + position * 2,
                null_field.data_type().clone(),
                null_field.name().clone(),
            ),
        ));
        on.push((
            outer_value,
            BoundExpr::column(
                visible_width + position * 2 + 1,
                value_field.data_type().clone(),
                value_field.name().clone(),
            ),
        ));
    }
    match kind {
        DependentJoinKind::GuardedScalar { .. } => Err(Error::Internal(
            "guarded scalar projection was not delayed before aggregate domain decorrelation"
                .into(),
        )),
        DependentJoinKind::Scalar => {
            let left_width = left.schema().arrow().fields().len();
            let schema = PlanSchema::left_join(left.schema(), right.schema());
            let residual = if scalar_single { None } else { guard.clone() };
            let join = LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                residual,
                null_aware: None,
                join_type: if scalar_single {
                    JoinType::LeftSingle
                } else {
                    JoinType::Left
                },
                schema,
            };
            let field = output_schema.arrow().field(left_width);
            let result =
                BoundExpr::column(left_width, field.data_type().clone(), field.name().clone());
            if let Some(guard) = guard {
                return crate::sql::scalar_subquery::guarded::project(
                    join,
                    result,
                    guard,
                    output_schema,
                );
            }
            let mut expressions = (0..left_width)
                .map(|index| {
                    let field = output_schema.arrow().field(index);
                    BoundExpr::column(index, field.data_type().clone(), field.name().clone())
                })
                .collect::<Vec<_>>();
            expressions.push(result);
            Ok(LogicalPlan::Projection {
                input: Box::new(join),
                expressions,
                schema: output_schema,
            })
        }
        DependentJoinKind::Exists => Ok(LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            on,
            residual: guard,
            null_aware: None,
            join_type: JoinType::Mark,
            schema: output_schema,
        }),
        DependentJoinKind::In { needle } => {
            let field = right.schema().arrow().field(0);
            let mut right_value = BoundExpr::column(0, field.data_type().clone(), field.name());
            if right_value.data_type != needle.data_type {
                right_value = cast_to(right_value, needle.data_type.clone());
            }
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                residual: guard,
                null_aware: Some((needle, right_value)),
                join_type: JoinType::Mark,
                schema: output_schema,
            })
        }
        DependentJoinKind::ExistsFilter { negated } => Ok(LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            on,
            residual: guard,
            null_aware: None,
            join_type: if negated {
                JoinType::Anti
            } else {
                JoinType::Semi
            },
            schema: output_schema,
        }),
        DependentJoinKind::InFilter { needle, negated } => {
            let field = right.schema().arrow().field(0);
            let mut right_value = BoundExpr::column(0, field.data_type().clone(), field.name());
            if right_value.data_type != needle.data_type {
                right_value = cast_to(right_value, needle.data_type.clone());
            }
            let membership = (needle, right_value);
            let (join_type, null_aware) = if negated {
                (JoinType::NullAwareAnti, Some(membership))
            } else {
                on.push(membership);
                (JoinType::Semi, None)
            };
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                residual: guard,
                null_aware,
                join_type,
                schema: output_schema,
            })
        }
    }
}

fn canonical_pair(raw: BoundExpr) -> Result<(BoundExpr, BoundExpr)> {
    let is_null = BoundExpr {
        kind: ExprKind::IsNull {
            expr: Box::new(raw.clone()),
            negated: false,
        },
        data_type: DataType::Boolean,
        display_name: format!("{} IS NULL", raw.display_name),
    };
    let default = default_value(&raw.data_type)?;
    let value = BoundExpr {
        kind: ExprKind::Case {
            when_then: vec![(is_null.clone(), default)],
            else_expr: Box::new(raw.clone()),
        },
        data_type: raw.data_type.clone(),
        display_name: format!("__rustdb_domain_value_{}", raw.display_name),
    };
    Ok((is_null, value))
}

fn default_value(data_type: &DataType) -> Result<BoundExpr> {
    let value = match data_type {
        DataType::Boolean => BoundExpr::literal(ScalarValue::Boolean(false)),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            cast_to(BoundExpr::literal(ScalarValue::Int64(0)), data_type.clone())
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => cast_to(
            BoundExpr::literal(ScalarValue::UInt64(0)),
            data_type.clone(),
        ),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => cast_to(
            BoundExpr::literal(ScalarValue::Float64(0.0)),
            data_type.clone(),
        ),
        DataType::Decimal128(precision, scale) => BoundExpr::literal(ScalarValue::Decimal128 {
            value: 0,
            precision: *precision,
            scale: *scale,
        }),
        DataType::Utf8 => BoundExpr::literal(ScalarValue::Utf8(String::new())),
        DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => cast_to(
            BoundExpr::literal(ScalarValue::Utf8(String::new())),
            data_type.clone(),
        ),
        DataType::Date32 => BoundExpr::literal(ScalarValue::Date32(0)),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            BoundExpr::literal(ScalarValue::TimestampMicrosecond(0))
        }
        DataType::Timestamp(_, None | Some(_)) => cast_to(
            BoundExpr::literal(ScalarValue::TimestampMicrosecond(0)),
            data_type.clone(),
        ),
        DataType::Interval(IntervalUnit::YearMonth) => {
            BoundExpr::literal(ScalarValue::MonthInterval(0))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            BoundExpr::literal(ScalarValue::DayInterval(0))
        }
        other => {
            return Err(Error::Unsupported(format!(
                "correlation domain does not support parameter type {other}"
            )));
        }
    };
    Ok(value)
}

fn cast_to(expr: BoundExpr, data_type: DataType) -> BoundExpr {
    if expr.data_type == data_type {
        return expr;
    }
    let display_name = format!("CAST({} AS {data_type})", expr.display_name);
    BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
        },
        data_type,
        display_name,
    }
}

fn gate_aggregate_argument(expression: BoundExpr, sentinel_index: usize) -> BoundExpr {
    let sentinel = BoundExpr::column(sentinel_index, DataType::Int64, "__rustdb_inner_match");
    let missing = BoundExpr {
        kind: ExprKind::IsNull {
            expr: Box::new(sentinel),
            negated: false,
        },
        data_type: DataType::Boolean,
        display_name: "__rustdb_inner_match IS NULL".into(),
    };
    let null = cast_to(
        BoundExpr::literal(ScalarValue::Null),
        expression.data_type.clone(),
    );
    BoundExpr {
        kind: ExprKind::Case {
            when_then: vec![(missing, null)],
            else_expr: Box::new(expression.clone()),
        },
        data_type: expression.data_type,
        display_name: expression.display_name,
    }
}

fn aggregate_schema(groups: &[BoundExpr], aggregates: &[AggregateExpr]) -> PlanSchema {
    let fields = groups
        .iter()
        .map(|expression| {
            Field::new(
                expression.display_name.clone(),
                expression.data_type.clone(),
                true,
            )
        })
        .chain(aggregates.iter().map(|aggregate| {
            Field::new(
                aggregate.display_name.clone(),
                aggregate.data_type.clone(),
                true,
            )
        }))
        .collect::<Vec<_>>();
    PlanSchema::unqualified(Arc::new(Schema::new(fields)))
}

fn expression_schema(expressions: &[BoundExpr]) -> PlanSchema {
    PlanSchema::unqualified(Arc::new(Schema::new(
        expressions
            .iter()
            .map(|expression| {
                Field::new(
                    expression.display_name.clone(),
                    expression.data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<_>>(),
    )))
}

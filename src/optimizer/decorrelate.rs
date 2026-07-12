use arrow::datatypes::DataType;

use crate::sql::{
    BoundExpr, DependentJoinKind, ExprKind, JoinType, LogicalPlan, PlanSchema, UnaryOp,
};
use crate::{Error, Result};

mod domain;
mod pull;
mod rewrite;

use pull::{Correlation, pull};
use rewrite::{combine_and, outer_to_columns, right_residual_to_join};

pub(super) fn apply(plan: LogicalPlan) -> Result<LogicalPlan> {
    let plan = rewrite_plan(plan)?;
    lower_mark_filters(plan)
}

fn rewrite_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => plan,
        LogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => LogicalPlan::Filter {
            input: Box::new(rewrite_plan(*input)?),
            predicate,
            schema,
        },
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => LogicalPlan::Projection {
            input: Box::new(rewrite_plan(*input)?),
            expressions,
            schema,
        },
        LogicalPlan::Scalarize { input, schema } => LogicalPlan::Scalarize {
            input: Box::new(rewrite_plan(*input)?),
            schema,
        },
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(rewrite_plan(*input)?),
            group_exprs,
            aggregate_exprs,
            schema,
        },
        LogicalPlan::Append { inputs, schema } => LogicalPlan::Append {
            inputs: inputs
                .into_iter()
                .map(rewrite_plan)
                .collect::<Result<Vec<_>>>()?,
            schema,
        },
        LogicalPlan::Window {
            input,
            expressions,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(rewrite_plan(*input)?),
            expressions,
            schema,
        },
        LogicalPlan::Sort {
            input,
            expressions,
            fetch,
            schema,
        } => LogicalPlan::Sort {
            input: Box::new(rewrite_plan(*input)?),
            expressions,
            fetch,
            schema,
        },
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            schema,
        } => LogicalPlan::Limit {
            input: Box::new(rewrite_plan(*input)?),
            offset,
            limit,
            schema,
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(rewrite_plan(*left)?),
            right: Box::new(rewrite_plan(*right)?),
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        },
        LogicalPlan::DependentJoin {
            left,
            right,
            kind,
            guard,
            schema,
        } => {
            let left = rewrite_plan(*left)?;
            let right = rewrite_plan(*right)?;
            decorrelate(left, right, kind, guard, schema)?
        }
    })
}

fn decorrelate(
    left: LogicalPlan,
    right: LogicalPlan,
    kind: DependentJoinKind,
    guard: Option<BoundExpr>,
    output_schema: PlanSchema,
) -> Result<LogicalPlan> {
    if domain::is_aggregate(&right) {
        return domain::rewrite(left, right, kind, guard, output_schema);
    }
    let pulled = pull(right)?;
    let mut on = Vec::new();
    let mut residuals = Vec::new();
    let left_width = left.schema().arrow().fields().len();
    for correlation in pulled.correlations {
        match correlation {
            Correlation::Key { mut outer, inner } => {
                outer_to_columns(&mut outer)?;
                on.push((outer, inner));
            }
            Correlation::Residual(mut residual) => {
                right_residual_to_join(&mut residual, left_width)?;
                residuals.push(residual);
            }
        }
    }
    // DuckDB still enforces per-key scalar cardinality for a correlated
    // subquery in an inactive CASE/AND/OR branch. Guard only the delayed
    // projection; do not remove candidate rows from LeftSingle.
    if let Some(guard) = &guard
        && matches!(
            &kind,
            DependentJoinKind::Exists | DependentJoinKind::In { .. }
        )
    {
        residuals.push(guard.clone());
    }
    if on.is_empty() {
        return Err(Error::Unsupported(
            "correlated subqueries require at least one outer-to-inner equality key".into(),
        ));
    }
    let residual = combine_and(residuals)?;
    match kind {
        DependentJoinKind::Scalar => {
            let result_index = pulled.old_to_new.first().copied().ok_or_else(|| {
                Error::Internal("correlated subquery lost its first output column".into())
            })?;
            scalar_join(
                left,
                pulled.plan,
                on,
                residual,
                result_index,
                pulled.scalar_aggregate,
                guard,
                output_schema,
            )
        }
        DependentJoinKind::GuardedScalar { mut expression } => {
            let guard = guard.ok_or_else(|| {
                Error::Internal("guarded scalar subquery is missing its guard".into())
            })?;
            rewrite::remap_columns(&mut expression, &pulled.old_to_new)?;
            guarded_scalar_join(
                left,
                pulled.plan,
                on,
                residual,
                expression,
                guard,
                output_schema,
            )
        }
        DependentJoinKind::Exists => Ok(LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(pulled.plan),
            on,
            null_equal_keys: false,
            residual,
            null_aware: None,
            join_type: JoinType::Mark,
            schema: output_schema,
        }),
        DependentJoinKind::In { needle } => {
            let result_index = pulled.old_to_new.first().copied().ok_or_else(|| {
                Error::Internal("correlated subquery lost its first output column".into())
            })?;
            let field = pulled.plan.schema().arrow().field(result_index);
            let mut right_value = BoundExpr::column(
                result_index,
                field.data_type().clone(),
                field.name().clone(),
            );
            if right_value.data_type != needle.data_type {
                right_value = cast_to(right_value, needle.data_type.clone());
            }
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(pulled.plan),
                on,
                null_equal_keys: false,
                residual,
                null_aware: Some((needle, right_value)),
                join_type: JoinType::Mark,
                schema: output_schema,
            })
        }
        DependentJoinKind::ExistsFilter { negated } => Ok(LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(pulled.plan),
            on,
            null_equal_keys: false,
            residual,
            null_aware: None,
            join_type: if negated {
                JoinType::Anti
            } else {
                JoinType::Semi
            },
            schema: output_schema,
        }),
        DependentJoinKind::InFilter { needle, negated } => {
            let result_index = pulled.old_to_new.first().copied().ok_or_else(|| {
                Error::Internal("correlated subquery lost its first output column".into())
            })?;
            let field = pulled.plan.schema().arrow().field(result_index);
            let mut right_value = BoundExpr::column(
                result_index,
                field.data_type().clone(),
                field.name().clone(),
            );
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
                right: Box::new(pulled.plan),
                on,
                null_equal_keys: false,
                residual,
                null_aware,
                join_type,
                schema: output_schema,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn guarded_scalar_join(
    left: LogicalPlan,
    right: LogicalPlan,
    on: Vec<(BoundExpr, BoundExpr)>,
    residual: Option<BoundExpr>,
    mut expression: BoundExpr,
    guard: BoundExpr,
    output_schema: PlanSchema,
) -> Result<LogicalPlan> {
    let left_width = left.schema().arrow().fields().len();
    crate::sql::scalar_subquery::guarded::shift_columns(&mut expression, left_width)?;
    let schema = PlanSchema::left_join(left.schema(), right.schema());
    let join = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        on,
        null_equal_keys: false,
        residual,
        null_aware: None,
        join_type: JoinType::LeftSingle,
        schema,
    };
    crate::sql::scalar_subquery::guarded::project(join, expression, guard, output_schema)
}

#[allow(clippy::too_many_arguments)]
fn scalar_join(
    left: LogicalPlan,
    right: LogicalPlan,
    on: Vec<(BoundExpr, BoundExpr)>,
    residual: Option<BoundExpr>,
    result_index: usize,
    scalar_aggregate: bool,
    guard: Option<BoundExpr>,
    output_schema: PlanSchema,
) -> Result<LogicalPlan> {
    let left_width = left.schema().arrow().fields().len();
    let field = right.schema().arrow().field(result_index);
    let result_type = field.data_type().clone();
    let result_name = field.name().clone();
    let residual = if scalar_aggregate {
        combine_and(residual.into_iter().chain(guard.clone()).collect())?
    } else {
        residual
    };
    let join_schema = PlanSchema::left_join(left.schema(), right.schema());
    let join = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        on,
        null_equal_keys: false,
        residual,
        null_aware: None,
        join_type: if scalar_aggregate {
            JoinType::Left
        } else {
            JoinType::LeftSingle
        },
        schema: join_schema,
    };
    let mut expressions = output_schema
        .arrow()
        .fields()
        .iter()
        .take(left_width)
        .enumerate()
        .map(|(index, field)| {
            BoundExpr::column(index, field.data_type().clone(), field.name().clone())
        })
        .collect::<Vec<_>>();
    let mut result = BoundExpr::column(left_width + result_index, result_type, result_name);
    let output = output_schema.arrow().field(left_width);
    result.display_name = output.name().clone();
    if let Some(guard) = guard {
        return crate::sql::scalar_subquery::guarded::project(join, result, guard, output_schema);
    }
    expressions.push(result);
    Ok(LogicalPlan::Projection {
        input: Box::new(join),
        expressions,
        schema: output_schema,
    })
}

fn lower_mark_filters(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Filter {
            input,
            predicate,
            schema: _,
        } => {
            let input = lower_mark_filters(*input)?;
            if let Some(negated) = marker_predicate(&predicate, &input) {
                lower_marker(input, negated)?
            } else {
                let schema = input.schema().clone();
                LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                    schema,
                }
            }
        }
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => LogicalPlan::Projection {
            input: Box::new(lower_mark_filters(*input)?),
            expressions,
            schema,
        },
        LogicalPlan::Scalarize { input, schema } => LogicalPlan::Scalarize {
            input: Box::new(lower_mark_filters(*input)?),
            schema,
        },
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(lower_mark_filters(*input)?),
            group_exprs,
            aggregate_exprs,
            schema,
        },
        LogicalPlan::Append { inputs, schema } => LogicalPlan::Append {
            inputs: inputs
                .into_iter()
                .map(lower_mark_filters)
                .collect::<Result<Vec<_>>>()?,
            schema,
        },
        LogicalPlan::Window {
            input,
            expressions,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(lower_mark_filters(*input)?),
            expressions,
            schema,
        },
        LogicalPlan::Sort {
            input,
            expressions,
            fetch,
            schema,
        } => LogicalPlan::Sort {
            input: Box::new(lower_mark_filters(*input)?),
            expressions,
            fetch,
            schema,
        },
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            schema,
        } => LogicalPlan::Limit {
            input: Box::new(lower_mark_filters(*input)?),
            offset,
            limit,
            schema,
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(lower_mark_filters(*left)?),
            right: Box::new(lower_mark_filters(*right)?),
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        },
        LogicalPlan::DependentJoin { .. } => {
            return Err(Error::Internal(
                "DependentJoin remained after decorrelation".into(),
            ));
        }
        LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. } => plan,
    })
}

fn marker_predicate(predicate: &BoundExpr, input: &LogicalPlan) -> Option<bool> {
    let LogicalPlan::Join {
        left,
        join_type: JoinType::Mark,
        ..
    } = input
    else {
        return None;
    };
    let marker = left.schema().arrow().fields().len();
    match &predicate.kind {
        ExprKind::Column(index) if *index == marker => Some(false),
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } if matches!(expr.kind, ExprKind::Column(index) if index == marker) => Some(true),
        _ => None,
    }
}

fn lower_marker(plan: LogicalPlan, negated: bool) -> Result<LogicalPlan> {
    let LogicalPlan::Join {
        left,
        right,
        mut on,
        residual,
        null_aware,
        join_type: JoinType::Mark,
        ..
    } = plan
    else {
        return Err(Error::Internal(
            "marker lowering received a non-Mark join".into(),
        ));
    };
    let schema = left.schema().clone();
    let (join_type, null_aware) = match (negated, null_aware) {
        (false, Some(pair)) => {
            on.push(pair);
            (JoinType::Semi, None)
        }
        (false, None) => (JoinType::Semi, None),
        (true, Some(pair)) => (JoinType::NullAwareAnti, Some(pair)),
        (true, None) => (JoinType::Anti, None),
    };
    Ok(LogicalPlan::Join {
        left,
        right,
        on,
        null_equal_keys: false,
        residual,
        null_aware,
        join_type,
        schema,
    })
}

fn cast_to(expr: BoundExpr, data_type: DataType) -> BoundExpr {
    let display_name = format!("CAST({} AS {data_type})", expr.display_name);
    BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
        },
        data_type,
        display_name,
    }
}

use sqlparser::ast::{BinaryOperator, Expr};

use crate::sql::{BoundExpr, ExprKind, JoinType, LogicalPlan, PlanSchema, ScalarValue};
use crate::{Error, Result};

pub(super) fn bind(
    guard: Option<&Expr>,
    schema: &PlanSchema,
    aggregate_groups: Option<&[Expr]>,
) -> Result<Option<BoundExpr>> {
    guard
        .map(|guard| match crate::sql::binder::bind_expr(guard, schema) {
            Ok(bound) => Ok(bound),
            Err(_) if crate::sql::aggregate::contains_aggregate(guard) => {
                let groups = aggregate_groups.ok_or_else(|| {
                    Error::Internal(
                        "aggregate short-circuit guard was found outside an aggregate query".into(),
                    )
                })?;
                crate::sql::aggregate::bind_deferred_result(guard, schema, groups)
            }
            Err(error) => Err(error),
        })
        .transpose()
}

pub(super) fn and(existing: Option<Expr>, condition: Expr) -> Option<Expr> {
    Some(match existing {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(condition),
        },
        None => condition,
    })
}

pub(super) fn case_branch_selected(operand: Option<&Expr>, condition: &Expr) -> Expr {
    match operand {
        Some(operand) => Expr::BinaryOp {
            left: Box::new(operand.clone()),
            op: BinaryOperator::Eq,
            right: Box::new(condition.clone()),
        },
        None => condition.clone(),
    }
}

pub(super) fn split_projection(plan: LogicalPlan) -> (LogicalPlan, Option<BoundExpr>) {
    match plan {
        LogicalPlan::Projection {
            input,
            mut expressions,
            schema: _,
        } if expressions.len() == 1 && !is_aggregate_shape(&input) => (*input, expressions.pop()),
        other => (other, None),
    }
}

pub(super) fn join(
    left: LogicalPlan,
    right: LogicalPlan,
    mut expression: BoundExpr,
    guard: BoundExpr,
    output_schema: PlanSchema,
) -> Result<LogicalPlan> {
    let left_width = left.schema().arrow().fields().len();
    let join_schema = PlanSchema::left_join(left.schema(), right.schema());
    let join = LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        on: Vec::new(),
        residual: Some(guard.clone()),
        null_aware: None,
        join_type: JoinType::LeftSingle,
        schema: join_schema,
    };
    shift_columns(&mut expression, left_width)?;
    project(join, expression, guard, output_schema)
}

pub(crate) fn project(
    join: LogicalPlan,
    expression: BoundExpr,
    guard: BoundExpr,
    output_schema: PlanSchema,
) -> Result<LogicalPlan> {
    let left_width = output_schema.arrow().fields().len().saturating_sub(1);
    let mut expressions = (0..left_width)
        .map(|index| {
            let field = output_schema.arrow().field(index);
            BoundExpr::column(index, field.data_type().clone(), field.name().clone())
        })
        .collect::<Vec<_>>();
    let mut result = guarded_result(guard, expression);
    result.display_name = output_schema.arrow().field(left_width).name().clone();
    expressions.push(result);
    Ok(LogicalPlan::Projection {
        input: Box::new(join),
        expressions,
        schema: output_schema,
    })
}

pub(crate) fn shift_columns(expr: &mut BoundExpr, offset: usize) -> Result<()> {
    match &mut expr.kind {
        ExprKind::Column(index) => {
            *index = index
                .checked_add(offset)
                .ok_or_else(|| Error::Internal("guarded scalar column offset overflow".into()))?;
        }
        ExprKind::OuterRef { .. } => {
            return Err(Error::Unsupported(
                "an outer reference in a correlated subquery SELECT list is not supported".into(),
            ));
        }
        ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => {
            return Err(Error::Internal(
                "deferred aggregate result reached guarded scalar projection".into(),
            ));
        }
        ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. }
        | ExprKind::Like {
            expr: left,
            pattern: right,
            ..
        } => {
            shift_columns(left, offset)?;
            shift_columns(right, offset)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            shift_columns(expr, offset)?
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                shift_columns(when, offset)?;
                shift_columns(then, offset)?;
            }
            shift_columns(else_expr, offset)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for argument in args {
                shift_columns(argument, offset)?;
            }
        }
    }
    Ok(())
}

fn guarded_result(guard: BoundExpr, expression: BoundExpr) -> BoundExpr {
    let data_type = expression.data_type.clone();
    let null = BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(BoundExpr::literal(ScalarValue::Null)),
        },
        data_type: data_type.clone(),
        display_name: format!("CAST(NULL AS {data_type})"),
    };
    BoundExpr {
        kind: ExprKind::Case {
            when_then: vec![(guard, expression)],
            else_expr: Box::new(null),
        },
        data_type,
        display_name: "guarded scalar subquery".into(),
    }
}

fn is_aggregate_shape(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Aggregate { .. })
        || matches!(plan, LogicalPlan::Filter { input, .. } if matches!(input.as_ref(), LogicalPlan::Aggregate { .. }))
}

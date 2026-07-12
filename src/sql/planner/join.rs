use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use sqlparser::ast::{BinaryOperator, Expr, Ident, JoinConstraint};

use crate::{Error, Result};

use super::super::{
    BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan, PlanSchema, ScalarFunction,
    binder::{bind_expr, ensure_boolean, make_binary},
    coercion::cast_if_needed,
};

pub(super) struct UsingColumn {
    pub(super) name: Ident,
    pub(super) left_index: usize,
    pub(super) right_index: usize,
    left_expr: BoundExpr,
    right_expr: BoundExpr,
}

pub(super) struct JoinBinding {
    pub(super) keys: Vec<(BoundExpr, BoundExpr)>,
    pub(super) residual: Option<BoundExpr>,
    pub(super) using_columns: Vec<UsingColumn>,
}

pub(super) fn bind_join_constraint(
    constraint: &JoinConstraint,
    left: &PlanSchema,
    right: &PlanSchema,
) -> Result<JoinBinding> {
    match constraint {
        JoinConstraint::On(expr) => {
            let combined = PlanSchema::join(left, right);
            let mut binding = JoinBinding {
                keys: Vec::new(),
                residual: None,
                using_columns: Vec::new(),
            };
            bind_join_on(expr, &combined, left.arrow().fields().len(), &mut binding)?;
            Ok(binding)
        }
        JoinConstraint::Using(names) => bind_using(names, left, right),
        JoinConstraint::Natural => Err(Error::Unsupported(
            "NATURAL JOIN is not supported; list the key columns with JOIN ... USING".into(),
        )),
        JoinConstraint::None => Err(Error::Unsupported(
            "JOIN requires an ON or USING constraint".into(),
        )),
    }
}

pub(super) fn apply_using_projection(
    input: LogicalPlan,
    join_type: JoinType,
    left: &PlanSchema,
    right: &PlanSchema,
    using_columns: &[UsingColumn],
    keys: &[(BoundExpr, BoundExpr)],
) -> Result<LogicalPlan> {
    if using_columns.is_empty() {
        return Ok(input);
    }
    let left_width = left.arrow().fields().len();
    let mut expressions = Vec::with_capacity(
        using_columns
            .len()
            .saturating_add(left_width)
            .saturating_add(right.arrow().fields().len()),
    );
    let mut fields = Vec::with_capacity(expressions.capacity());
    let mut qualifiers = Vec::with_capacity(expressions.capacity());
    let mut visible = Vec::with_capacity(expressions.capacity());

    for (column, (left_key, _)) in using_columns.iter().zip(keys) {
        let mut right_expr = column.right_expr.clone();
        crate::sql::scalar_subquery::guarded::shift_columns(&mut right_expr, left_width)?;
        let mut expression = match join_type {
            JoinType::Inner | JoinType::Left => column.left_expr.clone(),
            JoinType::Right => right_expr,
            JoinType::Full => BoundExpr {
                data_type: left_key.data_type.clone(),
                display_name: column.name.value.clone(),
                kind: ExprKind::ScalarFunction {
                    function: ScalarFunction::Coalesce,
                    args: vec![
                        cast_if_needed(column.left_expr.clone(), &left_key.data_type),
                        cast_if_needed(right_expr, &left_key.data_type),
                    ],
                },
            },
            _ => unreachable!("USING is only exposed for relational joins"),
        };
        expression.display_name = column.name.value.clone();
        let nullable = match join_type {
            JoinType::Inner | JoinType::Left => left.arrow().field(column.left_index).is_nullable(),
            JoinType::Right => right.arrow().field(column.right_index).is_nullable(),
            JoinType::Full => true,
            _ => unreachable!("USING is only exposed for relational joins"),
        };
        fields.push(Field::new(
            column.name.value.clone(),
            expression.data_type.clone(),
            nullable,
        ));
        qualifiers.push(None);
        visible.push(true);
        expressions.push(expression);
    }

    append_side_columns(
        &mut expressions,
        &mut fields,
        &mut qualifiers,
        &mut visible,
        left,
        input.schema(),
        using_columns,
        true,
    )?;
    append_side_columns(
        &mut expressions,
        &mut fields,
        &mut qualifiers,
        &mut visible,
        right,
        input.schema(),
        using_columns,
        false,
    )?;

    let schema =
        PlanSchema::new_with_visibility(Arc::new(Schema::new(fields)), qualifiers, visible);
    Ok(LogicalPlan::Projection {
        input: Box::new(input),
        expressions,
        schema,
    })
}

#[allow(clippy::too_many_arguments)]
fn append_side_columns(
    expressions: &mut Vec<BoundExpr>,
    fields: &mut Vec<Field>,
    qualifiers: &mut Vec<Option<String>>,
    visible: &mut Vec<bool>,
    source: &PlanSchema,
    joined: &PlanSchema,
    using_columns: &[UsingColumn],
    left: bool,
) -> Result<()> {
    let offset = if left {
        0
    } else {
        joined.arrow().fields().len() - source.arrow().fields().len()
    };
    for (index, field) in source.arrow().fields().iter().enumerate() {
        let key = using_columns.iter().position(|column| {
            if left {
                column.left_index == index
            } else {
                column.right_index == index
            }
        });
        let output_index = offset.saturating_add(index);
        if let Some(key) = key {
            let Some(qualifier) = source.qualifier(index) else {
                continue;
            };
            let mut expression = if left {
                using_columns[key].left_expr.clone()
            } else {
                let mut expression = using_columns[key].right_expr.clone();
                crate::sql::scalar_subquery::guarded::shift_columns(&mut expression, offset)?;
                expression
            };
            expression.display_name = field.name().clone();
            fields.push(Field::new(
                field.name(),
                expression.data_type.clone(),
                joined.arrow().field(output_index).is_nullable(),
            ));
            qualifiers.push(Some(qualifier.to_owned()));
            visible.push(false);
            expressions.push(expression);
        } else {
            expressions.push(BoundExpr::column(
                output_index,
                field.data_type().clone(),
                field.name(),
            ));
            fields.push(joined.arrow().field(output_index).as_ref().clone());
            qualifiers.push(source.qualifier(index).map(str::to_owned));
            visible.push(source.is_visible(index));
        }
    }
    Ok(())
}

fn bind_using(
    names: &[sqlparser::ast::ObjectName],
    left: &PlanSchema,
    right: &PlanSchema,
) -> Result<JoinBinding> {
    if names.is_empty() {
        return Err(Error::InvalidArgument(
            "JOIN ... USING requires at least one column".into(),
        ));
    }
    let mut binding = JoinBinding {
        keys: Vec::with_capacity(names.len()),
        residual: None,
        using_columns: Vec::with_capacity(names.len()),
    };
    for name in names {
        let [part] = name.0.as_slice() else {
            return Err(Error::InvalidArgument(format!(
                "JOIN ... USING column `{name}` must be an unqualified identifier"
            )));
        };
        let ident = part.as_ident().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "JOIN ... USING column `{name}` must be a plain identifier"
            ))
        })?;
        if binding
            .using_columns
            .iter()
            .any(|column| column.name.value.eq_ignore_ascii_case(&ident.value))
        {
            return Err(Error::InvalidArgument(format!(
                "JOIN ... USING column `{ident}` is specified more than once"
            )));
        }
        let left_expr = bind_expr(&Expr::Identifier(ident.clone()), left)?;
        let right_expr = bind_expr(&Expr::Identifier(ident.clone()), right)?;
        let (left_index, right_index) = match (&left_expr.kind, &right_expr.kind) {
            (ExprKind::Column(left), ExprKind::Column(right)) => (*left, *right),
            _ => {
                return Err(Error::Internal(
                    "JOIN ... USING did not bind to direct input columns".into(),
                ));
            }
        };
        let equality = make_binary(left_expr.clone(), BinaryOp::Eq, right_expr.clone())?;
        let ExprKind::Binary { left, right, .. } = equality.kind else {
            unreachable!("equality binder must produce a binary expression")
        };
        binding.keys.push((*left, *right));
        binding.using_columns.push(UsingColumn {
            name: ident.clone(),
            left_index,
            right_index,
            left_expr,
            right_expr,
        });
    }
    Ok(binding)
}

fn bind_join_on(
    expr: &Expr,
    schema: &PlanSchema,
    left_width: usize,
    binding: &mut JoinBinding,
) -> Result<()> {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        bind_join_on(left, schema, left_width, binding)?;
        return bind_join_on(right, schema, left_width, binding);
    }

    let bound = bind_expr(expr, schema)?;
    ensure_boolean(&bound).map_err(|_| {
        Error::InvalidArgument(format!(
            "join predicate `{expr}` requires BOOLEAN, got {}",
            bound.data_type
        ))
    })?;
    if let ExprKind::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = &bound.kind
        && let (Ok(left_side), Ok(right_side)) = (
            expression_side(left, left_width),
            expression_side(right, left_width),
        )
    {
        match (left_side, right_side) {
            (JoinExprSide::Left, JoinExprSide::Right) => {
                binding.keys.push((
                    (**left).clone(),
                    rebase_right((**right).clone(), left_width)?,
                ));
                return Ok(());
            }
            (JoinExprSide::Right, JoinExprSide::Left) => {
                binding.keys.push((
                    (**right).clone(),
                    rebase_right((**left).clone(), left_width)?,
                ));
                return Ok(());
            }
            _ => {}
        }
    }
    binding.residual = Some(match binding.residual.take() {
        None => bound,
        Some(previous) => make_binary(previous, BinaryOp::And, bound)?,
    });
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum JoinExprSide {
    Left,
    Right,
}

fn expression_side(expr: &BoundExpr, left_width: usize) -> Result<JoinExprSide> {
    let mut columns = Vec::new();
    expr.referenced_columns(&mut columns);
    if columns.is_empty() {
        return Err(Error::Unsupported(
            "constant join keys are not supported".into(),
        ));
    }
    if columns.iter().all(|index| *index < left_width) {
        Ok(JoinExprSide::Left)
    } else if columns.iter().all(|index| *index >= left_width) {
        Ok(JoinExprSide::Right)
    } else {
        Err(Error::Unsupported(
            "a join key cannot reference both inputs".into(),
        ))
    }
}

pub(super) fn rebase_right(mut expr: BoundExpr, left_width: usize) -> Result<BoundExpr> {
    match &mut expr.kind {
        ExprKind::Column(index) => {
            *index = index.checked_sub(left_width).ok_or_else(|| {
                Error::Internal("right join expression used a left column".into())
            })?;
        }
        ExprKind::OuterRef { .. } => {
            return Err(Error::Internal(
                "OuterRef reached parser-visible JOIN rebasing".into(),
            ));
        }
        ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => {
            return Err(Error::Internal(
                "deferred aggregate result reached parser-visible JOIN rebasing".into(),
            ));
        }
        ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            **left = rebase_right((**left).clone(), left_width)?;
            **right = rebase_right((**right).clone(), left_width)?;
        }
        ExprKind::Like { expr, pattern, .. } => {
            **expr = rebase_right((**expr).clone(), left_width)?;
            **pattern = rebase_right((**pattern).clone(), left_width)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                *when = rebase_right(when.clone(), left_width)?;
                *then = rebase_right(then.clone(), left_width)?;
            }
            **else_expr = rebase_right((**else_expr).clone(), left_width)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            **expr = rebase_right((**expr).clone(), left_width)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for arg in args {
                *arg = rebase_right(arg.clone(), left_width)?;
            }
        }
    }
    Ok(expr)
}

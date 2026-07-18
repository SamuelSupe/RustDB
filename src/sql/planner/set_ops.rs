use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use sqlparser::ast::{Expr, OrderBy, OrderByKind, SetExpr, SetOperator, SetQuantifier, Spanned};

use crate::{Error, Result};

use super::super::{BoundExpr, JoinType, LogicalPlan, PlanSchema, SortExpr};
use super::{Planner, output_column};
use crate::sql::{
    binder::bind_expr, coercion::cast_if_needed, name_resolution::source_location_suffix,
    relation::CteScope,
};

mod multiset;
mod types;
mod values;

use multiset::plan_multiset_operation;
use types::{common_set_type, is_nested, supports_distinct_key};
use values::plan_values;

impl Planner<'_> {
    pub(super) fn plan_set_expr(
        &self,
        expression: &SetExpr,
        ctes: &CteScope,
        outer: Option<&PlanSchema>,
    ) -> Result<LogicalPlan> {
        match expression {
            SetExpr::Select(select) => self.plan_select(select, ctes, &[], outer),
            SetExpr::Query(query) => self.plan_query_with_outer(query, ctes, outer),
            SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                let left = self.plan_set_expr(left, ctes, outer)?;
                let right = self.plan_set_expr(right, ctes, outer)?;
                plan_set_operation(left, right, *op, *set_quantifier)
            }
            SetExpr::Values(values) => plan_values(values),
            SetExpr::Table(_) => Err(Error::Unsupported(
                "TABLE query bodies are not supported".into(),
            )),
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => {
                Err(Error::Unsupported(
                    "write statements are not supported by the read-only engine".into(),
                ))
            }
        }
    }
}

pub(super) fn apply_set_order(
    mut plan: LogicalPlan,
    order_by: Option<&OrderBy>,
    limit: Option<usize>,
    offset: usize,
) -> Result<LogicalPlan> {
    let Some(order_by) = order_by else {
        return Ok(plan);
    };
    let OrderByKind::Expressions(items) = &order_by.kind else {
        return Err(Error::Unsupported("ORDER BY ALL is not supported".into()));
    };
    if items.is_empty() {
        return Ok(plan);
    }

    let width = plan.schema().arrow().fields().len();
    let expressions = items
        .iter()
        .map(|item| {
            if item.with_fill.is_some() {
                return Err(Error::Unsupported(
                    "ORDER BY WITH FILL is not supported".into(),
                ));
            }
            let expression = if let Some(position) =
                super::super::name_resolution::ordinal(&item.expr, "ORDER BY")?
            {
                output_column(
                    plan.schema(),
                    position,
                    width,
                    "ORDER BY",
                    item.expr.span(),
                )?
            } else {
                if !matches!(&item.expr, Expr::Identifier(_)) {
                    return Err(Error::InvalidArgument(format!(
                        "ORDER BY on a set operation must reference an output column name or position ({}){}",
                        item.expr,
                        source_location_suffix(item.expr.span())
                    )));
                }
                bind_expr(&item.expr, plan.schema()).map_err(|error| match error {
                    Error::Catalog(message) => Error::InvalidArgument(format!(
                        "ORDER BY on a set operation must reference an output column ({message}){}",
                        source_location_suffix(item.expr.span())
                    )),
                    other => other,
                })?
            };
            Ok(SortExpr {
                expr: expression,
                descending: item.options.asc == Some(false),
                nulls_first: item.options.nulls_first.unwrap_or(false),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = plan.schema().clone();
    plan = LogicalPlan::Sort {
        input: Box::new(plan),
        expressions,
        fetch: limit.map(|limit| limit.saturating_add(offset)),
        schema,
    };
    Ok(plan)
}

fn plan_set_operation(
    left: LogicalPlan,
    right: LogicalPlan,
    operator: SetOperator,
    quantifier: SetQuantifier,
) -> Result<LogicalPlan> {
    reject_by_name(quantifier)?;
    let distinct = match (operator, quantifier) {
        (SetOperator::Union, SetQuantifier::All) => false,
        (SetOperator::Union, SetQuantifier::Distinct | SetQuantifier::None) => true,
        (SetOperator::Intersect | SetOperator::Except, SetQuantifier::All) => false,
        (
            SetOperator::Intersect | SetOperator::Except,
            SetQuantifier::Distinct | SetQuantifier::None,
        ) => true,
        (SetOperator::Minus, _) => {
            return Err(Error::Unsupported(
                "MINUS is not supported; use EXCEPT".into(),
            ));
        }
        (_, SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName) => {
            unreachable!("BY NAME quantifiers were rejected above")
        }
    };

    let (left, right, schema) = align_inputs(left, right)?;
    if distinct || matches!(operator, SetOperator::Intersect | SetOperator::Except) {
        validate_distinct_schema(&schema)?;
    }
    match operator {
        SetOperator::Union => {
            let append = LogicalPlan::Append {
                inputs: vec![left, right],
                schema,
            };
            Ok(if distinct {
                deduplicate(append)
            } else {
                append
            })
        }
        SetOperator::Intersect | SetOperator::Except if quantifier == SetQuantifier::All => {
            plan_multiset_operation(left, right, schema, operator)
        }
        SetOperator::Intersect | SetOperator::Except => {
            let left = deduplicate(left);
            let right = deduplicate(right);
            let on = set_keys(left.schema(), right.schema());
            let schema = left.schema().clone();
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                residual: None,
                null_aware: None,
                null_equal_keys: true,
                join_type: if operator == SetOperator::Intersect {
                    JoinType::Semi
                } else {
                    JoinType::Anti
                },
                schema,
            })
        }
        SetOperator::Minus => unreachable!("MINUS was rejected above"),
    }
}

fn reject_by_name(quantifier: SetQuantifier) -> Result<()> {
    if matches!(
        quantifier,
        SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
    ) {
        return Err(Error::Unsupported(
            "UNION/INTERSECT/EXCEPT BY NAME is not supported".into(),
        ));
    }
    Ok(())
}

fn align_inputs(
    left: LogicalPlan,
    right: LogicalPlan,
) -> Result<(LogicalPlan, LogicalPlan, PlanSchema)> {
    let left_fields = left.schema().arrow().fields();
    let right_fields = right.schema().arrow().fields();
    if left_fields.len() != right_fields.len() {
        return Err(Error::InvalidArgument(format!(
            "set operation column count mismatch: left has {} columns but right has {}",
            left_fields.len(),
            right_fields.len()
        )));
    }

    let fields = left_fields
        .iter()
        .zip(right_fields)
        .enumerate()
        .map(|(index, (left, right))| {
            let data_type =
                common_set_type(left.data_type(), right.data_type()).map_err(|reason| {
                    Error::InvalidArgument(format!(
                        "set operation column {} ('{}') has incompatible types {} and {}: {reason}",
                        index + 1,
                        left.name(),
                        left.data_type(),
                        right.data_type()
                    ))
                })?;
            Ok(Field::new(
                left.name(),
                data_type,
                left.is_nullable() || right.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = PlanSchema::unqualified(Arc::new(Schema::new(fields)));
    let left = align_input(left, &schema);
    let right = align_input(right, &schema);
    Ok((left, right, schema))
}

fn align_input(input: LogicalPlan, schema: &PlanSchema) -> LogicalPlan {
    let expressions = input
        .schema()
        .arrow()
        .fields()
        .iter()
        .zip(schema.arrow().fields())
        .enumerate()
        .map(|(index, (source, target))| {
            cast_if_needed(
                BoundExpr::column(index, source.data_type().clone(), target.name()),
                target.data_type(),
            )
        })
        .collect();
    LogicalPlan::Projection {
        input: Box::new(input),
        expressions,
        schema: schema.clone(),
    }
}

fn deduplicate(input: LogicalPlan) -> LogicalPlan {
    let schema = input.schema().clone();
    let group_exprs = schema
        .arrow()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| BoundExpr::column(index, field.data_type().clone(), field.name()))
        .collect();
    LogicalPlan::Aggregate {
        input: Box::new(input),
        group_exprs,
        aggregate_exprs: Vec::new(),
        schema,
    }
}

fn set_keys(left: &PlanSchema, right: &PlanSchema) -> Vec<(BoundExpr, BoundExpr)> {
    left.arrow()
        .fields()
        .iter()
        .zip(right.arrow().fields())
        .enumerate()
        .map(|(index, (left, right))| {
            (
                BoundExpr::column(index, left.data_type().clone(), left.name()),
                BoundExpr::column(index, right.data_type().clone(), right.name()),
            )
        })
        .collect()
}

fn validate_distinct_schema(schema: &PlanSchema) -> Result<()> {
    for field in schema.arrow().fields() {
        if is_nested(field.data_type()) {
            return Err(Error::Unsupported(format!(
                "nested column '{}' of type {} cannot be used as a DISTINCT set key",
                field.name(),
                field.data_type()
            )));
        }
        if !supports_distinct_key(field.data_type()) {
            return Err(Error::Unsupported(format!(
                "column '{}' of type {} cannot be used as a DISTINCT set key",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

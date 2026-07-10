use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use sqlparser::ast::{
    Distinct, Expr, GroupByExpr, JoinOperator, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, TableFactor, TableWithJoins,
};

use crate::{Error, Result};

use super::super::{
    BoundExpr, JoinType, LogicalPlan, PlanSchema,
    aggregate::{is_aggregate_query, plan_aggregate_projection},
    binder::bind_expr,
    relation::{CteScope, alias_plan, cte_name},
    scalar_subquery::extract as extract_scalar_subqueries,
    subquery::apply_where,
};
use super::{
    Planner,
    join::{JoinBinding, bind_join_constraint},
    last_name_part, object_name,
};

impl Planner<'_> {
    pub(super) fn plan_select(&self, select: &Select, ctes: &CteScope) -> Result<LogicalPlan> {
        if select.top.is_some() || select.qualify.is_some() || !select.named_window.is_empty() {
            return Err(Error::Unsupported(
                "TOP, QUALIFY, and window clauses are not supported".into(),
            ));
        }
        let distinct = match &select.distinct {
            None | Some(Distinct::All) => false,
            Some(Distinct::Distinct) => true,
            Some(Distinct::On(_)) => {
                return Err(Error::Unsupported("DISTINCT ON is not supported".into()));
            }
        };

        let mut projection = select.projection.clone();
        let mut selection = select.selection.clone();
        let mut having = select.having.clone();
        let mut group_ast = match &select.group_by {
            GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => {
                expressions.clone()
            }
            GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => {
                return Err(Error::Unsupported(
                    "GROUP BY modifiers and GROUP BY ALL are not supported".into(),
                ));
            }
        };
        let aggregate_query = is_aggregate_query(&group_ast, &projection, having.as_ref());

        let mut plan = self.plan_from(&select.from, ctes)?;
        if let Some(predicate) = &mut selection {
            plan = self.extract_scalars(plan, predicate, None, ctes)?;
            let mut plan_subquery = |query: &Query| self.plan_query_scoped(query, ctes);
            plan = apply_where(plan, predicate, &mut plan_subquery)?;
        }
        for group in &mut group_ast {
            plan = self.extract_scalars(plan, group, None, ctes)?;
        }
        let mut hidden_groups = Vec::new();
        for item in &mut projection {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => continue,
            };
            plan = self.extract_scalars(
                plan,
                expr,
                aggregate_query.then_some(&mut hidden_groups),
                ctes,
            )?;
        }
        if let Some(predicate) = &mut having {
            plan = self.extract_scalars(
                plan,
                predicate,
                aggregate_query.then_some(&mut hidden_groups),
                ctes,
            )?;
        }
        group_ast.extend(hidden_groups);

        if aggregate_query {
            if distinct {
                return Err(Error::Unsupported(
                    "SELECT DISTINCT with aggregates or HAVING is not supported".into(),
                ));
            }
            plan_aggregate_projection(plan, &group_ast, &projection, having.as_ref())
        } else {
            let plan = self.plan_projection(plan, &projection)?;
            if distinct {
                Ok(plan_distinct(plan))
            } else {
                Ok(plan)
            }
        }
    }

    fn plan_from(&self, from: &[TableWithJoins], ctes: &CteScope) -> Result<LogicalPlan> {
        let Some((first, rest)) = from.split_first() else {
            return Ok(LogicalPlan::Empty {
                produce_one_row: true,
                schema: PlanSchema::empty(),
            });
        };
        let mut plan = self.plan_table_with_joins(first, ctes)?;
        for table in rest {
            let right = self.plan_table_with_joins(table, ctes)?;
            let schema = PlanSchema::join(plan.schema(), right.schema());
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                on: Vec::new(),
                join_type: JoinType::Inner,
                schema,
            };
        }
        Ok(plan)
    }

    fn plan_table_with_joins(
        &self,
        table: &TableWithJoins,
        ctes: &CteScope,
    ) -> Result<LogicalPlan> {
        let mut left = self.plan_table(&table.relation, ctes)?;
        for join in &table.joins {
            let mut right = self.plan_table(&join.relation, ctes)?;
            let (join_type, constraint) = match &join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    (JoinType::Inner, constraint)
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (JoinType::Left, constraint)
                }
                other => {
                    return Err(Error::Unsupported(format!(
                        "join type {other:?} is not supported"
                    )));
                }
            };
            let JoinBinding {
                keys: on,
                right_filters,
            } = bind_join_constraint(constraint, left.schema(), right.schema())?;
            if on.is_empty() {
                return Err(Error::Unsupported(
                    "joins require at least one equality key".into(),
                ));
            }
            for predicate in right_filters {
                let schema = right.schema().clone();
                right = LogicalPlan::Filter {
                    input: Box::new(right),
                    predicate,
                    schema,
                };
            }
            let schema = match join_type {
                JoinType::Inner => PlanSchema::join(left.schema(), right.schema()),
                JoinType::Left => PlanSchema::left_join(left.schema(), right.schema()),
                JoinType::Semi | JoinType::Anti => left.schema().clone(),
            };
            left = LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                join_type,
                schema,
            };
        }
        Ok(left)
    }

    fn plan_table(&self, table: &TableFactor, ctes: &CteScope) -> Result<LogicalPlan> {
        match table {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if args.is_some() {
                    return Err(Error::Unsupported(
                        "table functions are not supported".into(),
                    ));
                }
                let table_name = object_name(name);
                let qualifier = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| last_name_part(&table_name).to_owned());
                if let Some(plan) = ctes.get(&cte_name(last_name_part(&table_name))) {
                    return alias_plan(
                        plan.clone(),
                        Some(&qualifier),
                        alias
                            .as_ref()
                            .map(|alias| alias.columns.as_slice())
                            .unwrap_or(&[]),
                    );
                }
                let entry = self
                    .catalog
                    .table(&table_name)
                    .or_else(|| self.catalog.table(last_name_part(&table_name)))
                    .ok_or_else(|| {
                        Error::Catalog(format!("table '{table_name}' does not exist"))
                    })?;
                let provider = Arc::clone(entry.provider());
                let schema = provider.schema();
                let plan_schema = PlanSchema::new(
                    schema,
                    vec![Some(qualifier.clone()); provider.schema().fields().len()],
                );
                let plan = LogicalPlan::Scan {
                    table_name,
                    provider,
                    projection: None,
                    pushed_filter: None,
                    limit: None,
                    schema: plan_schema,
                };
                let column_aliases = alias
                    .as_ref()
                    .map(|alias| alias.columns.as_slice())
                    .unwrap_or(&[]);
                if column_aliases.is_empty() {
                    Ok(plan)
                } else {
                    alias_plan(plan, Some(&qualifier), column_aliases)
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
                sample,
            } => {
                if *lateral {
                    return Err(Error::Unsupported(
                        "correlated LATERAL derived tables are not supported".into(),
                    ));
                }
                if sample.is_some() {
                    return Err(Error::Unsupported(
                        "TABLESAMPLE on derived tables is not supported".into(),
                    ));
                }
                let plan = self
                    .plan_query_scoped(subquery, ctes)
                    .map_err(|error| match error {
                        Error::Catalog(message) => Error::Unsupported(format!(
                            "correlated derived tables are not supported ({message})"
                        )),
                        other => other,
                    })?;
                alias_plan(
                    plan,
                    alias.as_ref().map(|alias| alias.name.value.as_str()),
                    alias
                        .as_ref()
                        .map(|alias| alias.columns.as_slice())
                        .unwrap_or(&[]),
                )
            }
            _ => Err(Error::Unsupported(
                "this table expression is not supported".into(),
            )),
        }
    }

    fn plan_projection(&self, input: LogicalPlan, items: &[SelectItem]) -> Result<LogicalPlan> {
        let expressions = bind_select_items(items, input.schema())?;
        let schema = expression_schema(&expressions);
        Ok(LogicalPlan::Projection {
            input: Box::new(input),
            expressions,
            schema,
        })
    }

    fn extract_scalars(
        &self,
        plan: LogicalPlan,
        expr: &mut Expr,
        hidden_groups: Option<&mut Vec<Expr>>,
        ctes: &CteScope,
    ) -> Result<LogicalPlan> {
        let mut next_name = || {
            let index = self.next_scalar.get();
            self.next_scalar.set(index.saturating_add(1));
            format!("__rustdb_scalar_subquery_{index}")
        };
        let mut plan_subquery = |query: &Query| self.plan_query_scoped(query, ctes);
        extract_scalar_subqueries(
            plan,
            expr,
            hidden_groups,
            &mut next_name,
            &mut plan_subquery,
        )
    }
}

fn bind_select_items(items: &[SelectItem], schema: &PlanSchema) -> Result<Vec<BoundExpr>> {
    let mut output = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => output.push(bind_expr(expr, schema)?),
            SelectItem::ExprWithAlias { expr, alias } => {
                let mut expr = bind_expr(expr, schema)?;
                expr.display_name = alias.value.clone();
                output.push(expr);
            }
            SelectItem::Wildcard(_) => output.extend(
                schema
                    .arrow()
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        BoundExpr::column(index, field.data_type().clone(), field.name())
                    }),
            ),
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _) => {
                bind_qualified_wildcard(name, schema, &mut output)?;
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "select item `{other}` is not supported"
                )));
            }
        }
    }
    Ok(output)
}

fn bind_qualified_wildcard(
    name: &sqlparser::ast::ObjectName,
    schema: &PlanSchema,
    output: &mut Vec<BoundExpr>,
) -> Result<()> {
    let qualifier = object_name(name);
    let start = output.len();
    output.extend(
        schema
            .arrow()
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                schema
                    .qualifier(*index)
                    .is_some_and(|value| value.eq_ignore_ascii_case(last_name_part(&qualifier)))
            })
            .map(|(index, field)| {
                BoundExpr::column(index, field.data_type().clone(), field.name())
            }),
    );
    if output.len() == start {
        return Err(Error::Catalog(format!(
            "relation '{qualifier}' does not exist"
        )));
    }
    Ok(())
}

fn plan_distinct(input: LogicalPlan) -> LogicalPlan {
    let schema = input.schema().clone();
    let group_exprs = schema
        .arrow()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            BoundExpr::column(index, field.data_type().clone(), field.name().clone())
        })
        .collect();
    LogicalPlan::Aggregate {
        input: Box::new(input),
        group_exprs,
        aggregate_exprs: Vec::new(),
        schema,
    }
}

fn expression_schema(expressions: &[BoundExpr]) -> PlanSchema {
    PlanSchema::unqualified(Arc::new(Schema::new(
        expressions
            .iter()
            .map(|expr| Field::new(expr.display_name.clone(), expr.data_type.clone(), true))
            .collect::<Vec<_>>(),
    )))
}

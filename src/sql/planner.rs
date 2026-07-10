use std::{cell::Cell, collections::HashSet};

use sqlparser::{
    ast::{Expr, LimitClause, ObjectName, OrderByKind, Query, SetExpr, Statement, Value},
    dialect::DuckDbDialect,
    parser::Parser,
};

use crate::{Catalog, Error, Result};

use super::{
    BoundExpr, LogicalPlan, PlanSchema, SortExpr, StatementPlan,
    binder::bind_expr,
    relation::{CteScope, alias_plan, cte_name},
};

mod join;
mod select;

pub fn plan_sql(catalog: &Catalog, sql: &str) -> Result<StatementPlan> {
    let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql)?;
    if statements.len() != 1 {
        return Err(Error::InvalidArgument(
            "exactly one SQL statement is required".into(),
        ));
    }
    Planner {
        catalog,
        next_scalar: Cell::new(0),
    }
    .plan_statement(statements.remove(0))
}

struct Planner<'a> {
    catalog: &'a Catalog,
    next_scalar: Cell<usize>,
}

impl Planner<'_> {
    fn plan_statement(&self, statement: Statement) -> Result<StatementPlan> {
        match statement {
            Statement::Query(query) => Ok(StatementPlan::Query(crate::optimizer::optimize(
                self.plan_query(&query)?,
            )?)),
            Statement::Explain {
                analyze, statement, ..
            } => {
                let Statement::Query(query) = *statement else {
                    return Err(Error::Unsupported(
                        "EXPLAIN currently supports SELECT only".into(),
                    ));
                };
                let plan = crate::optimizer::optimize(self.plan_query(&query)?)?;
                Ok(if analyze {
                    StatementPlan::ExplainAnalyze(plan)
                } else {
                    StatementPlan::Explain(plan)
                })
            }
            other => Err(Error::Unsupported(format!(
                "statement `{other}` is not supported by the read-only engine"
            ))),
        }
    }

    fn plan_query(&self, query: &Query) -> Result<LogicalPlan> {
        self.plan_query_scoped(query, &CteScope::new())
    }

    fn plan_query_scoped(&self, query: &Query, inherited_ctes: &CteScope) -> Result<LogicalPlan> {
        if !query.pipe_operators.is_empty() {
            return Err(Error::Unsupported("pipe queries are not supported".into()));
        }
        let mut ctes = inherited_ctes.clone();
        if let Some(with) = &query.with {
            if with.recursive {
                return Err(Error::Unsupported(
                    "recursive CTEs are not supported".into(),
                ));
            }
            let mut local_names = HashSet::new();
            for cte in &with.cte_tables {
                if cte.from.is_some() {
                    return Err(Error::Unsupported(
                        "CTE AS ... FROM is not supported".into(),
                    ));
                }
                let name = cte_name(&cte.alias.name.value);
                if !local_names.insert(name.clone()) {
                    return Err(Error::InvalidArgument(format!(
                        "CTE '{}' is defined more than once",
                        cte.alias.name.value
                    )));
                }
                let plan = self.plan_query_scoped(&cte.query, &ctes)?;
                let plan = alias_plan(plan, Some(&cte.alias.name.value), &cte.alias.columns)?;
                ctes.insert(name, plan);
            }
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Err(Error::Unsupported(
                "set operations and nested query bodies are not supported".into(),
            ));
        };

        let (offset, limit) = query_limit(query)?;
        let mut plan = self.plan_select(select, &ctes)?;
        if let Some(order_by) = &query.order_by {
            let OrderByKind::Expressions(order_exprs) = &order_by.kind else {
                return Err(Error::Unsupported("ORDER BY ALL is not supported".into()));
            };
            let expressions = order_exprs
                .iter()
                .map(|order| {
                    if order.with_fill.is_some() {
                        return Err(Error::Unsupported(
                            "ORDER BY WITH FILL is not supported".into(),
                        ));
                    }
                    let expr = bind_order_expr(&order.expr, plan.schema())?;
                    let descending = order.options.asc == Some(false);
                    Ok(SortExpr {
                        expr,
                        descending,
                        nulls_first: order.options.nulls_first.unwrap_or(descending),
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
        }
        if offset != 0 || limit.is_some() {
            let schema = plan.schema().clone();
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                offset,
                limit,
                schema,
            };
        }
        Ok(plan)
    }
}

fn bind_order_expr(expr: &Expr, schema: &PlanSchema) -> Result<BoundExpr> {
    if let Expr::Value(value) = expr
        && let Value::Number(value, _) = &value.value
        && let Ok(position) = value.parse::<usize>()
        && (1..=schema.arrow().fields().len()).contains(&position)
    {
        let field = schema.arrow().field(position - 1);
        return Ok(BoundExpr::column(
            position - 1,
            field.data_type().clone(),
            field.name(),
        ));
    }
    bind_expr(expr, schema)
}

fn query_limit(query: &Query) -> Result<(usize, Option<usize>)> {
    let (mut offset, mut limit) = match &query.limit_clause {
        None => (0, None),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) if limit_by.is_empty() => (
            offset
                .as_ref()
                .map(|offset| constant_usize(&offset.value, "OFFSET"))
                .transpose()?
                .unwrap_or(0),
            limit
                .as_ref()
                .map(|limit| constant_usize(limit, "LIMIT"))
                .transpose()?,
        ),
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (
            constant_usize(offset, "OFFSET")?,
            Some(constant_usize(limit, "LIMIT")?),
        ),
        Some(_) => {
            return Err(Error::Unsupported("LIMIT BY is not supported".into()));
        }
    };
    if let Some(fetch) = &query.fetch {
        if fetch.percent || fetch.with_ties || limit.is_some() {
            return Err(Error::Unsupported(
                "FETCH PERCENT/WITH TIES or combining FETCH with LIMIT is not supported".into(),
            ));
        }
        limit = Some(
            fetch
                .quantity
                .as_ref()
                .map(|expr| constant_usize(expr, "FETCH"))
                .transpose()?
                .unwrap_or(1),
        );
    }
    if limit == Some(0) {
        offset = 0;
    }
    Ok((offset, limit))
}

fn constant_usize(expr: &Expr, clause: &str) -> Result<usize> {
    let Expr::Value(value) = expr else {
        return Err(Error::InvalidArgument(format!(
            "{clause} must be a non-negative integer literal"
        )));
    };
    let Value::Number(value, _) = &value.value else {
        return Err(Error::InvalidArgument(format!(
            "{clause} must be a non-negative integer literal"
        )));
    };
    value
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("{clause} value '{value}' is out of range")))
}

fn object_name(name: &ObjectName) -> String {
    name.to_string()
}

fn last_name_part(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name).trim_matches('"')
}

use std::{cell::Cell, collections::HashSet, sync::Arc};

use arrow::datatypes::Schema;

use sqlparser::{
    ast::{
        Distinct, Expr, LimitClause, ObjectName, OrderByKind, Query, SetExpr, Spanned, Statement,
        Value,
    },
    dialect::DuckDbDialect,
    parser::Parser,
    tokenizer::Span,
};

use crate::{Catalog, Error, Result, runtime::QueryContext};

use super::{
    BoundExpr, LogicalPlan, PlanSchema, SortExpr, StatementPlan,
    binder::bind_expr,
    name_resolution::{
        explicit_projection_index, ordinal, rewrite_projection_aliases, source_location_suffix,
    },
    relation::{CteScope, alias_plan, cte_name},
};

mod join;
mod select;

#[cfg(test)]
pub fn plan_sql(catalog: &Catalog, sql: &str) -> Result<StatementPlan> {
    optimize_statement(bind_sql(catalog, sql)?, None)
}

pub(crate) fn bind_sql(catalog: &Catalog, sql: &str) -> Result<StatementPlan> {
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

pub(crate) fn optimize_statement(
    statement: StatementPlan,
    context: Option<&QueryContext>,
) -> Result<StatementPlan> {
    let optimize = |mut plan: LogicalPlan| {
        if let Some(context) = context {
            plan.freeze_query_statistics(context);
        }
        crate::optimizer::optimize(plan)
    };
    Ok(match statement {
        StatementPlan::Query(plan) => StatementPlan::Query(optimize(plan)?),
        StatementPlan::Explain(plan) => StatementPlan::Explain(optimize(plan)?),
        StatementPlan::ExplainAnalyze(plan) => StatementPlan::ExplainAnalyze(optimize(plan)?),
    })
}

struct Planner<'a> {
    catalog: &'a Catalog,
    next_scalar: Cell<usize>,
}

impl Planner<'_> {
    fn plan_statement(&self, statement: Statement) -> Result<StatementPlan> {
        match statement {
            Statement::Query(query) => Ok(StatementPlan::Query(self.plan_query(&query)?)),
            Statement::Explain {
                analyze, statement, ..
            } => {
                let Statement::Query(query) = *statement else {
                    return Err(Error::Unsupported(
                        "EXPLAIN currently supports SELECT only".into(),
                    ));
                };
                let plan = self.plan_query(&query)?;
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
        let distinct = matches!(&select.distinct, Some(Distinct::Distinct));
        let mut hidden_order = Vec::new();
        let mut prepared_order = Vec::new();
        if let Some(order_by) = &query.order_by {
            let OrderByKind::Expressions(order_exprs) = &order_by.kind else {
                return Err(Error::Unsupported("ORDER BY ALL is not supported".into()));
            };
            for order in order_exprs {
                if order.with_fill.is_some() {
                    return Err(Error::Unsupported(
                        "ORDER BY WITH FILL is not supported".into(),
                    ));
                }
                let target = if let Some(position) = ordinal(&order.expr, "ORDER BY")? {
                    OrderTarget::Ordinal(position)
                } else {
                    let mut expression = order.expr.clone();
                    rewrite_projection_aliases(&mut expression, &select.projection, "ORDER BY")?;
                    if distinct {
                        OrderTarget::Distinct(Box::new(expression))
                    } else {
                        let index = hidden_order.len();
                        hidden_order.push(expression);
                        OrderTarget::Hidden(index)
                    }
                };
                let descending = order.options.asc == Some(false);
                prepared_order.push(PreparedOrder {
                    target,
                    span: order.expr.span(),
                    descending,
                    // DuckDB defaults NULL values to the end for both ASC and DESC.
                    nulls_first: order.options.nulls_first.unwrap_or(false),
                });
            }
        }

        let mut plan = self.plan_select(select, &ctes, &hidden_order)?;
        if !prepared_order.is_empty() {
            let visible_width = plan
                .schema()
                .arrow()
                .fields()
                .len()
                .checked_sub(hidden_order.len())
                .ok_or_else(|| {
                    Error::Internal("hidden ORDER BY width exceeds projection".into())
                })?;
            let expressions = prepared_order
                .into_iter()
                .map(|order| {
                    let expr = match order.target {
                        OrderTarget::Ordinal(position) => output_column(
                            plan.schema(),
                            position,
                            visible_width,
                            "ORDER BY",
                            order.span,
                        )?,
                        OrderTarget::Hidden(index) => output_column(
                            plan.schema(),
                            visible_width + index + 1,
                            plan.schema().arrow().fields().len(),
                            "ORDER BY",
                            order.span,
                        )?,
                        OrderTarget::Distinct(expr) => {
                            bind_distinct_order(&expr, select, plan.schema())?
                        }
                    };
                    Ok(SortExpr {
                        expr,
                        descending: order.descending,
                        nulls_first: order.nulls_first,
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
            if !hidden_order.is_empty() {
                plan = project_visible(plan, visible_width);
            }
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

struct PreparedOrder {
    target: OrderTarget,
    span: Span,
    descending: bool,
    nulls_first: bool,
}

enum OrderTarget {
    Ordinal(usize),
    Hidden(usize),
    Distinct(Box<Expr>),
}

fn bind_distinct_order(
    expr: &Expr,
    select: &sqlparser::ast::Select,
    schema: &PlanSchema,
) -> Result<BoundExpr> {
    match bind_expr(expr, schema) {
        Ok(bound) => return Ok(bound),
        Err(Error::Catalog(message)) if message.contains("ambiguous") => {
            return Err(Error::Catalog(message));
        }
        Err(Error::Catalog(_)) => {}
        Err(error) => return Err(error),
    }
    if let Some(index) = explicit_projection_index(&select.projection, expr, "ORDER BY")? {
        let field = schema.arrow().field(index);
        return Ok(BoundExpr::column(
            index,
            field.data_type().clone(),
            field.name(),
        ));
    }
    Err(Error::InvalidArgument(format!(
        "ORDER BY expression `{expr}` must reference the SELECT DISTINCT output{}",
        source_location_suffix(expr.span())
    )))
}

fn output_column(
    schema: &PlanSchema,
    position: usize,
    allowed_width: usize,
    clause: &str,
    span: Span,
) -> Result<BoundExpr> {
    if position == 0 || position > allowed_width {
        return Err(Error::InvalidArgument(format!(
            "{clause} position {position} is out of range (output has {allowed_width} columns){}",
            source_location_suffix(span)
        )));
    }
    let field = schema.arrow().field(position - 1);
    Ok(BoundExpr::column(
        position - 1,
        field.data_type().clone(),
        field.name(),
    ))
}

fn project_visible(input: LogicalPlan, visible_width: usize) -> LogicalPlan {
    let expressions = input
        .schema()
        .arrow()
        .fields()
        .iter()
        .take(visible_width)
        .enumerate()
        .map(|(index, field)| {
            BoundExpr::column(index, field.data_type().clone(), field.name().clone())
        })
        .collect::<Vec<_>>();
    let schema = PlanSchema::unqualified(Arc::new(Schema::new(
        input
            .schema()
            .arrow()
            .fields()
            .iter()
            .take(visible_width)
            .cloned()
            .collect::<Vec<_>>(),
    )));
    LogicalPlan::Projection {
        input: Box::new(input),
        expressions,
        schema,
    }
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

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use sqlparser::ast::{Expr, NamedWindowDefinition, SelectItem};

use crate::{Error, Result};

use super::{ast, bind};
use crate::sql::{
    AggregateExpr, BoundExpr, LogicalPlan, PlanSchema, WindowExpr,
    aggregate::{bind_after_aggregate, bind_aggregate},
    binder::{bind_expr_scoped, bind_expr_scoped_internal, ensure_boolean},
};

#[allow(clippy::too_many_arguments)]
/// Plans the post-HAVING window phase. `qualify` has already had SELECT aliases
/// resolved by the query-block name resolver; this function only replaces
/// bound group, aggregate, and window subexpressions with hidden columns.
pub(crate) fn plan_window_projection(
    input: LogicalPlan,
    group_ast: &[Expr],
    projection: &[SelectItem],
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    named: &[NamedWindowDefinition],
    outer: Option<&PlanSchema>,
    aggregate_query: bool,
) -> Result<LogicalPlan> {
    let calls = ast::collect_window_calls(projection, qualify)?;
    if calls.is_empty() {
        return Err(Error::InvalidArgument(
            "QUALIFY requires at least one window function in the query block".into(),
        ));
    }
    if aggregate_query {
        plan_aggregate_window(
            input, group_ast, projection, having, qualify, named, outer, &calls,
        )
    } else {
        plan_plain_window(input, projection, qualify, named, outer, &calls)
    }
}

fn plan_plain_window(
    input: LogicalPlan,
    projection: &[SelectItem],
    qualify: Option<&Expr>,
    named: &[NamedWindowDefinition],
    outer: Option<&PlanSchema>,
    calls: &[ast::WindowCall],
) -> Result<LogicalPlan> {
    let input_schema = input.schema().clone();
    let mut windows = bind::bind_windows(calls, named, |expression| {
        bind_expr_scoped(expression, &input_schema, outer)
    })?;
    assign_hidden_names(&mut windows, &input_schema);
    let mut plan = append_window_nodes(input, &windows);
    let window_names = window_rewrites(&windows);
    if let Some(qualify) = qualify {
        let mut qualify = qualify.clone();
        ast::rewrite_expression(&mut qualify, &[], &[], &window_names);
        let predicate = bind_expr_scoped_internal(&qualify, plan.schema(), outer)?;
        ensure_qualify_boolean(&predicate)?;
        let schema = plan.schema().clone();
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
            schema,
        };
    }
    project_rewritten(plan, projection, &[], &[], &window_names, outer)
}

#[allow(clippy::too_many_arguments)]
fn plan_aggregate_window(
    input: LogicalPlan,
    group_ast: &[Expr],
    projection: &[SelectItem],
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    named: &[NamedWindowDefinition],
    outer: Option<&PlanSchema>,
    calls: &[ast::WindowCall],
) -> Result<LogicalPlan> {
    let input_schema = input.schema().clone();
    let group_exprs = group_ast
        .iter()
        .map(|expression| bind_expr_scoped(expression, &input_schema, outer))
        .collect::<Result<Vec<_>>>()?;

    let mut aggregate_exprs = Vec::new();
    let mut aggregate_calls = Vec::new();
    collect_aggregates(
        projection,
        having,
        qualify,
        named,
        &input_schema,
        &mut aggregate_exprs,
        &mut aggregate_calls,
    )?;

    let mut windows = bind::bind_windows(calls, named, |expression| {
        bind_after_aggregate(
            expression,
            &input_schema,
            group_ast,
            &group_exprs,
            &mut aggregate_exprs,
        )
    })?;
    let having = having
        .map(|expression| {
            bind_after_aggregate(
                expression,
                &input_schema,
                group_ast,
                &group_exprs,
                &mut aggregate_exprs,
            )
        })
        .transpose()?;
    if let Some(predicate) = &having {
        ensure_boolean(predicate).map_err(|_| {
            Error::InvalidArgument(format!(
                "HAVING requires BOOLEAN, got {}",
                predicate.data_type
            ))
        })?;
    }

    let aggregate_names = (0..aggregate_exprs.len())
        .map(|index| format!("__rustdb_aggregate_{index}"))
        .collect::<Vec<_>>();
    let group_names = (0..group_exprs.len())
        .map(|index| format!("__rustdb_group_{index}"))
        .collect::<Vec<_>>();
    let aggregate_schema = hidden_aggregate_schema(
        &group_exprs,
        &aggregate_exprs,
        &group_names,
        &aggregate_names,
    );
    assign_hidden_names(&mut windows, &aggregate_schema);
    let mut plan = LogicalPlan::Aggregate {
        input: Box::new(input),
        group_exprs,
        aggregate_exprs: aggregate_exprs.clone(),
        schema: aggregate_schema.clone(),
    };
    if let Some(predicate) = having {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
            schema: aggregate_schema,
        };
    }
    plan = append_window_nodes(plan, &windows);

    let groups = group_ast
        .iter()
        .cloned()
        .zip(group_names)
        .collect::<Vec<_>>();
    let aggregates = aggregate_calls
        .into_iter()
        .map(|(expression, index)| (expression, aggregate_names[index].clone()))
        .collect::<Vec<_>>();
    let window_names = window_rewrites(&windows);
    if let Some(qualify) = qualify {
        let mut qualify = qualify.clone();
        ast::rewrite_expression(&mut qualify, &groups, &aggregates, &window_names);
        let predicate = bind_expr_scoped_internal(&qualify, plan.schema(), outer)?;
        ensure_qualify_boolean(&predicate)?;
        let schema = plan.schema().clone();
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
            schema,
        };
    }
    project_rewritten(plan, projection, &groups, &aggregates, &window_names, outer)
}

#[allow(clippy::too_many_arguments)]
fn collect_aggregates(
    projection: &[SelectItem],
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    named: &[NamedWindowDefinition],
    input_schema: &PlanSchema,
    aggregates: &mut Vec<AggregateExpr>,
    calls: &mut Vec<(Expr, usize)>,
) -> Result<()> {
    let mut functions = Vec::new();
    for item in projection {
        if let SelectItem::UnnamedExpr(expression)
        | SelectItem::ExprWithAlias {
            expr: expression, ..
        } = item
        {
            ast::collect_functions(expression, &mut functions);
        }
    }
    if let Some(expression) = having {
        ast::collect_functions(expression, &mut functions);
    }
    if let Some(expression) = qualify {
        ast::collect_functions(expression, &mut functions);
    }
    for sqlparser::ast::NamedWindowDefinition(_, expression) in named {
        if let sqlparser::ast::NamedWindowExpr::WindowSpec(spec) = expression {
            for expression in &spec.partition_by {
                ast::collect_functions(expression, &mut functions);
            }
            for order in &spec.order_by {
                ast::collect_functions(&order.expr, &mut functions);
            }
        }
    }

    for function in functions {
        if !is_aggregate_name(&function.name.to_string()) {
            continue;
        }
        let aggregate = bind_aggregate(&function, input_schema)?;
        let index = aggregates
            .iter()
            .position(|existing| existing == &aggregate)
            .unwrap_or_else(|| {
                aggregates.push(aggregate);
                aggregates.len() - 1
            });
        let expression = Expr::Function(function);
        if !calls.iter().any(|(existing, _)| existing == &expression) {
            calls.push((expression, index));
        }
    }
    Ok(())
}

fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
}

fn append_window_nodes(mut input: LogicalPlan, windows: &[bind::WindowBinding]) -> LogicalPlan {
    let mut groups: Vec<Vec<&bind::WindowBinding>> = Vec::new();
    for window in windows {
        if let Some(group) = groups.iter_mut().find(|group| {
            group
                .first()
                .is_some_and(|existing| same_spec(&existing.bound, &window.bound))
        }) {
            group.push(window);
        } else {
            groups.push(vec![window]);
        }
    }
    for group in groups {
        let fields = group
            .iter()
            .map(|window| {
                Field::new(
                    window.hidden_name.clone(),
                    window.bound.data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<_>>();
        let expressions = group
            .into_iter()
            .map(|window| window.bound.clone())
            .collect::<Vec<_>>();
        let schema = append_schema(input.schema(), fields);
        input = LogicalPlan::Window {
            input: Box::new(input),
            expressions,
            schema,
        };
    }
    input
}

fn same_spec(left: &WindowExpr, right: &WindowExpr) -> bool {
    left.partition_by == right.partition_by
        && left.order_by == right.order_by
        && left.frame == right.frame
}

fn append_schema(input: &PlanSchema, fields: Vec<Field>) -> PlanSchema {
    let mut output_fields = input.arrow().fields().iter().cloned().collect::<Vec<_>>();
    output_fields.extend(fields.into_iter().map(Arc::new));
    let mut qualifiers = (0..input.arrow().fields().len())
        .map(|index| input.qualifier(index).map(str::to_owned))
        .collect::<Vec<_>>();
    qualifiers.resize(output_fields.len(), None);
    let mut visible = (0..input.arrow().fields().len())
        .map(|index| input.is_visible(index))
        .collect::<Vec<_>>();
    visible.resize(output_fields.len(), false);
    PlanSchema::new_with_visibility(Arc::new(Schema::new(output_fields)), qualifiers, visible)
}

fn hidden_aggregate_schema(
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    group_names: &[String],
    aggregate_names: &[String],
) -> PlanSchema {
    let fields = groups
        .iter()
        .zip(group_names)
        .map(|(expression, name)| Field::new(name, expression.data_type.clone(), true))
        .chain(
            aggregates
                .iter()
                .zip(aggregate_names)
                .map(|(expression, name)| Field::new(name, expression.data_type.clone(), true)),
        )
        .collect::<Vec<_>>();
    PlanSchema::unqualified(Arc::new(Schema::new(fields)))
}

fn window_rewrites(windows: &[bind::WindowBinding]) -> Vec<(Expr, String)> {
    windows
        .iter()
        .map(|window| (window.expression.clone(), window.hidden_name.clone()))
        .collect()
}

fn assign_hidden_names(windows: &mut [bind::WindowBinding], input: &PlanSchema) {
    let mut used = input
        .arrow()
        .fields()
        .iter()
        .map(|field| field.name().to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    for (index, window) in windows.iter_mut().enumerate() {
        let mut suffix = 0usize;
        loop {
            let candidate = if suffix == 0 {
                format!("__rustdb_window_{index}")
            } else {
                format!("__rustdb_window_{index}_{suffix}")
            };
            if used.insert(candidate.to_ascii_lowercase()) {
                window.hidden_name = candidate;
                break;
            }
            suffix += 1;
        }
    }
}

fn project_rewritten(
    input: LogicalPlan,
    projection: &[SelectItem],
    groups: &[(Expr, String)],
    aggregates: &[(Expr, String)],
    windows: &[(Expr, String)],
    outer: Option<&PlanSchema>,
) -> Result<LogicalPlan> {
    let mut expressions = Vec::with_capacity(projection.len());
    for item in projection {
        let (source, alias) = match item {
            SelectItem::UnnamedExpr(expression) => (expression, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            other => {
                return Err(Error::Unsupported(format!(
                    "select item `{other}` is not supported in window queries"
                )));
            }
        };
        let mut rewritten = source.clone();
        ast::rewrite_expression(&mut rewritten, groups, aggregates, windows);
        let mut bound = bind_expr_scoped_internal(&rewritten, input.schema(), outer)?;
        bound.display_name = alias
            .map(|alias| alias.value.clone())
            .unwrap_or_else(|| source.to_string());
        expressions.push(bound);
    }
    let schema = PlanSchema::unqualified(Arc::new(Schema::new(
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
    )));
    Ok(LogicalPlan::Projection {
        input: Box::new(input),
        expressions,
        schema,
    })
}

fn ensure_qualify_boolean(predicate: &BoundExpr) -> Result<()> {
    ensure_boolean(predicate).map_err(|_| {
        Error::InvalidArgument(format!(
            "QUALIFY requires BOOLEAN, got {}",
            predicate.data_type
        ))
    })
}

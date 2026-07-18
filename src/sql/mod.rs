use std::sync::Arc;

mod aggregate;
mod binder;
mod coercion;
mod expr;
mod functions;
mod interval;
mod literal;
mod name_resolution;
mod parameter_literal;
mod parser;
mod plan;
mod planner;
mod relation;
pub(crate) mod scalar_subquery;
mod subquery;
pub(crate) mod temporal;
mod window;
mod window_types;

pub use expr::{
    AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, DateTimePart, ExprKind, ScalarFunction,
    ScalarValue, SortExpr, UnaryOp,
};
pub(crate) use parser::{parse_statements, split_statement_text};
pub use plan::{DependentJoinKind, JoinType, LogicalPlan, PlanSchema, StatementPlan};
pub(crate) use plan::{UNMATERIALIZED_FIELD_KEY, field_is_materialized};
#[cfg(test)]
pub use planner::plan_sql;
pub(crate) use planner::{bind_statement, optimize_statement};
#[allow(unused_imports)]
pub(crate) use window_types::{
    WindowExpr, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction,
};

pub(crate) fn bind_table_filter(
    expression: &sqlparser::ast::Expr,
    schema: arrow::datatypes::SchemaRef,
    table_name: &str,
) -> crate::Result<BoundExpr> {
    let qualifiers = vec![Some(table_name.to_owned()); schema.fields().len()];
    let bound = binder::bind_expr(expression, &PlanSchema::new(schema, qualifiers))?;
    if bound.data_type != arrow::datatypes::DataType::Boolean {
        return Err(crate::Error::InvalidArgument(format!(
            "WHERE predicate must be Boolean, found {}",
            bound.data_type
        )));
    }
    Ok(bound)
}

pub(crate) fn bind_table_value(
    expression: &sqlparser::ast::Expr,
    schema: arrow::datatypes::SchemaRef,
    table_name: &str,
    target: &arrow::datatypes::DataType,
) -> crate::Result<BoundExpr> {
    let qualifiers = vec![Some(table_name.to_owned()); schema.fields().len()];
    let bound = binder::bind_expr(expression, &PlanSchema::new(schema, qualifiers))?;
    Ok(coercion::cast_if_needed(bound, target))
}

pub(crate) fn bind_table_projection(
    items: &[sqlparser::ast::SelectItem],
    schema: arrow::datatypes::SchemaRef,
    table_name: &str,
) -> crate::Result<(Vec<BoundExpr>, arrow::datatypes::SchemaRef)> {
    use arrow::datatypes::{Field, Schema};
    use sqlparser::ast::{SelectItem, SelectItemQualifiedWildcardKind};

    let plan_schema = PlanSchema::new(
        Arc::clone(&schema),
        vec![Some(table_name.to_owned()); schema.fields().len()],
    );
    let mut expressions = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expression) => {
                expressions.push(binder::bind_expr(expression, &plan_schema)?);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let mut expression = binder::bind_expr(expr, &plan_schema)?;
                expression.display_name = alias.value.clone();
                expressions.push(expression);
            }
            SelectItem::Wildcard(options) if plain_wildcard(options) => {
                expressions.extend(schema.fields().iter().enumerate().map(|(index, field)| {
                    BoundExpr::column(index, field.data_type().clone(), field.name())
                }));
            }
            SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(name),
                options,
            ) if plain_wildcard(options)
                && returning_qualifier(name).is_some_and(|name| {
                    crate::catalog_name::qualifier_matches(table_name, &name)
                }) =>
            {
                expressions.extend(schema.fields().iter().enumerate().map(|(index, field)| {
                    BoundExpr::column(index, field.data_type().clone(), field.name())
                }));
            }
            other => {
                return Err(crate::Error::Unsupported(format!(
                    "RETURNING item '{other}' is not supported"
                )));
            }
        }
    }
    let fields = expressions
        .iter()
        .map(|expression| Field::new(&expression.display_name, expression.data_type.clone(), true))
        .collect::<Vec<_>>();
    Ok((expressions, Arc::new(Schema::new(fields))))
}

fn returning_qualifier(name: &sqlparser::ast::ObjectName) -> Option<String> {
    let identifiers = name
        .0
        .iter()
        .map(|part| part.as_ident().map(|identifier| identifier.value.as_str()))
        .collect::<Option<Vec<_>>>()?;
    (identifiers.len() <= 2).then(|| identifiers.join("."))
}

fn plain_wildcard(options: &sqlparser::ast::WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

#[cfg(test)]
mod correctness_tests;
#[cfg(test)]
mod correlation_aggregate_tests;
#[cfg(test)]
mod correlation_tests;
#[cfg(test)]
mod join_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod timezone_tests;
#[cfg(test)]
mod tpch_queries;
#[cfg(test)]
mod tpch_tests;

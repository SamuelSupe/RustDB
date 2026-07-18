use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use sqlparser::ast::{
    Distinct, Expr, GroupByExpr, Ident, JoinOperator, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, Spanned, TableFactor, TableWithJoins,
};

use crate::{Error, Result};

use super::super::{
    BoundExpr, JoinType, LogicalPlan, PlanSchema,
    aggregate::{is_aggregate_query, plan_aggregate_projection},
    binder::{bind_expr, bind_expr_scoped},
    name_resolution::{resolve_group_by, rewrite_projection_aliases, source_location_suffix},
    relation::{CteScope, alias_plan, cte_name},
    scalar_subquery::extract as extract_scalar_subqueries,
    subquery::{apply_where, is_direct_mark_attachment, stageable_direct_mark_term},
    window,
};
use super::{
    Planner, grouping_sets,
    join::{JoinBinding, apply_using_projection, bind_join_constraint},
    last_name_part, object_name,
};

impl Planner<'_> {
    pub(super) fn plan_select(
        &self,
        select: &Select,
        ctes: &CteScope,
        hidden_projection: &[Expr],
        outer: Option<&PlanSchema>,
    ) -> Result<LogicalPlan> {
        if select.top.is_some() {
            return Err(Error::Unsupported("TOP is not supported".into()));
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
        let mut qualify = select.qualify.clone();
        let expanded_grouping = grouping_sets::expand(&select.group_by)?;
        let (mut group_ast, mut grouping_plan) = match expanded_grouping {
            Some(expanded) => (expanded.universe, Some(expanded.sets)),
            None => match &select.group_by {
                GroupByExpr::Expressions(expressions, _) => (expressions.clone(), None),
                GroupByExpr::All(_) => unreachable!("GROUP BY ALL was rejected by expansion"),
            },
        };
        let mut plan = self.plan_from(&select.from, ctes)?;
        projection = expand_wildcards(&projection, plan.schema())?;
        resolve_group_by(&mut group_ast, &projection, plan.schema())?;
        if let Some(sets) = grouping_plan.as_mut() {
            grouping_sets::normalize_resolved(&mut group_ast, sets)?;
        }
        if let Some(predicate) = &mut having {
            rewrite_projection_aliases(predicate, &projection, "HAVING")?;
        }
        if let Some(predicate) = &mut qualify {
            rewrite_projection_aliases(predicate, &projection, "QUALIFY")?;
        }
        if selection.as_ref().is_some_and(window::contains_window) {
            return Err(Error::InvalidArgument(
                "window functions are not allowed in WHERE".into(),
            ));
        }
        if group_ast.iter().any(window::contains_window) {
            return Err(Error::InvalidArgument(
                "window functions are not allowed in GROUP BY".into(),
            ));
        }
        if having.as_ref().is_some_and(window::contains_window) {
            return Err(Error::InvalidArgument(
                "window functions are not allowed in HAVING".into(),
            ));
        }
        projection.extend(
            hidden_projection
                .iter()
                .cloned()
                .map(SelectItem::UnnamedExpr),
        );
        let aggregate_query = grouping_plan.is_some()
            || is_aggregate_query(
                &group_ast,
                &projection,
                having.as_ref(),
                qualify.as_ref(),
                &select.named_window,
            );

        if let Some(predicate) = &mut selection {
            plan = self.plan_where(plan, predicate, outer, ctes)?;
        }
        for group in &mut group_ast {
            plan = self.extract_scalars(plan, group, None, None, None, ctes)?;
        }
        let grouped_outer_columns = if aggregate_query {
            let mut columns = group_ast
                .iter()
                .filter_map(|group| bind_expr(group, plan.schema()).ok())
                .filter_map(|group| match group.kind {
                    super::super::ExprKind::Column(index) => Some(index),
                    _ => None,
                })
                .collect::<Vec<_>>();
            columns.sort_unstable();
            columns.dedup();
            Some(columns)
        } else {
            None
        };
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
                aggregate_query.then_some(group_ast.as_slice()),
                grouped_outer_columns.as_deref(),
                ctes,
            )?;
        }
        if let Some(predicate) = &mut having {
            plan = self.extract_scalars(
                plan,
                predicate,
                aggregate_query.then_some(&mut hidden_groups),
                aggregate_query.then_some(group_ast.as_slice()),
                grouped_outer_columns.as_deref(),
                ctes,
            )?;
        }
        if let Some(predicate) = &mut qualify {
            plan = self.extract_scalars(
                plan,
                predicate,
                aggregate_query.then_some(&mut hidden_groups),
                aggregate_query.then_some(group_ast.as_slice()),
                grouped_outer_columns.as_deref(),
                ctes,
            )?;
        }
        group_ast.extend(hidden_groups.iter().cloned());

        let has_windows = projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => window::contains_window(expression),
            _ => false,
        }) || qualify.as_ref().is_some_and(window::contains_window);
        if has_windows || qualify.is_some() {
            if grouping_plan.is_some() {
                return Err(Error::Unsupported(
                    "window functions over grouping sets are not supported yet".into(),
                ));
            }
            let plan = window::plan_window_projection(
                plan,
                &group_ast,
                &projection,
                having.as_ref(),
                qualify.as_ref(),
                &select.named_window,
                outer,
                aggregate_query,
            )?;
            return Ok(if distinct { plan_distinct(plan) } else { plan });
        }

        if aggregate_query {
            let plan = if let Some(sets) = grouping_plan.as_deref() {
                grouping_sets::plan(plan, &group_ast, sets, &projection, having.as_ref(), outer)?
            } else {
                plan_aggregate_projection(
                    plan,
                    &group_ast,
                    &projection,
                    having.as_ref(),
                    outer,
                    &hidden_groups,
                )?
            };
            Ok(if distinct { plan_distinct(plan) } else { plan })
        } else {
            let plan = self.plan_projection(plan, &projection, outer)?;
            if distinct {
                Ok(plan_distinct(plan))
            } else {
                Ok(plan)
            }
        }
    }

    fn plan_where(
        &self,
        plan: LogicalPlan,
        predicate: &mut Expr,
        outer: Option<&PlanSchema>,
        ctes: &CteScope,
    ) -> Result<LogicalPlan> {
        if let Some((mut direct_mark, mut remainder)) = stageable_direct_mark_term(predicate) {
            let scalar_checkpoint = self.next_scalar.get();
            let candidate =
                self.extract_scalars(plan.clone(), &mut direct_mark, None, None, None, ctes)?;
            if is_direct_mark_attachment(&candidate) {
                let candidate = apply_where(candidate, &direct_mark, outer)?;
                return self.plan_where(candidate, &mut remainder, outer, ctes);
            }
            // Speculative extraction did not produce a direct marker
            // attachment. Discard it and preserve deterministic names for the
            // guarded path.
            self.next_scalar.set(scalar_checkpoint);
        }

        let plan = self.extract_scalars(plan, predicate, None, None, None, ctes)?;
        apply_where(plan, predicate, outer)
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
                null_equal_keys: false,
                residual: None,
                null_aware: None,
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
            let right = self.plan_table(&join.relation, ctes)?;
            let (join_type, constraint) = match &join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    (JoinType::Inner, constraint)
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (JoinType::Left, constraint)
                }
                JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
                    (JoinType::Right, constraint)
                }
                JoinOperator::FullOuter(constraint) => (JoinType::Full, constraint),
                JoinOperator::Semi(constraint) | JoinOperator::LeftSemi(constraint) => {
                    (JoinType::Semi, constraint)
                }
                JoinOperator::Anti(constraint) | JoinOperator::LeftAnti(constraint) => {
                    (JoinType::Anti, constraint)
                }
                other => {
                    return Err(Error::Unsupported(format!(
                        "join type {other:?} is not supported"
                    )));
                }
            };
            let JoinBinding {
                keys: on,
                residual,
                using_columns,
                unhashable_equality_reason,
            } = bind_join_constraint(constraint, left.schema(), right.schema())?;
            if on.is_empty() {
                if let Some(reason) = unhashable_equality_reason {
                    return Err(Error::InvalidArgument(format!(
                        "{reason}; use an explicit CAST so both sides have the same type, or add another same-type equality key"
                    )));
                }
                return Err(Error::Unsupported(
                    "joins require at least one equality key".into(),
                ));
            }
            let left_schema = left.schema().clone();
            let right_schema = right.schema().clone();
            let schema = match join_type {
                JoinType::Inner => PlanSchema::join(left.schema(), right.schema()),
                JoinType::Left => PlanSchema::left_join(left.schema(), right.schema()),
                JoinType::Right => PlanSchema::right_join(left.schema(), right.schema()),
                JoinType::Full => PlanSchema::full_join(left.schema(), right.schema()),
                JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti => left.schema().clone(),
                JoinType::LeftSingle | JoinType::Mark => unreachable!(
                    "parser-visible joins do not produce internal correlated join types"
                ),
            };
            let using_keys = on.clone();
            let joined = LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                null_equal_keys: false,
                residual,
                null_aware: None,
                join_type,
                schema,
            };
            left = if matches!(join_type, JoinType::Semi | JoinType::Anti) {
                joined
            } else {
                apply_using_projection(
                    joined,
                    join_type,
                    &left_schema,
                    &right_schema,
                    &using_columns,
                    &using_keys,
                )?
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
                let display_name = object_name(name);
                let table_name = crate::catalog_name::object(name, "table")?;
                if name.0.len() == 1
                    && let Some(plan) = ctes.get(&cte_name(last_name_part(&display_name)))
                {
                    let qualifier = alias
                        .as_ref()
                        .map(|alias| alias.name.value.clone())
                        .unwrap_or_else(|| last_name_part(&display_name).to_owned());
                    return alias_plan(
                        plan.clone(),
                        Some(&qualifier),
                        alias
                            .as_ref()
                            .map(|alias| alias.columns.as_slice())
                            .unwrap_or(&[]),
                    );
                }
                let qualifier = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| {
                        if table_name.contains('.') {
                            table_name.clone()
                        } else {
                            format!("{}.{}", crate::catalog_name::DEFAULT_SCHEMA, table_name)
                        }
                    });
                let entry = self.catalog.table(&table_name).ok_or_else(|| {
                    Error::Catalog(format!("table '{display_name}' does not exist"))
                })?;
                let provider = Arc::clone(entry.provider());
                let schema = provider.schema();
                let statistics = provider.statistics();
                let plan_schema = PlanSchema::new(
                    schema,
                    vec![Some(qualifier.clone()); provider.schema().fields().len()],
                );
                let plan = LogicalPlan::Scan {
                    table_name: display_name,
                    provider,
                    statistics,
                    projection: None,
                    pushed_filter: None,
                    exact_filter: None,
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

    fn plan_projection(
        &self,
        input: LogicalPlan,
        items: &[SelectItem],
        outer: Option<&PlanSchema>,
    ) -> Result<LogicalPlan> {
        let expressions = bind_select_items(items, input.schema(), outer)?;
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
        aggregate_groups: Option<&[Expr]>,
        allowed_outer_columns: Option<&[usize]>,
        ctes: &CteScope,
    ) -> Result<LogicalPlan> {
        let mut next_name = || {
            let index = self.next_scalar.get();
            self.next_scalar.set(index.saturating_add(1));
            format!("__rustdb_scalar_subquery_{index}")
        };
        let outer_schema = plan.schema().clone();
        let mut plan_subquery = |query: &Query| {
            let plan = self.plan_query_with_outer(query, ctes, Some(&outer_schema))?;
            if let Some(allowed) = allowed_outer_columns {
                super::super::scalar_subquery::validate_outer_grouping(
                    &plan,
                    allowed,
                    &outer_schema,
                    &source_location_suffix(query.span()),
                )?;
            }
            Ok(plan)
        };
        extract_scalar_subqueries(
            plan,
            expr,
            hidden_groups,
            aggregate_groups,
            &mut next_name,
            &mut plan_subquery,
        )
    }
}

fn expand_wildcards(items: &[SelectItem], schema: &PlanSchema) -> Result<Vec<SelectItem>> {
    let mut output = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => output.extend(
                (0..schema.arrow().fields().len())
                    .filter(|index| schema.is_visible(*index))
                    .map(|index| SelectItem::UnnamedExpr(column_expr(schema, index))),
            ),
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _) => {
                let display_name = object_name(name);
                let qualifier = wildcard_qualifier(name)?;
                let start = output.len();
                output.extend(
                    (0..schema.arrow().fields().len())
                        .filter(|index| schema.qualifier_matches(*index, &qualifier))
                        .map(|index| SelectItem::UnnamedExpr(column_expr(schema, index))),
                );
                if output.len() == start {
                    return Err(Error::Catalog(format!(
                        "relation '{display_name}' does not exist"
                    )));
                }
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::Expr(_), _) => {
                return Err(Error::Unsupported(
                    "wildcards on arbitrary expressions are not supported".into(),
                ));
            }
            other => output.push(other.clone()),
        }
    }
    Ok(output)
}

fn column_expr(schema: &PlanSchema, index: usize) -> Expr {
    let column = Ident::new(schema.arrow().field(index).name());
    match schema.qualifier(index) {
        Some(qualifier) => Expr::CompoundIdentifier(
            qualifier
                .split('.')
                .map(Ident::new)
                .chain(std::iter::once(column))
                .collect(),
        ),
        None => Expr::Identifier(column),
    }
}

fn bind_select_items(
    items: &[SelectItem],
    schema: &PlanSchema,
    outer: Option<&PlanSchema>,
) -> Result<Vec<BoundExpr>> {
    let mut output = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => output.push(bind_expr_scoped(expr, schema, outer)?),
            SelectItem::ExprWithAlias { expr, alias } => {
                let mut expr = bind_expr_scoped(expr, schema, outer)?;
                expr.display_name = alias.value.clone();
                output.push(expr);
            }
            SelectItem::Wildcard(_) => output.extend(
                schema
                    .arrow()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| schema.is_visible(*index))
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
    let display_name = object_name(name);
    let qualifier = wildcard_qualifier(name)?;
    let start = output.len();
    output.extend(
        schema
            .arrow()
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| schema.qualifier_matches(*index, &qualifier))
            .map(|(index, field)| {
                BoundExpr::column(index, field.data_type().clone(), field.name())
            }),
    );
    if output.len() == start {
        return Err(Error::Catalog(format!(
            "relation '{display_name}' does not exist"
        )));
    }
    Ok(())
}

fn wildcard_qualifier(name: &sqlparser::ast::ObjectName) -> Result<String> {
    if let [part] = name.0.as_slice() {
        return part
            .as_ident()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| Error::Unsupported("relation aliases must be identifiers".into()));
    }
    crate::catalog_name::qualifier(name, "relation")
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

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use sqlparser::ast::{Expr, Ident, SelectItem};

use crate::sql::{BoundExpr, DependentJoinKind, ExprKind, JoinType, LogicalPlan, PlanSchema};
use crate::{Error, Result};

use super::{aggregate_schema, bind_after_aggregate, expression_schema, select_expr};

mod rewrite;
mod usage;

pub(super) fn has_attachments(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            schema,
            ..
        } => (on.is_empty() && is_attachment(*join_type, right, schema)) || has_attachments(left),
        LogicalPlan::DependentJoin {
            left, kind, guard, ..
        } => {
            matches!(kind, DependentJoinKind::In { needle } if needle.contains_deferred_aggregate())
                || guard
                    .as_ref()
                    .is_some_and(BoundExpr::contains_deferred_aggregate)
                || has_attachments(left)
        }
        _ => false,
    }
}

pub(super) fn plan(
    input: LogicalPlan,
    group_ast: &[Expr],
    hidden_groups: &[Expr],
    items: &[SelectItem],
    having: Option<&Expr>,
) -> Result<LogicalPlan> {
    let usage = usage::collect(items, having);
    let mut attachments = Vec::new();
    let input = peel(input, &mut attachments, &usage.outside_aggregate)?;
    let source_schema = input.schema().clone();
    // Peeling walks the outermost (latest) attachment first. Reversing keeps
    // the original evaluation/dependency order without lexicographic bugs at
    // generated ids such as 10 versus 2.
    attachments.reverse();
    let names = attachments
        .iter()
        .map(|attachment| attachment.name.as_str())
        .collect::<Vec<_>>();
    let hidden_names = hidden_groups
        .iter()
        .filter_map(identifier_name)
        .collect::<Vec<_>>();
    let actual_group_ast = group_ast
        .iter()
        .filter(|expression| {
            let Some(name) = identifier_name(expression) else {
                return true;
            };
            !hidden_names.contains(&name)
                || (usage.outside_aggregate.contains(name)
                    && !names
                        .iter()
                        .any(|deferred| deferred.eq_ignore_ascii_case(name)))
        })
        .cloned()
        .collect::<Vec<_>>();
    let group_exprs = actual_group_ast
        .iter()
        .map(|expression| crate::sql::binder::bind_expr(expression, input.schema()))
        .collect::<Result<Vec<_>>>()?;

    let mut discovery_group_ast = actual_group_ast.clone();
    let mut discovery_group_exprs = group_exprs.clone();
    for attachment in &attachments {
        discovery_group_ast.push(Expr::Identifier(Ident::new(&attachment.name)));
        discovery_group_exprs.push(BoundExpr::column(
            0,
            attachment.field.data_type().clone(),
            attachment.name.clone(),
        ));
    }
    let mut aggregates = Vec::new();
    for item in items {
        let (expression, _) = select_expr(item)?;
        bind_after_aggregate(
            expression,
            &source_schema,
            &discovery_group_ast,
            &discovery_group_exprs,
            &mut aggregates,
        )?;
    }
    if let Some(predicate) = having {
        bind_after_aggregate(
            predicate,
            &source_schema,
            &discovery_group_ast,
            &discovery_group_exprs,
            &mut aggregates,
        )?;
    }
    for attachment in &attachments {
        for expression in attachment.deferred_expressions() {
            rewrite::register_aggregates(expression, &mut aggregates);
        }
    }

    let aggregate_schema = aggregate_schema(&group_exprs, &aggregates);
    let mut plan = LogicalPlan::Aggregate {
        input: Box::new(input),
        group_exprs: group_exprs.clone(),
        aggregate_exprs: aggregates.clone(),
        schema: aggregate_schema,
    };
    let aggregate_width = aggregates.len();
    let group_width = group_exprs.len();
    let input_to_group = input_to_group_mapping(&group_exprs);
    let mut deferred_columns = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let index = plan.schema().arrow().fields().len();
        plan = attach(plan, attachment, &input_to_group, group_width, &aggregates)?;
        deferred_columns.push(index);
    }

    let mut reorder = Vec::with_capacity(plan.schema().arrow().fields().len());
    reorder.extend(columns(&plan, 0..group_width));
    reorder.extend(deferred_columns.iter().map(|index| column(&plan, *index)));
    reorder.extend(columns(&plan, group_width..group_width + aggregate_width));
    let reorder_schema = expression_schema(&reorder);
    plan = LogicalPlan::Projection {
        input: Box::new(plan),
        expressions: reorder,
        schema: reorder_schema,
    };

    let mut output_exprs = Vec::with_capacity(items.len());
    for item in items {
        let (expression, alias) = select_expr(item)?;
        let mut bound = bind_after_aggregate(
            expression,
            &source_schema,
            &discovery_group_ast,
            &discovery_group_exprs,
            &mut aggregates,
        )?;
        if let Some(alias) = alias {
            bound.display_name = alias.value.clone();
        }
        output_exprs.push(bound);
    }
    let having = having
        .map(|predicate| {
            bind_after_aggregate(
                predicate,
                &source_schema,
                &discovery_group_ast,
                &discovery_group_exprs,
                &mut aggregates,
            )
        })
        .transpose()?;
    if let Some(predicate) = &having {
        crate::sql::binder::ensure_boolean(predicate).map_err(|_| {
            Error::InvalidArgument(format!(
                "HAVING requires BOOLEAN, got {}",
                predicate.data_type
            ))
        })?;
    }
    if let Some(predicate) = having {
        let schema = plan.schema().clone();
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
            schema,
        };
    }
    let schema = expression_schema(&output_exprs);
    Ok(LogicalPlan::Projection {
        input: Box::new(plan),
        expressions: output_exprs,
        schema,
    })
}

struct Attachment {
    name: String,
    field: Arc<Field>,
    kind: AttachmentKind,
}

enum AttachmentKind {
    Join {
        right: LogicalPlan,
        join_type: JoinType,
        residual: Option<BoundExpr>,
        null_aware: Option<(BoundExpr, BoundExpr)>,
    },
    Dependent {
        right: LogicalPlan,
        kind: DependentJoinKind,
        guard: Option<BoundExpr>,
    },
}

impl Attachment {
    fn deferred_expressions(&self) -> Vec<&BoundExpr> {
        let mut expressions = Vec::with_capacity(2);
        match &self.kind {
            AttachmentKind::Join {
                residual,
                null_aware,
                ..
            } => {
                if let Some(residual) = residual {
                    expressions.push(residual);
                }
                if let Some((needle, _)) = null_aware {
                    expressions.push(needle);
                }
            }
            AttachmentKind::Dependent { kind, guard, .. } => {
                if let Some(guard) = guard {
                    expressions.push(guard);
                }
                if let DependentJoinKind::In { needle }
                | DependentJoinKind::InFilter { needle, .. } = kind
                {
                    expressions.push(needle);
                }
            }
        }
        expressions
    }
}

fn peel(
    plan: LogicalPlan,
    output: &mut Vec<Attachment>,
    candidates: &std::collections::HashSet<String>,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        } if on.is_empty() && is_attachment(join_type, &right, &schema) => {
            let field = Arc::clone(&schema.arrow().fields()[schema.arrow().fields().len() - 1]);
            if candidates.contains(field.name()) {
                output.push(Attachment {
                    name: field.name().clone(),
                    field,
                    kind: AttachmentKind::Join {
                        right: *right,
                        join_type,
                        residual,
                        null_aware,
                    },
                });
                peel(*left, output, candidates)
            } else {
                let left = peel(*left, output, candidates)?;
                let result_schema = PlanSchema::unqualified(Arc::new(Schema::new(vec![field])));
                let schema = PlanSchema::join(left.schema(), &result_schema);
                Ok(LogicalPlan::Join {
                    left: Box::new(left),
                    right,
                    on,
                    null_equal_keys,
                    residual,
                    null_aware,
                    join_type,
                    schema,
                })
            }
        }
        LogicalPlan::DependentJoin {
            left,
            right,
            kind:
                kind @ (DependentJoinKind::ExistsFilter { .. } | DependentJoinKind::InFilter { .. }),
            guard,
            ..
        } => {
            let left = peel(*left, output, candidates)?;
            let schema = left.schema().clone();
            Ok(LogicalPlan::DependentJoin {
                left: Box::new(left),
                right,
                kind,
                guard,
                schema,
            })
        }
        LogicalPlan::DependentJoin {
            left,
            right,
            kind,
            guard,
            schema,
        } => {
            let result = Arc::clone(&schema.arrow().fields()[schema.arrow().fields().len() - 1]);
            if candidates.contains(result.name())
                && (matches!(&kind, DependentJoinKind::In { needle } if needle.contains_deferred_aggregate())
                    || guard
                        .as_ref()
                        .is_some_and(BoundExpr::contains_deferred_aggregate))
            {
                output.push(Attachment {
                    name: result.name().clone(),
                    field: result,
                    kind: AttachmentKind::Dependent {
                        right: *right,
                        kind,
                        guard,
                    },
                });
                return peel(*left, output, candidates);
            }
            let left = peel(*left, output, candidates)?;
            let result_schema = PlanSchema::unqualified(Arc::new(Schema::new(vec![result])));
            let schema = PlanSchema::join(left.schema(), &result_schema);
            Ok(LogicalPlan::DependentJoin {
                left: Box::new(left),
                right,
                kind,
                guard,
                schema,
            })
        }
        other => Ok(other),
    }
}

fn is_attachment(join_type: JoinType, right: &LogicalPlan, schema: &PlanSchema) -> bool {
    let generated = schema
        .arrow()
        .fields()
        .last()
        .is_some_and(|field| field.name().starts_with("__rustdb_scalar_subquery_"));
    generated
        && match join_type {
            JoinType::Inner => matches!(right, LogicalPlan::Scalarize { .. }),
            JoinType::LeftSingle | JoinType::Mark => true,
            _ => false,
        }
}

fn attach(
    left: LogicalPlan,
    mut attachment: Attachment,
    input_to_group: &[(usize, usize)],
    group_width: usize,
    aggregates: &[crate::sql::AggregateExpr],
) -> Result<LogicalPlan> {
    let result_schema =
        PlanSchema::unqualified(Arc::new(Schema::new(vec![Arc::clone(&attachment.field)])));
    let schema = PlanSchema::join(left.schema(), &result_schema);
    match &mut attachment.kind {
        AttachmentKind::Join {
            right,
            join_type,
            residual,
            null_aware,
        } => {
            if let Some(guard) = residual {
                rewrite::remap_group_columns(guard, input_to_group, left.schema())?;
                rewrite::resolve_results(guard, group_width, aggregates)?;
            }
            if let Some((needle, _)) = null_aware {
                rewrite::remap_group_columns(needle, input_to_group, left.schema())?;
                rewrite::resolve_results(needle, group_width, aggregates)?;
            }
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(std::mem::replace(
                    right,
                    LogicalPlan::Empty {
                        produce_one_row: false,
                        schema: PlanSchema::empty(),
                    },
                )),
                on: Vec::new(),
                null_equal_keys: false,
                residual: residual.take(),
                null_aware: null_aware.take(),
                join_type: *join_type,
                schema,
            })
        }
        AttachmentKind::Dependent { right, kind, guard } => {
            if let DependentJoinKind::In { needle } | DependentJoinKind::InFilter { needle, .. } =
                kind
            {
                rewrite::remap_group_columns(needle, input_to_group, left.schema())?;
                rewrite::resolve_results(needle, group_width, aggregates)?;
            }
            if let Some(guard) = guard {
                rewrite::remap_group_columns(guard, input_to_group, left.schema())?;
                rewrite::resolve_results(guard, group_width, aggregates)?;
            }
            rewrite::remap_outer_plan(right, input_to_group)?;
            Ok(LogicalPlan::DependentJoin {
                left: Box::new(left),
                right: Box::new(std::mem::replace(
                    right,
                    LogicalPlan::Empty {
                        produce_one_row: false,
                        schema: PlanSchema::empty(),
                    },
                )),
                kind: kind.clone(),
                guard: guard.take(),
                schema,
            })
        }
    }
}

fn input_to_group_mapping(groups: &[BoundExpr]) -> Vec<(usize, usize)> {
    groups
        .iter()
        .enumerate()
        .filter_map(|(group, expression)| match expression.kind {
            ExprKind::Column(input) => Some((input, group)),
            _ => None,
        })
        .collect()
}

fn identifier_name(expression: &Expr) -> Option<&str> {
    match expression {
        Expr::Identifier(ident) => Some(&ident.value),
        _ => None,
    }
}

fn columns(plan: &LogicalPlan, range: std::ops::Range<usize>) -> Vec<BoundExpr> {
    range.map(|index| column(plan, index)).collect()
}

fn column(plan: &LogicalPlan, index: usize) -> BoundExpr {
    let field = plan.schema().arrow().field(index);
    BoundExpr::column(index, field.data_type().clone(), field.name().clone())
}

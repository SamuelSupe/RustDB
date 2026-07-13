use std::sync::Arc;

use arrow::datatypes::{Field, Schema};

use crate::sql::{BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan, PlanSchema};
use crate::{Error, Result};

use super::rewrite::{combine_and, remap_columns, split_and};

pub(super) enum Correlation {
    Key { outer: BoundExpr, inner: BoundExpr },
    Residual(BoundExpr),
}

pub(super) struct Pulled {
    pub(super) plan: LogicalPlan,
    pub(super) correlations: Vec<Correlation>,
    pub(super) old_to_new: Vec<usize>,
    pub(super) scalar_aggregate: bool,
}

pub(super) fn pull(plan: LogicalPlan) -> Result<Pulled> {
    match plan {
        LogicalPlan::Empty {
            schema,
            produce_one_row,
        } => Ok(Pulled {
            old_to_new: identity(schema.arrow().fields().len()),
            plan: LogicalPlan::Empty {
                produce_one_row,
                schema,
            },
            correlations: Vec::new(),
            scalar_aggregate: false,
        }),
        LogicalPlan::Scan {
            table_name,
            provider,
            statistics,
            projection,
            pushed_filter,
            limit,
            schema,
        } => {
            if pushed_filter
                .as_ref()
                .is_some_and(BoundExpr::contains_outer_ref)
            {
                return Err(Error::Internal(
                    "outer reference was pushed into a scan before decorrelation".into(),
                ));
            }
            let old_to_new = identity(schema.arrow().fields().len());
            Ok(Pulled {
                plan: LogicalPlan::Scan {
                    table_name,
                    provider,
                    statistics,
                    projection,
                    pushed_filter,
                    limit,
                    schema,
                },
                correlations: Vec::new(),
                old_to_new,
                scalar_aggregate: false,
            })
        }
        LogicalPlan::Filter {
            input,
            mut predicate,
            schema,
        } => pull_filter(*input, &mut predicate, schema),
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => pull_projection(*input, expressions, schema),
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => pull_aggregate(*input, group_exprs, aggregate_exprs, schema),
        LogicalPlan::Append { inputs, schema } => {
            let mut rewritten = Vec::with_capacity(inputs.len());
            for input in inputs {
                let child = pull(input)?;
                if !child.correlations.is_empty() {
                    return Err(Error::Unsupported(
                        "set operations in a correlated subquery are not supported".into(),
                    ));
                }
                rewritten.push(child.plan);
            }
            Ok(Pulled {
                old_to_new: identity(schema.arrow().fields().len()),
                plan: LogicalPlan::Append {
                    inputs: rewritten,
                    schema,
                },
                correlations: Vec::new(),
                scalar_aggregate: false,
            })
        }
        LogicalPlan::Repeat {
            input,
            count,
            schema,
        } => {
            let child = pull(*input)?;
            if !child.correlations.is_empty() || count.contains_outer_ref() {
                return Err(Error::Unsupported(
                    "multiset set operations in a correlated subquery are not supported".into(),
                ));
            }
            Ok(Pulled {
                old_to_new: identity(schema.arrow().fields().len()),
                plan: LogicalPlan::Repeat {
                    input: Box::new(child.plan),
                    count,
                    schema,
                },
                correlations: Vec::new(),
                scalar_aggregate: false,
            })
        }
        LogicalPlan::Window {
            input,
            expressions,
            schema,
        } => {
            let child = pull(*input)?;
            if !child.correlations.is_empty() {
                return Err(Error::Unsupported(
                    "window functions in a correlated subquery are not supported".into(),
                ));
            }
            Ok(Pulled {
                old_to_new: identity(schema.arrow().fields().len()),
                plan: LogicalPlan::Window {
                    input: Box::new(child.plan),
                    expressions,
                    schema,
                },
                correlations: Vec::new(),
                scalar_aggregate: false,
            })
        }
        LogicalPlan::Sort {
            input,
            mut expressions,
            fetch,
            schema: _,
        } => {
            let child = pull(*input)?;
            for expression in &mut expressions {
                remap_columns(&mut expression.expr, &child.old_to_new)?;
            }
            let old_to_new = child.old_to_new.clone();
            let schema = child.plan.schema().clone();
            Ok(Pulled {
                plan: LogicalPlan::Sort {
                    input: Box::new(child.plan),
                    expressions,
                    fetch,
                    schema,
                },
                correlations: child.correlations,
                old_to_new,
                scalar_aggregate: child.scalar_aggregate,
            })
        }
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            schema,
        } => {
            let child = pull(*input)?;
            if !child.correlations.is_empty() {
                return Err(Error::Unsupported(
                    "LIMIT in a correlated subquery is not supported".into(),
                ));
            }
            Ok(Pulled {
                plan: LogicalPlan::Limit {
                    input: Box::new(child.plan),
                    offset,
                    limit,
                    schema,
                },
                correlations: Vec::new(),
                old_to_new: child.old_to_new,
                scalar_aggregate: child.scalar_aggregate,
            })
        }
        LogicalPlan::Scalarize { input, schema } => {
            let child = pull(*input)?;
            if !child.correlations.is_empty() {
                return Err(Error::Unsupported(
                    "nested scalarization in a correlated subquery is not supported".into(),
                ));
            }
            Ok(Pulled {
                plan: LogicalPlan::Scalarize {
                    input: Box::new(child.plan),
                    schema,
                },
                correlations: Vec::new(),
                old_to_new: vec![0],
                scalar_aggregate: false,
            })
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        } => pull_join(
            *left,
            *right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        ),
        LogicalPlan::DependentJoin { .. } => Err(Error::Internal(
            "nested DependentJoin reached correlation-key extraction".into(),
        )),
    }
}

fn pull_filter(
    input: LogicalPlan,
    predicate: &mut BoundExpr,
    _schema: PlanSchema,
) -> Result<Pulled> {
    let mut child = pull(input)?;
    remap_columns(predicate, &child.old_to_new)?;
    let mut terms = Vec::new();
    split_and(predicate.clone(), &mut terms);
    let mut local = Vec::new();
    for term in terms {
        match classify(&term)? {
            Some(correlation) => child.correlations.push(correlation),
            None => local.push(term),
        }
    }
    let plan = if let Some(predicate) = combine_and(local)? {
        let schema = child.plan.schema().clone();
        LogicalPlan::Filter {
            input: Box::new(child.plan),
            predicate,
            schema,
        }
    } else {
        child.plan
    };
    Ok(Pulled {
        plan,
        correlations: child.correlations,
        old_to_new: child.old_to_new,
        scalar_aggregate: child.scalar_aggregate,
    })
}

fn classify(term: &BoundExpr) -> Result<Option<Correlation>> {
    if !term.contains_outer_ref() {
        return Ok(None);
    }
    if let ExprKind::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = &term.kind
    {
        let left_outer = left.contains_outer_ref();
        let right_outer = right.contains_outer_ref();
        let mut left_columns = Vec::new();
        let mut right_columns = Vec::new();
        left.referenced_columns(&mut left_columns);
        right.referenced_columns(&mut right_columns);
        if left_outer && !right_outer && left_columns.is_empty() && !right_columns.is_empty() {
            return Ok(Some(Correlation::Key {
                outer: (**left).clone(),
                inner: (**right).clone(),
            }));
        }
        if right_outer && !left_outer && right_columns.is_empty() && !left_columns.is_empty() {
            return Ok(Some(Correlation::Key {
                outer: (**right).clone(),
                inner: (**left).clone(),
            }));
        }
    }
    Ok(Some(Correlation::Residual(term.clone())))
}

fn pull_projection(
    input: LogicalPlan,
    mut expressions: Vec<BoundExpr>,
    schema: PlanSchema,
) -> Result<Pulled> {
    let child = pull(input)?;
    for expression in &mut expressions {
        remap_columns(expression, &child.old_to_new)?;
        if expression.contains_outer_ref() {
            return Err(Error::Unsupported(
                "an outer reference in a correlated subquery SELECT list is not supported".into(),
            ));
        }
    }
    let original_width = expressions.len();
    let mut correlations = Vec::with_capacity(child.correlations.len());
    for correlation in child.correlations {
        match correlation {
            Correlation::Key { outer, inner } => {
                let index = expose_expression(&mut expressions, inner);
                let exposed = expressions[index].clone();
                correlations.push(Correlation::Key {
                    outer,
                    inner: BoundExpr::column(index, exposed.data_type, exposed.display_name),
                });
            }
            Correlation::Residual(mut residual) => {
                let child_width = child.plan.schema().arrow().fields().len();
                let mut mapping = vec![usize::MAX; child_width];
                for (index, field) in child.plan.schema().arrow().fields().iter().enumerate() {
                    let exposed =
                        BoundExpr::column(index, field.data_type().clone(), field.name().clone());
                    mapping[index] = expose_expression(&mut expressions, exposed);
                }
                remap_columns(&mut residual, &mapping)?;
                correlations.push(Correlation::Residual(residual));
            }
        }
    }
    let schema = extended_projection_schema(&schema, &expressions[original_width..]);
    Ok(Pulled {
        plan: LogicalPlan::Projection {
            input: Box::new(child.plan),
            expressions,
            schema,
        },
        correlations,
        old_to_new: identity(original_width),
        scalar_aggregate: child.scalar_aggregate,
    })
}

fn pull_aggregate(
    input: LogicalPlan,
    mut group_exprs: Vec<BoundExpr>,
    mut aggregate_exprs: Vec<crate::sql::AggregateExpr>,
    _schema: PlanSchema,
) -> Result<Pulled> {
    let child = pull(input)?;
    for expression in &mut group_exprs {
        remap_columns(expression, &child.old_to_new)?;
    }
    for aggregate in &mut aggregate_exprs {
        if let Some(expression) = &mut aggregate.expr {
            remap_columns(expression, &child.old_to_new)?;
        }
    }
    let original_groups = group_exprs.len();
    let original_aggregates = aggregate_exprs.len();
    let mut correlations = Vec::with_capacity(child.correlations.len());
    let mut residuals = Vec::new();
    for correlation in child.correlations {
        match correlation {
            Correlation::Key { outer, inner } => {
                let index = expose_expression(&mut group_exprs, inner);
                let group = group_exprs[index].clone();
                correlations.push(Correlation::Key {
                    outer,
                    inner: BoundExpr::column(index, group.data_type, group.display_name),
                });
            }
            Correlation::Residual(residual) => residuals.push(residual),
        }
    }
    if !residuals.is_empty() {
        let input_width = child.plan.schema().arrow().fields().len();
        let mut mapping = vec![usize::MAX; input_width];
        for (group_index, group) in group_exprs.iter().enumerate() {
            if let ExprKind::Column(input_index) = group.kind
                && input_index < mapping.len()
            {
                mapping[input_index] = group_index;
            }
        }
        for mut residual in residuals {
            remap_columns(&mut residual, &mapping).map_err(|_| {
                Error::Unsupported(
                    "a cross-side residual below a correlated aggregate may reference only exposed correlation/group key columns; pre-aggregate outer-dependent filtering is not supported"
                        .into(),
                )
            })?;
            correlations.push(Correlation::Residual(residual));
        }
    }
    let added_groups = group_exprs.len() - original_groups;
    let mut old_to_new = identity(original_groups);
    old_to_new.extend((0..original_aggregates).map(|index| original_groups + added_groups + index));
    let schema = aggregate_schema(&group_exprs, &aggregate_exprs);
    Ok(Pulled {
        plan: LogicalPlan::Aggregate {
            input: Box::new(child.plan),
            group_exprs,
            aggregate_exprs,
            schema,
        },
        correlations,
        old_to_new,
        scalar_aggregate: original_groups == 0,
    })
}

#[allow(clippy::too_many_arguments)]
fn pull_join(
    left: LogicalPlan,
    right: LogicalPlan,
    mut on: Vec<(BoundExpr, BoundExpr)>,
    null_equal_keys: bool,
    mut residual: Option<BoundExpr>,
    mut null_aware: Option<(BoundExpr, BoundExpr)>,
    join_type: JoinType,
    schema: PlanSchema,
) -> Result<Pulled> {
    let left = pull(left)?;
    let right = pull(right)?;
    if !left.correlations.is_empty() || !right.correlations.is_empty() {
        return Err(Error::Unsupported(
            "correlation predicates inside a subquery JOIN input are not supported; place them in WHERE"
                .into(),
        ));
    }
    for (left_key, right_key) in &mut on {
        remap_columns(left_key, &left.old_to_new)?;
        remap_columns(right_key, &right.old_to_new)?;
    }
    if let Some((left_value, right_value)) = &mut null_aware {
        remap_columns(left_value, &left.old_to_new)?;
        remap_columns(right_value, &right.old_to_new)?;
    }
    if let Some(residual) = &mut residual {
        let old_left_width = left.old_to_new.len();
        let new_left_width = left.plan.schema().arrow().fields().len();
        let mut mapping = left.old_to_new.clone();
        mapping.extend(
            right
                .old_to_new
                .iter()
                .map(|index| new_left_width.saturating_add(*index)),
        );
        debug_assert_eq!(mapping.len(), old_left_width + right.old_to_new.len());
        remap_columns(residual, &mapping)?;
    }
    let old_to_new = join_output_mapping(&left, &right, join_type);
    let rebuilt_schema = join_schema(join_type, left.plan.schema(), right.plan.schema(), &schema);
    Ok(Pulled {
        plan: LogicalPlan::Join {
            left: Box::new(left.plan),
            right: Box::new(right.plan),
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema: rebuilt_schema,
        },
        correlations: Vec::new(),
        old_to_new,
        scalar_aggregate: false,
    })
}

fn expose_expression(expressions: &mut Vec<BoundExpr>, expression: BoundExpr) -> usize {
    if let Some(index) = expressions.iter().position(|item| item == &expression) {
        index
    } else {
        expressions.push(expression);
        expressions.len() - 1
    }
}

fn extended_projection_schema(original: &PlanSchema, appended: &[BoundExpr]) -> PlanSchema {
    let mut fields = original
        .arrow()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.extend(appended.iter().map(|expression| {
        Arc::new(Field::new(
            expression.display_name.clone(),
            expression.data_type.clone(),
            true,
        ))
    }));
    let mut qualifiers = (0..original.arrow().fields().len())
        .map(|index| original.qualifier(index).map(str::to_owned))
        .collect::<Vec<_>>();
    qualifiers.resize(fields.len(), None);
    let mut visible = (0..original.arrow().fields().len())
        .map(|index| original.is_visible(index))
        .collect::<Vec<_>>();
    visible.resize(fields.len(), false);
    PlanSchema::new_with_visibility(Arc::new(Schema::new(fields)), qualifiers, visible)
}

fn aggregate_schema(groups: &[BoundExpr], aggregates: &[crate::sql::AggregateExpr]) -> PlanSchema {
    let fields = groups
        .iter()
        .map(|expression| {
            Field::new(
                expression.display_name.clone(),
                expression.data_type.clone(),
                true,
            )
        })
        .chain(aggregates.iter().map(|aggregate| {
            Field::new(
                aggregate.display_name.clone(),
                aggregate.data_type.clone(),
                true,
            )
        }))
        .collect::<Vec<_>>();
    PlanSchema::unqualified(Arc::new(Schema::new(fields)))
}

fn join_output_mapping(left: &Pulled, right: &Pulled, join_type: JoinType) -> Vec<usize> {
    let mut mapping = left.old_to_new.clone();
    if matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full | JoinType::LeftSingle
    ) {
        let left_width = left.plan.schema().arrow().fields().len();
        mapping.extend(
            right
                .old_to_new
                .iter()
                .map(|index| left_width.saturating_add(*index)),
        );
    } else if join_type == JoinType::Mark {
        mapping.push(left.plan.schema().arrow().fields().len());
    }
    mapping
}

fn join_schema(
    join_type: JoinType,
    left: &PlanSchema,
    right: &PlanSchema,
    original: &PlanSchema,
) -> PlanSchema {
    match join_type {
        JoinType::Inner => PlanSchema::join(left, right),
        JoinType::Left | JoinType::LeftSingle => PlanSchema::left_join(left, right),
        JoinType::Right => PlanSchema::right_join(left, right),
        JoinType::Full => PlanSchema::full_join(left, right),
        JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti => left.clone(),
        JoinType::Mark => {
            let marker =
                Arc::clone(&original.arrow().fields()[original.arrow().fields().len() - 1]);
            let marker = PlanSchema::unqualified(Arc::new(Schema::new(vec![marker])));
            PlanSchema::join(left, &marker)
        }
    }
}

fn identity(width: usize) -> Vec<usize> {
    (0..width).collect()
}

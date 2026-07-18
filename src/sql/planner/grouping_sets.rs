use std::collections::HashSet;

use std::ops::ControlFlow;

use sqlparser::{
    ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem, Spanned,
        VisitMut, VisitorMut,
    },
    tokenizer::Span,
};

use crate::sql::{BoundExpr, LogicalPlan, PlanSchema, ScalarValue};
use crate::{Error, Result};

use super::super::{
    aggregate::plan_aggregate_projection_with_groups, binder::bind_expr_scoped,
    coercion::cast_if_needed,
};

const MAX_GROUPING_SETS: usize = 4096;

pub(super) struct ExpandedGrouping {
    pub(super) universe: Vec<Expr>,
    pub(super) sets: Vec<Vec<usize>>,
}

pub(super) fn expand(group_by: &GroupByExpr) -> Result<Option<ExpandedGrouping>> {
    let GroupByExpr::Expressions(expressions, modifiers) = group_by else {
        return Err(Error::Unsupported("GROUP BY ALL is not supported".into()));
    };
    if !modifiers.is_empty() {
        return Err(Error::Unsupported(
            "GROUP BY trailing modifiers are not supported; use ROLLUP, CUBE, or GROUPING SETS"
                .into(),
        ));
    }
    let special = expressions.iter().any(|expression| {
        matches!(
            expression,
            Expr::GroupingSets(_) | Expr::Rollup(_) | Expr::Cube(_)
        )
    });
    if !special {
        return Ok(None);
    }

    let mut raw_sets = vec![Vec::new()];
    for expression in expressions {
        let choices = choices(expression)?;
        raw_sets = product(raw_sets, choices)?;
    }
    let mut universe = Vec::<Expr>::new();
    for set in &raw_sets {
        for expression in set {
            if !universe.contains(expression) {
                universe.push(expression.clone());
            }
        }
    }
    let sets = raw_sets
        .into_iter()
        .map(|set| {
            let mut seen = HashSet::new();
            set.into_iter()
                .map(|expression| {
                    universe
                        .iter()
                        .position(|candidate| candidate == &expression)
                        .ok_or_else(|| {
                            Error::Internal("grouping expression was lost from its universe".into())
                        })
                })
                .filter_map(|index| match index {
                    Ok(index) if seen.insert(index) => Some(Ok(index)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(ExpandedGrouping { universe, sets }))
}

/// Collapses grouping expressions that become identical only after ordinal
/// and projection-alias resolution, then rewrites every grouping-set index to
/// the canonical slot. Duplicate grouping sets remain duplicate branches, as
/// required by SQL multiplicity semantics.
pub(super) fn normalize_resolved(universe: &mut Vec<Expr>, sets: &mut [Vec<usize>]) -> Result<()> {
    let resolved = std::mem::take(universe);
    let mut remap = Vec::with_capacity(resolved.len());
    for expression in resolved {
        let index = match universe
            .iter()
            .position(|candidate| candidate == &expression)
        {
            Some(index) => index,
            None => {
                let index = universe.len();
                universe.push(expression);
                index
            }
        };
        remap.push(index);
    }
    for set in sets {
        let mut seen = HashSet::new();
        let mut normalized = Vec::with_capacity(set.len());
        for index in set.iter().copied() {
            let index = remap.get(index).copied().ok_or_else(|| {
                Error::Internal(format!(
                    "grouping set references missing resolved expression {index}"
                ))
            })?;
            if seen.insert(index) {
                normalized.push(index);
            }
        }
        *set = normalized;
    }
    Ok(())
}

pub(super) fn plan(
    input: LogicalPlan,
    group_ast: &[Expr],
    sets: &[Vec<usize>],
    items: &[SelectItem],
    having: Option<&Expr>,
    outer: Option<&PlanSchema>,
) -> Result<LogicalPlan> {
    let base = group_ast
        .iter()
        .map(|expression| bind_expr_scoped(expression, input.schema(), outer))
        .collect::<Result<Vec<_>>>()?;
    let visible_width = sets
        .iter()
        .flat_map(|set| set.iter().copied())
        .max()
        .map_or(0, |index| index + 1)
        .min(base.len());
    let mut branches = Vec::with_capacity(sets.len());
    for set in sets {
        let active = set.iter().copied().collect::<HashSet<_>>();
        let groups = base
            .iter()
            .enumerate()
            .map(|(index, expression)| {
                if index >= visible_width || active.contains(&index) {
                    expression.clone()
                } else {
                    let mut null = cast_if_needed(
                        BoundExpr::literal(ScalarValue::Null),
                        &expression.data_type,
                    );
                    null.display_name = expression.display_name.clone();
                    null
                }
            })
            .collect();
        let mut branch_items = items.to_vec();
        let mut branch_having = having.cloned();
        rewrite_grouping_functions(
            &mut branch_items,
            branch_having.as_mut(),
            group_ast,
            visible_width,
            &active,
        )?;
        branches.push(plan_aggregate_projection_with_groups(
            input.clone(),
            group_ast,
            groups,
            &branch_items,
            branch_having.as_ref(),
        )?);
    }
    let schema = branches
        .first()
        .map(|branch| branch.schema().clone())
        .ok_or_else(|| Error::InvalidArgument("GROUPING SETS produced no grouping set".into()))?;
    for branch in branches.iter_mut().skip(1) {
        let fields = branch.schema().arrow().fields();
        if fields.len() != schema.arrow().fields().len()
            || fields
                .iter()
                .zip(schema.arrow().fields())
                .any(|(source, target)| source.data_type() != target.data_type())
        {
            return Err(Error::Internal(format!(
                "grouping-set branches produced incompatible schemas: expected {:?}, got {:?}",
                schema.arrow(),
                branch.schema().arrow(),
            )));
        }
        if branch.schema().arrow() != schema.arrow() {
            let expressions = fields
                .iter()
                .zip(schema.arrow().fields())
                .enumerate()
                .map(|(index, (source, target))| {
                    BoundExpr::column(index, source.data_type().clone(), target.name())
                })
                .collect();
            *branch = LogicalPlan::Projection {
                input: Box::new(branch.clone()),
                expressions,
                schema: schema.clone(),
            };
        }
    }
    Ok(LogicalPlan::Append {
        inputs: branches,
        schema,
    })
}

fn rewrite_grouping_functions(
    items: &mut [SelectItem],
    having: Option<&mut Expr>,
    groups: &[Expr],
    visible_width: usize,
    active: &HashSet<usize>,
) -> Result<()> {
    struct Rewriter<'a> {
        groups: &'a [Expr],
        visible_width: usize,
        active: &'a HashSet<usize>,
        query_depth: usize,
    }

    impl VisitorMut for Rewriter<'_> {
        type Break = Error;

        fn pre_visit_query(
            &mut self,
            _query: &mut sqlparser::ast::Query,
        ) -> ControlFlow<Self::Break> {
            self.query_depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(
            &mut self,
            _query: &mut sqlparser::ast::Query,
        ) -> ControlFlow<Self::Break> {
            self.query_depth = self.query_depth.saturating_sub(1);
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
            if self.query_depth != 0 {
                return ControlFlow::Continue(());
            }
            let Expr::Function(function) = expression else {
                return ControlFlow::Continue(());
            };
            let name = function.name.to_string().to_ascii_lowercase();
            if !matches!(name.as_str(), "grouping" | "grouping_id") {
                return ControlFlow::Continue(());
            }
            if function.over.is_some()
                || function.filter.is_some()
                || function.null_treatment.is_some()
                || !function.within_group.is_empty()
                || !matches!(function.parameters, FunctionArguments::None)
            {
                return ControlFlow::Break(Error::InvalidArgument(
                    "GROUPING does not accept aggregate or window modifiers".into(),
                ));
            }
            let FunctionArguments::List(arguments) = &function.args else {
                return ControlFlow::Break(Error::InvalidArgument(
                    "GROUPING requires parentheses".into(),
                ));
            };
            if arguments.args.is_empty()
                || arguments.args.len() > 63
                || arguments.duplicate_treatment.is_some()
                || !arguments.clauses.is_empty()
            {
                return ControlFlow::Break(Error::InvalidArgument(
                    "GROUPING requires between 1 and 63 grouping expressions".into(),
                ));
            }
            let mut mask = 0u64;
            for argument in &arguments.args {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(argument)) = argument else {
                    return ControlFlow::Break(Error::InvalidArgument(
                        "GROUPING requires expression arguments".into(),
                    ));
                };
                let Some(index) = self.groups[..self.visible_width]
                    .iter()
                    .position(|group| group == argument)
                else {
                    return ControlFlow::Break(Error::InvalidArgument(format!(
                        "GROUPING argument `{argument}` is not a GROUP BY expression"
                    )));
                };
                mask = (mask << 1) | u64::from(!self.active.contains(&index));
            }
            let span = expression.span();
            *expression = Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Number(mask.to_string(), false),
                span: if span == Span::empty() {
                    Span::empty()
                } else {
                    span
                },
            });
            ControlFlow::Continue(())
        }
    }

    let mut rewriter = Rewriter {
        groups,
        visible_width,
        active,
        query_depth: 0,
    };
    for item in items {
        let expression = match item {
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => expression,
            _ => continue,
        };
        if let ControlFlow::Break(error) = expression.visit(&mut rewriter) {
            return Err(error);
        }
    }
    if let Some(having) = having
        && let ControlFlow::Break(error) = having.visit(&mut rewriter)
    {
        return Err(error);
    }
    Ok(())
}

fn choices(expression: &Expr) -> Result<Vec<Vec<Expr>>> {
    match expression {
        Expr::GroupingSets(sets) => Ok(sets.clone()),
        Expr::Rollup(elements) => Ok((0..=elements.len())
            .rev()
            .map(|length| elements[..length].iter().flatten().cloned().collect())
            .collect()),
        Expr::Cube(elements) => {
            if elements.len() >= usize::BITS as usize {
                return Err(Error::InvalidArgument(
                    "CUBE has too many grouping elements".into(),
                ));
            }
            let count = 1usize << elements.len();
            if count > MAX_GROUPING_SETS {
                return Err(Error::InvalidArgument(format!(
                    "CUBE expands to {count} grouping sets; maximum is {MAX_GROUPING_SETS}"
                )));
            }
            Ok((0..count)
                .rev()
                .map(|mask| {
                    elements
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| mask & (1 << index) != 0)
                        .flat_map(|(_, expressions)| expressions.iter().cloned())
                        .collect()
                })
                .collect())
        }
        expression => Ok(vec![vec![expression.clone()]]),
    }
}

fn product(left: Vec<Vec<Expr>>, right: Vec<Vec<Expr>>) -> Result<Vec<Vec<Expr>>> {
    let count = left
        .len()
        .checked_mul(right.len())
        .ok_or_else(|| Error::InvalidArgument("GROUPING SETS expansion overflowed usize".into()))?;
    if count > MAX_GROUPING_SETS {
        return Err(Error::InvalidArgument(format!(
            "GROUP BY expands to {count} grouping sets; maximum is {MAX_GROUPING_SETS}"
        )));
    }
    let mut output = Vec::with_capacity(count);
    for left in left {
        for right in &right {
            let mut set = left.clone();
            set.extend(right.iter().cloned());
            output.push(set);
        }
    }
    Ok(output)
}

use crate::sql::{LogicalPlan, PlanSchema};
use crate::{Error, Result};

pub(super) fn has_outer_refs(plan: &LogicalPlan) -> bool {
    let mut references = Vec::new();
    collect_outer_refs(plan, &mut references);
    !references.is_empty()
}

pub(crate) fn validate_outer_grouping(
    plan: &LogicalPlan,
    allowed: &[usize],
    outer_schema: &PlanSchema,
    location_suffix: &str,
) -> Result<()> {
    let mut references = Vec::new();
    collect_outer_refs(plan, &mut references);
    references.sort_unstable();
    references.dedup();
    for (depth, index) in references {
        if depth != 1 {
            return Err(Error::Unsupported(format!(
                "correlated subquery depth {depth} is not supported; maximum depth is 1"
            )));
        }
        if !allowed.contains(&index) {
            let name = outer_schema
                .arrow()
                .fields()
                .get(index)
                .map_or("<unknown>", |field| field.name());
            return Err(Error::InvalidArgument(format!(
                "correlated subquery references outer column '{name}', which must appear directly in GROUP BY{location_suffix}"
            )));
        }
    }
    Ok(())
}

fn collect_outer_refs(plan: &LogicalPlan, output: &mut Vec<(u8, usize)>) {
    match plan {
        LogicalPlan::Empty { .. } => {}
        LogicalPlan::Scan { pushed_filter, .. } => {
            if let Some(predicate) = pushed_filter {
                predicate.outer_references(output);
            }
        }
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            predicate.outer_references(output);
            collect_outer_refs(input, output);
        }
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            for expression in expressions {
                expression.outer_references(output);
            }
            collect_outer_refs(input, output);
        }
        LogicalPlan::Scalarize { input, .. } | LogicalPlan::Limit { input, .. } => {
            collect_outer_refs(input, output);
        }
        // Nested attachments own the OuterRefs in their right input; those
        // bind to this node's left query block and must not escape outward.
        LogicalPlan::DependentJoin { .. } => {}
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            ..
        } => {
            for expression in group_exprs {
                expression.outer_references(output);
            }
            for aggregate in aggregate_exprs {
                if let Some(expression) = &aggregate.expr {
                    expression.outer_references(output);
                }
            }
            collect_outer_refs(input, output);
        }
        LogicalPlan::Append { inputs, .. } => {
            for input in inputs {
                collect_outer_refs(input, output);
            }
        }
        LogicalPlan::Repeat { input, count, .. } => {
            count.outer_references(output);
            collect_outer_refs(input, output);
        }
        LogicalPlan::Window {
            input, expressions, ..
        } => {
            for expression in expressions {
                match &expression.function {
                    crate::sql::WindowFunction::Aggregate(aggregate) => {
                        if let Some(argument) = &aggregate.expr {
                            argument.outer_references(output);
                        }
                    }
                    crate::sql::WindowFunction::Lead { expr, default, .. }
                    | crate::sql::WindowFunction::Lag { expr, default, .. } => {
                        expr.outer_references(output);
                        default.outer_references(output);
                    }
                    crate::sql::WindowFunction::FirstValue(expr)
                    | crate::sql::WindowFunction::LastValue(expr) => {
                        expr.outer_references(output);
                    }
                    crate::sql::WindowFunction::RowNumber
                    | crate::sql::WindowFunction::Rank
                    | crate::sql::WindowFunction::DenseRank
                    | crate::sql::WindowFunction::Ntile(_)
                    | crate::sql::WindowFunction::PercentRank
                    | crate::sql::WindowFunction::CumeDist => {}
                }
                for partition in &expression.partition_by {
                    partition.outer_references(output);
                }
                for order in &expression.order_by {
                    order.expr.outer_references(output);
                }
            }
            collect_outer_refs(input, output);
        }
        LogicalPlan::Sort {
            input, expressions, ..
        } => {
            for expression in expressions {
                expression.expr.outer_references(output);
            }
            collect_outer_refs(input, output);
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            ..
        } => {
            collect_outer_refs(left, output);
            collect_outer_refs(right, output);
            for (left, right) in on {
                left.outer_references(output);
                right.outer_references(output);
            }
            if let Some(residual) = residual {
                residual.outer_references(output);
            }
            if let Some((left, right)) = null_aware {
                left.outer_references(output);
                right.outer_references(output);
            }
        }
    }
}

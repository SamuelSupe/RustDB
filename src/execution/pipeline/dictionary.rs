use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::{AggregateExpr, BoundExpr, ExprKind};

use super::{FusedPipeline, PipelineOperator};

#[derive(Default)]
pub(super) struct GroupDictionaryPlan {
    pub(super) scan_columns: Vec<usize>,
    pub(super) output_columns: Vec<usize>,
}

impl GroupDictionaryPlan {
    pub(super) fn enabled(&self) -> bool {
        !self.scan_columns.is_empty()
    }
}

/// Finds source columns whose dictionary representation may safely cross the
/// fused pipeline boundary directly into Aggregate. All other shapes retain
/// the canonical Utf8/Binary scan contract.
pub(super) fn plan(
    pipeline: &FusedPipeline,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
) -> GroupDictionaryPlan {
    let Some(projection) = pipeline
        .scan
        .projection
        .as_deref()
        .filter(|projection| !projection.is_empty())
    else {
        return GroupDictionaryPlan::default();
    };
    let Some((
        PipelineOperator::Projection {
            expressions,
            schema,
        },
        filters,
    )) = pipeline.operators.split_last()
    else {
        return GroupDictionaryPlan::default();
    };
    if !filters
        .iter()
        .all(|operator| matches!(operator, PipelineOperator::Filter(_)))
    {
        return GroupDictionaryPlan::default();
    }

    let mut forbidden_outputs = HashSet::new();
    for aggregate in aggregates {
        if let Some(expression) = &aggregate.expr {
            collect_columns(expression, &mut forbidden_outputs);
        }
    }
    for group in groups {
        if !matches!(&group.kind, ExprKind::Column(_)) {
            collect_columns(group, &mut forbidden_outputs);
        }
    }

    let mut filter_columns = HashSet::new();
    if let Some(filter) = &pipeline.scan.pushed_filter {
        collect_columns(filter, &mut filter_columns);
    }
    for operator in filters {
        let PipelineOperator::Filter(filter) = operator else {
            unreachable!("filter prefix was checked above")
        };
        collect_columns(filter, &mut filter_columns);
    }

    let candidate_outputs = groups
        .iter()
        .filter_map(|group| match (&group.kind, &group.data_type) {
            (ExprKind::Column(index), data_type) if dictionary_value_type(data_type) => {
                Some(*index)
            }
            _ => None,
        })
        .filter(|index| !forbidden_outputs.contains(index))
        .collect::<HashSet<_>>();

    let mut scan_columns = Vec::new();
    let mut output_columns = Vec::new();
    for output_index in candidate_outputs.iter().copied() {
        let Some(expression) = expressions.get(output_index) else {
            continue;
        };
        let ExprKind::Column(source_index) = &expression.kind else {
            continue;
        };
        let Some(output_field) = schema.fields().get(output_index) else {
            continue;
        };
        let Some(source_field) = pipeline.scan.schema.fields().get(*source_index) else {
            continue;
        };
        if expression.data_type != *output_field.data_type()
            || expression.data_type != *source_field.data_type()
            || !dictionary_value_type(&expression.data_type)
            || !projection.contains(source_index)
            || filter_columns.contains(source_index)
            || source_used_outside_candidates(expressions, *source_index, &candidate_outputs)
        {
            continue;
        }
        scan_columns.push(*source_index);
        output_columns.push(output_index);
    }
    scan_columns.sort_unstable();
    scan_columns.dedup();
    output_columns.sort_unstable();
    output_columns.dedup();
    GroupDictionaryPlan {
        scan_columns,
        output_columns,
    }
}

fn dictionary_value_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

fn collect_columns(expression: &BoundExpr, output: &mut HashSet<usize>) {
    let mut columns = Vec::new();
    expression.referenced_columns(&mut columns);
    output.extend(columns);
}

fn source_used_outside_candidates(
    expressions: &[BoundExpr],
    source_index: usize,
    candidate_outputs: &HashSet<usize>,
) -> bool {
    expressions.iter().enumerate().any(|(output, expression)| {
        let mut columns = Vec::new();
        expression.referenced_columns(&mut columns);
        columns.contains(&source_index)
            && !(candidate_outputs.contains(&output)
                && matches!(&expression.kind, ExprKind::Column(index) if *index == source_index))
    })
}

#[cfg(test)]
#[path = "dictionary/tests.rs"]
mod tests;

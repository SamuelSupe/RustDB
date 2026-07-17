use std::sync::Arc;

use arrow::datatypes::{Field, Schema};

use crate::sql::{
    AggregateExpr, BoundExpr, ExprKind, LogicalPlan, PlanSchema, UNMATERIALIZED_FIELD_KEY,
};

mod remap;

/// Places a narrow direct-column projection at a Scan/Filter pipeline breaker.
/// The rewrite is deliberately limited to shapes the fused pipeline can keep
/// compact all the way into Aggregate.
pub(super) fn compact(
    input: &mut Box<LogicalPlan>,
    groups: &mut Vec<BoundExpr>,
    aggregates: &mut Vec<AggregateExpr>,
) -> bool {
    let required = remap::required_columns(groups, aggregates);

    if let LogicalPlan::Projection {
        input: source,
        expressions,
        schema,
    } = input.as_ref()
    {
        let Some(rewrite) = compact_direct_projection(source, expressions, schema, &required)
        else {
            return false;
        };
        let Some((new_groups, new_aggregates)) =
            remap::aggregate(groups, aggregates, &rewrite.remap)
        else {
            return false;
        };
        let LogicalPlan::Projection {
            expressions,
            schema,
            ..
        } = input.as_mut()
        else {
            unreachable!("projection shape was checked above")
        };
        *expressions = rewrite.expressions;
        *schema = rewrite.schema;
        *groups = new_groups;
        *aggregates = new_aggregates;
        return true;
    }

    let Some(has_filter) = scan_filter_chain(input) else {
        return false;
    };
    // A bare metadata-only Scan already emits the desired zero-column batch.
    if required.is_empty() && !has_filter {
        return false;
    }
    let Some(rewrite) = direct_projection(input.schema(), &required) else {
        return false;
    };
    let Some((new_groups, new_aggregates)) = remap::aggregate(groups, aggregates, &rewrite.remap)
    else {
        return false;
    };

    let source = std::mem::replace(
        input,
        Box::new(LogicalPlan::Empty {
            produce_one_row: false,
            schema: PlanSchema::empty(),
        }),
    );
    **input = LogicalPlan::Projection {
        input: source,
        expressions: rewrite.expressions,
        schema: rewrite.schema,
    };
    *groups = new_groups;
    *aggregates = new_aggregates;
    true
}

struct ProjectionRewrite {
    expressions: Vec<BoundExpr>,
    schema: PlanSchema,
    remap: Vec<Option<usize>>,
}

fn direct_projection(schema: &PlanSchema, required: &[usize]) -> Option<ProjectionRewrite> {
    let mut remap = vec![None; schema.arrow().fields().len()];
    let mut expressions = Vec::with_capacity(required.len());
    for (new_index, old_index) in required.iter().copied().enumerate() {
        let field = schema.arrow().fields().get(old_index)?;
        remap[old_index] = Some(new_index);
        expressions.push(BoundExpr::column(
            old_index,
            field.data_type().clone(),
            field.name(),
        ));
    }
    Some(ProjectionRewrite {
        expressions,
        schema: select_schema(schema, required),
        remap,
    })
}

fn compact_direct_projection(
    source: &LogicalPlan,
    expressions: &[BoundExpr],
    schema: &PlanSchema,
    required: &[usize],
) -> Option<ProjectionRewrite> {
    scan_filter_chain(source)?;
    if expressions.len() != schema.arrow().fields().len() {
        return None;
    }
    let source_width = source.schema().arrow().fields().len();
    let direct = expressions
        .iter()
        .enumerate()
        .map(|(output_index, expression)| {
            let ExprKind::Column(source_index) = &expression.kind else {
                return None;
            };
            let source_field = source.schema().arrow().fields().get(*source_index)?;
            let output_field = schema.arrow().fields().get(output_index)?;
            (expression.data_type == *source_field.data_type()
                && output_field.data_type() == source_field.data_type()
                && *source_index < source_width)
                .then_some(*source_index)
        })
        .collect::<Option<Vec<_>>>()?;

    let mut remap = vec![None; expressions.len()];
    let mut unique_sources = Vec::new();
    let mut selected_outputs = Vec::new();
    let mut selected_expressions = Vec::new();
    for old_index in required.iter().copied() {
        let source_index = *direct.get(old_index)?;
        let new_index = if let Some(index) = unique_sources
            .iter()
            .position(|existing| *existing == source_index)
        {
            index
        } else {
            let index = unique_sources.len();
            unique_sources.push(source_index);
            selected_outputs.push(old_index);
            selected_expressions.push(expressions.get(old_index)?.clone());
            index
        };
        remap[old_index] = Some(new_index);
    }

    Some(ProjectionRewrite {
        expressions: selected_expressions,
        schema: select_schema(schema, &selected_outputs),
        remap,
    })
}

fn scan_filter_chain(plan: &LogicalPlan) -> Option<bool> {
    match plan {
        LogicalPlan::Scan { .. } => Some(false),
        LogicalPlan::Filter { input, schema, .. }
            if same_physical_schema(schema, input.schema()) =>
        {
            scan_filter_chain(input).map(|_| true)
        }
        _ => None,
    }
}

fn same_physical_schema(left: &PlanSchema, right: &PlanSchema) -> bool {
    left.arrow().fields().len() == right.arrow().fields().len()
        && left
            .arrow()
            .fields()
            .iter()
            .zip(right.arrow().fields())
            .all(|(left, right)| left.data_type() == right.data_type())
}

fn select_schema(schema: &PlanSchema, indices: &[usize]) -> PlanSchema {
    let fields = indices
        .iter()
        .map(|index| materialized_field(schema.arrow().field(*index)))
        .collect::<Vec<_>>();
    let qualifiers = indices
        .iter()
        .map(|index| schema.qualifier(*index).map(str::to_owned))
        .collect();
    let visible = indices
        .iter()
        .map(|index| schema.is_visible(*index))
        .collect();
    PlanSchema::new_with_visibility(Arc::new(Schema::new(fields)), qualifiers, visible)
}

fn materialized_field(field: &Field) -> Arc<Field> {
    let mut metadata = field.metadata().clone();
    metadata.remove(UNMATERIALIZED_FIELD_KEY);
    Arc::new(field.clone().with_metadata(metadata))
}

#[cfg(test)]
#[path = "aggregate_input/tests.rs"]
mod tests;

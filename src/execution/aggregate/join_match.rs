use arrow::datatypes::DataType;

use crate::sql::{
    AggregateExpr, AggregateFunction, BoundExpr, ExprKind, JoinType, LogicalPlan, PlanSchema,
    field_is_materialized,
};

/// An ownership-preserving match for the first Join-to-global-Aggregate sink.
///
/// The executor still owns both Join inputs and the equality keys. Aggregate
/// arguments are rewritten to direct columns in the Join output schema, so a
/// later selection sink can choose the probe or build array from the index.
#[allow(dead_code)]
pub(in crate::execution) struct MatchedJoinAggregate {
    pub(in crate::execution) left: Box<LogicalPlan>,
    pub(in crate::execution) right: Box<LogicalPlan>,
    pub(in crate::execution) on: Vec<(BoundExpr, BoundExpr)>,
    pub(in crate::execution) schema: PlanSchema,
    pub(in crate::execution) aggregates: Vec<AggregateExpr>,
}

/// Returns the original input unchanged when the complete shape is not safe
/// for selection-aware execution.
// Keeping the rejected plan by value avoids cloning or boxing this optimizer
// matcher's ownership-preserving fallback.
#[allow(dead_code, clippy::result_large_err)]
pub(in crate::execution) fn match_plan(
    input: LogicalPlan,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
) -> std::result::Result<MatchedJoinAggregate, LogicalPlan> {
    let Some(remapped) = inspect(&input, groups, aggregates) else {
        return Err(input);
    };

    let mut current = input;
    let join = loop {
        match current {
            LogicalPlan::Projection { input, .. } => current = *input,
            join @ LogicalPlan::Join { .. } => break join,
            _ => unreachable!("inspect accepts only a Projection chain over Join"),
        }
    };
    let LogicalPlan::Join {
        left,
        right,
        on,
        null_equal_keys: false,
        residual: None,
        null_aware: None,
        join_type: JoinType::Inner,
        schema,
    } = join
    else {
        unreachable!("inspect accepts only a simple inner equality Join")
    };
    Ok(MatchedJoinAggregate {
        left,
        right,
        on,
        schema,
        aggregates: remapped,
    })
}

fn inspect(
    input: &LogicalPlan,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
) -> Option<Vec<AggregateExpr>> {
    if !groups.is_empty() || aggregates.is_empty() {
        return None;
    }
    let mut remapped = aggregates.to_vec();
    if !remapped.iter().all(valid_aggregate) {
        return None;
    }

    let mut plan = input;
    while let LogicalPlan::Projection {
        input,
        expressions,
        schema,
    } = plan
    {
        if !valid_projection(expressions, schema, input.schema()) {
            return None;
        }
        for aggregate in &mut remapped {
            let Some(argument) = aggregate.expr.as_mut() else {
                continue;
            };
            let ExprKind::Column(index) = &argument.kind else {
                unreachable!("valid_aggregate accepts only direct SUM columns")
            };
            let source = expressions.get(*index)?;
            let ExprKind::Column(source_index) = &source.kind else {
                unreachable!("valid_projection accepts only direct columns")
            };
            *argument = BoundExpr::column(
                *source_index,
                source.data_type.clone(),
                source.display_name.clone(),
            );
        }
        plan = input;
    }

    let LogicalPlan::Join {
        left,
        right,
        on,
        null_equal_keys,
        residual,
        null_aware,
        join_type,
        schema,
    } = plan
    else {
        return None;
    };
    if *join_type != JoinType::Inner
        || *null_equal_keys
        || residual.is_some()
        || null_aware.is_some()
        || on.is_empty()
        || !valid_join_schema(schema, left.schema(), right.schema())
        || !on
            .iter()
            .all(|(left_key, right_key)| valid_key_pair(left_key, right_key, left, right))
    {
        return None;
    }

    for aggregate in &mut remapped {
        let Some(argument) = aggregate.expr.as_mut() else {
            continue;
        };
        let ExprKind::Column(index) = &argument.kind else {
            unreachable!("SUM arguments remain direct columns while remapping")
        };
        let field = schema.arrow().fields().get(*index)?;
        if field.data_type() != &argument.data_type || !field_is_materialized(field) {
            return None;
        }
        *argument = BoundExpr::column(*index, field.data_type().clone(), field.name());
    }
    Some(remapped)
}

fn valid_aggregate(aggregate: &AggregateExpr) -> bool {
    if aggregate.distinct {
        return false;
    }
    match aggregate.function {
        AggregateFunction::Count => {
            aggregate.expr.is_none() && aggregate.data_type == DataType::Int64
        }
        AggregateFunction::Sum => {
            let Some(argument) = aggregate.expr.as_ref() else {
                return false;
            };
            matches!(&argument.kind, ExprKind::Column(_))
                && sum_output_type(&argument.data_type)
                    .is_some_and(|output| output == aggregate.data_type)
        }
        AggregateFunction::Min | AggregateFunction::Max | AggregateFunction::Avg => false,
    }
}

fn sum_output_type(input: &DataType) -> Option<DataType> {
    Some(match input {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Decimal128(38, 0),
        DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
        _ => return None,
    })
}

fn valid_projection(expressions: &[BoundExpr], output: &PlanSchema, input: &PlanSchema) -> bool {
    expressions.len() == output.arrow().fields().len()
        && expressions.iter().enumerate().all(|(output_index, expr)| {
            let ExprKind::Column(input_index) = &expr.kind else {
                return false;
            };
            let Some(input_field) = input.arrow().fields().get(*input_index) else {
                return false;
            };
            expr.data_type == *input_field.data_type()
                && output.arrow().field(output_index).data_type() == input_field.data_type()
        })
}

fn valid_join_schema(join: &PlanSchema, left: &PlanSchema, right: &PlanSchema) -> bool {
    let fields = join.arrow().fields();
    fields.len() == left.arrow().fields().len() + right.arrow().fields().len()
        && fields
            .iter()
            .zip(left.arrow().fields().iter().chain(right.arrow().fields()))
            .all(|(joined, source)| joined.data_type() == source.data_type())
}

fn valid_key_pair(
    left_key: &BoundExpr,
    right_key: &BoundExpr,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> bool {
    let (ExprKind::Column(left_index), ExprKind::Column(right_index)) =
        (&left_key.kind, &right_key.kind)
    else {
        return false;
    };
    let Some(left_field) = left.schema().arrow().fields().get(*left_index) else {
        return false;
    };
    let Some(right_field) = right.schema().arrow().fields().get(*right_index) else {
        return false;
    };
    left_key.data_type == *left_field.data_type()
        && right_key.data_type == *right_field.data_type()
        && left_key.data_type == right_key.data_type
}

#[cfg(test)]
#[path = "join_match/tests.rs"]
mod tests;

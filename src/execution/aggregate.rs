use std::{collections::HashMap, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::Result;
use crate::runtime::{QueryContext, RecordBatchStream, SpillFile, boxed_record_batch_stream};
use crate::sql::{AggregateExpr, AggregateFunction, BoundExpr};

use super::{
    expr::evaluate,
    value::{CellValue, cell, values_to_array},
};

mod spill;
mod state;

#[cfg(test)]
mod tests;

use spill::{
    MergeOutcome, PartitionTask, SPILL_PARTITIONS, merge_partition, repartition_partition,
    spill_states,
};
use state::{GroupState, estimate_group_bytes};

pub(crate) fn aggregate(
    mut input: RecordBatchStream,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> RecordBatchStream {
    boxed_record_batch_stream(async_stream::try_stream! {
        let mut group_index: HashMap<Vec<CellValue>, usize> = HashMap::new();
        let mut states = Vec::<GroupState>::new();
        let mut reservation = context.memory.reservation();
        let partial_schema = partial_schema(&groups, &aggregates);
        let mut spilled: Option<Vec<Vec<SpillFile>>> = None;

        if groups.is_empty() {
            group_index.insert(Vec::new(), 0);
            states.push(GroupState::new(Vec::new(), &aggregates));
            reservation.try_grow(estimate_group_bytes(&states[0]))?;
        }

        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            let group_arrays = groups
                .iter()
                .map(|expr| evaluate(expr, &batch))
                .collect::<Result<Vec<_>>>()?;
            let aggregate_arrays = aggregates
                .iter()
                .map(|aggregate| aggregate.expr.as_ref().map(|expr| evaluate(expr, &batch)).transpose())
                .collect::<Result<Vec<_>>>()?;

            for row in 0..batch.num_rows() {
                let key = group_arrays
                    .iter()
                    .map(|array| cell(array, row))
                    .collect::<Result<Vec<_>>>()?;
                let index = if let Some(index) = group_index.get(&key) {
                    *index
                } else {
                    let state = GroupState::new(key.clone(), &aggregates);
                    let bytes = estimate_group_bytes(&state);
                    if reservation.try_grow(bytes).is_err() {
                        let partitions = spilled.get_or_insert_with(|| {
                            (0..SPILL_PARTITIONS).map(|_| Vec::new()).collect()
                        });
                        spill_states(
                            &mut states,
                            &mut group_index,
                            &groups,
                            &aggregates,
                            Arc::clone(&partial_schema),
                            partitions,
                            &context,
                        )?;
                        reservation.try_resize(0)?;
                        reservation
                            .try_grow(bytes)
                            .map_err(|_| spill::single_group_error(bytes, &context))?;
                    }
                    let index = states.len();
                    states.push(state);
                    group_index.insert(key, index);
                    index
                };
                let state = &mut states[index];
                for (aggregate_index, aggregate) in aggregates.iter().enumerate() {
                    let value = aggregate_arrays[aggregate_index]
                        .as_ref()
                        .map(|array| cell(array, row))
                        .transpose()?;
                    state.aggregates[aggregate_index].update(aggregate, value)?;
                }
            }
        }

        if let Some(mut partitions) = spilled {
            spill_states(
                &mut states,
                &mut group_index,
                &groups,
                &aggregates,
                Arc::clone(&partial_schema),
                &mut partitions,
                &context,
            )?;
            reservation.try_resize(0)?;
            let mut pending = partitions
                .into_iter()
                .rev()
                .filter(|files| !files.is_empty())
                .map(PartitionTask::initial)
                .collect::<Vec<_>>();
            while let Some(task) = pending.pop() {
                context.check_cancelled()?;
                match merge_partition(
                    &task.files,
                    &groups,
                    &aggregates,
                    &context,
                    &mut reservation,
                )? {
                    MergeOutcome::Merged(partition_states) => {
                        spill::remove_files(&context, &task.files);
                        for chunk in partition_states.chunks(batch_size.max(1)) {
                            context.check_cancelled()?;
                            yield build_batch(chunk, &groups, &aggregates, Arc::clone(&schema))?;
                        }
                        reservation.try_resize(0)?;
                    }
                    MergeOutcome::Repartition => {
                        reservation.try_resize(0)?;
                        let next_depth = task.next_depth()?;
                        let child_partitions = repartition_partition(
                            &task.files,
                            &groups,
                            next_depth,
                            &context,
                        )?;
                        spill::remove_files(&context, &task.files);
                        for files in child_partitions.into_iter().rev() {
                            if !files.is_empty() {
                                pending.push(PartitionTask::child(files, next_depth));
                            }
                        }
                    }
                }
            }
        } else {
            for chunk in states.chunks(batch_size.max(1)) {
                context.check_cancelled()?;
                yield build_batch(chunk, &groups, &aggregates, Arc::clone(&schema))?;
            }
        }
    })
}

fn build_batch(
    states: &[GroupState],
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(groups.len() + aggregates.len());
    for (index, expression) in groups.iter().enumerate() {
        let values = states
            .iter()
            .map(|state| state.key[index].clone())
            .collect::<Vec<_>>();
        columns.push(values_to_array(&values, &expression.data_type)?);
    }
    for (index, expression) in aggregates.iter().enumerate() {
        let values = states
            .iter()
            .map(|state| state.aggregates[index].finish())
            .collect::<Result<Vec<_>>>()?;
        columns.push(values_to_array(&values, &expression.data_type)?);
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn partial_schema(groups: &[BoundExpr], aggregates: &[AggregateExpr]) -> SchemaRef {
    let mut fields = groups
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Field::new(
                format!("__group_{index}"),
                expression.data_type.clone(),
                true,
            )
        })
        .collect::<Vec<_>>();
    for (index, expression) in aggregates.iter().enumerate() {
        if expression.function == AggregateFunction::Avg {
            fields.push(Field::new(
                format!("__agg_{index}_sum"),
                match expression.data_type {
                    DataType::Decimal128(_, scale) => DataType::Decimal128(38, scale),
                    _ => DataType::Float64,
                },
                false,
            ));
            fields.push(Field::new(
                format!("__agg_{index}_count"),
                DataType::UInt64,
                false,
            ));
        } else if expression.function == AggregateFunction::Sum {
            fields.push(Field::new(
                format!("__agg_{index}_sum"),
                expression.data_type.clone(),
                false,
            ));
            fields.push(Field::new(
                format!("__agg_{index}_seen"),
                DataType::Boolean,
                false,
            ));
        } else {
            fields.push(Field::new(
                format!("__agg_{index}"),
                expression.data_type.clone(),
                true,
            ));
        }
    }
    Arc::new(Schema::new(fields))
}

fn build_partial_batch(
    states: &[GroupState],
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (index, expression) in groups.iter().enumerate() {
        let values = states
            .iter()
            .map(|state| state.key[index].clone())
            .collect::<Vec<_>>();
        columns.push(values_to_array(&values, &expression.data_type)?);
    }
    for (index, expression) in aggregates.iter().enumerate() {
        let partials = states
            .iter()
            .map(|state| state.aggregates[index].partial_values())
            .collect::<Result<Vec<_>>>()?;
        let width = if matches!(
            expression.function,
            AggregateFunction::Avg | AggregateFunction::Sum
        ) {
            2
        } else {
            1
        };
        for partial_index in 0..width {
            let values = partials
                .iter()
                .map(|values| values[partial_index].clone())
                .collect::<Vec<_>>();
            let data_type = schema.field(columns.len()).data_type();
            columns.push(values_to_array(&values, data_type)?);
        }
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

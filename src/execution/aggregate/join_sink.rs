use arrow::{
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryReservation, QueryContext},
    sql::{AggregateExpr, AggregateFunction, ExprKind, field_is_materialized},
};

use super::{
    OutputMode, build_output_envelope,
    state::{AggregateState, GroupState, estimate_group_bytes},
    update_global_batch,
};

mod fixed;
mod multiplicity;
mod selected;

/// Query-local state for a global aggregate directly above a simple inner join.
/// It owns no input batches and therefore remains small under duplicate-heavy joins.
pub(in crate::execution) struct JoinAggregateSink {
    state: GroupState,
    aggregates: Vec<AggregateExpr>,
    output_schema: SchemaRef,
    reservation: MemoryReservation,
}

impl JoinAggregateSink {
    pub(in crate::execution) fn memory_size(&self) -> usize {
        self.reservation.size()
    }

    pub(in crate::execution) fn try_new(
        aggregates: Vec<AggregateExpr>,
        output_schema: SchemaRef,
        input_schema: &SchemaRef,
        context: &QueryContext,
    ) -> Result<Self> {
        validate(&aggregates, &output_schema, input_schema)?;
        let state = GroupState::new(Vec::new(), &aggregates);
        let bytes = estimate_group_bytes(&state)
            .saturating_add(std::mem::size_of::<Self>())
            .saturating_add(
                aggregates
                    .capacity()
                    .saturating_mul(std::mem::size_of::<AggregateExpr>()),
            )
            .saturating_add(
                aggregates
                    .iter()
                    .map(|aggregate| {
                        aggregate.display_name.capacity().saturating_add(
                            aggregate
                                .expr
                                .as_ref()
                                .map_or(0, |expr| expr.display_name.capacity()),
                        )
                    })
                    .fold(0usize, usize::saturating_add),
            )
            .max(1);
        let reservation = context.memory.try_reserve(bytes).map_err(|error| {
            Error::ResourceExhausted(format!(
                "join aggregate state requires {bytes} bytes: {error}"
            ))
        })?;
        context.metrics.observe_memory(context.memory.used());
        Ok(Self {
            state,
            aggregates,
            output_schema,
            reservation,
        })
    }

    pub(in crate::execution) fn consume_inner_selection(
        &mut self,
        left: &RecordBatch,
        right: &RecordBatch,
        left_indices: &[u32],
        right_indices: &[Option<u32>],
    ) -> Result<()> {
        selected::update(
            &mut self.state.aggregates,
            &self.aggregates,
            left,
            right,
            left_indices,
            right_indices,
        )
    }

    /// Fixed-key inner joins use this seam to avoid materializing pair indices.
    /// The lookup closure is invoked exactly once for every probe row.
    pub(in crate::execution) fn consume_fixed_matches<'a, F>(
        &mut self,
        probe: &RecordBatch,
        build: &RecordBatch,
        lookup: F,
        context: &QueryContext,
    ) -> Result<usize>
    where
        F: FnMut(usize) -> Option<&'a [u32]>,
    {
        fixed::update(
            &mut self.state.aggregates,
            &self.aggregates,
            probe,
            build,
            lookup,
            context,
        )
    }

    pub(in crate::execution) fn consume_probe_multiplicity_range<F>(
        &mut self,
        probe: &RecordBatch,
        rows: std::ops::Range<usize>,
        lookup: F,
        context: &QueryContext,
    ) -> Result<u64>
    where
        F: FnMut(usize) -> Result<u64>,
    {
        multiplicity::update_range(
            &mut self.state.aggregates,
            &self.aggregates,
            probe,
            rows,
            lookup,
            context,
        )
    }

    /// Used by Grace/sort-merge paths, which keep their existing materialized output.
    pub(in crate::execution) async fn consume_batch(
        &mut self,
        batch: &RecordBatch,
        context: &QueryContext,
    ) -> Result<()> {
        let _compute = context.acquire_compute().await?;
        let _active = context.scheduler.enter_lane();
        update_global_batch(
            std::slice::from_mut(&mut self.state),
            &self.aggregates,
            batch,
            context,
        )
    }

    pub(in crate::execution) fn merge(&mut self, other: Self) -> Result<()> {
        if self.aggregates != other.aggregates
            || self.state.aggregates.len() != other.state.aggregates.len()
        {
            return Err(Error::Internal(
                "join aggregate partial states have incompatible schemas".into(),
            ));
        }
        for ((current, incoming), aggregate) in self
            .state
            .aggregates
            .iter_mut()
            .zip(other.state.aggregates)
            .zip(&self.aggregates)
        {
            merge_state(current, incoming, aggregate)?;
        }
        Ok(())
    }

    pub(in crate::execution) async fn finish(
        self,
        context: &QueryContext,
    ) -> Result<BatchEnvelope> {
        build_output_envelope(
            std::slice::from_ref(&self.state),
            &[],
            &self.aggregates,
            self.output_schema,
            OutputMode::Final,
            context,
            self.reservation.size(),
        )
        .await
    }
}

fn validate(
    aggregates: &[AggregateExpr],
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
) -> Result<()> {
    if aggregates.is_empty() || aggregates.len() != output_schema.fields().len() {
        return Err(Error::Internal(
            "unsupported aggregate reached the join selection sink".into(),
        ));
    }
    for (index, aggregate) in aggregates.iter().enumerate() {
        if aggregate.distinct || output_schema.field(index).data_type() != &aggregate.data_type {
            return Err(Error::Internal(
                "join aggregate output schema does not match its expressions".into(),
            ));
        }
        match aggregate.function {
            AggregateFunction::Count
                if aggregate.expr.is_none() && aggregate.data_type == DataType::Int64 => {}
            AggregateFunction::Sum => {
                let expression = aggregate.expr.as_ref().ok_or_else(|| {
                    Error::Internal("join aggregate SUM is missing its input".into())
                })?;
                let ExprKind::Column(column) = &expression.kind else {
                    return Err(Error::Internal(
                        "join aggregate SUM input is not a direct column".into(),
                    ));
                };
                let field = input_schema.fields().get(*column).ok_or_else(|| {
                    Error::Internal("join aggregate SUM column is out of bounds".into())
                })?;
                if field.data_type() != &expression.data_type
                    || !field_is_materialized(field)
                    || sum_output_type(field.data_type()) != Some(aggregate.data_type.clone())
                {
                    return Err(Error::Internal(
                        "join aggregate SUM type does not match its input column".into(),
                    ));
                }
            }
            _ => {
                return Err(Error::Internal(
                    "unsupported aggregate reached the join selection sink".into(),
                ));
            }
        }
    }
    Ok(())
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
        DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
        _ => return None,
    })
}

fn merge_state(
    current: &mut AggregateState,
    incoming: AggregateState,
    aggregate: &AggregateExpr,
) -> Result<()> {
    match (current, incoming) {
        (AggregateState::Count(left), AggregateState::Count(right)) => {
            *left = left
                .checked_add(right)
                .ok_or_else(|| Error::Execution("count overflowed INT64".into()))?;
        }
        (
            AggregateState::SumSigned {
                value: left,
                seen: left_seen,
                ..
            },
            AggregateState::SumSigned {
                value: right,
                seen: right_seen,
                ..
            },
        )
        | (
            AggregateState::SumDecimal {
                value: left,
                seen: left_seen,
                ..
            },
            AggregateState::SumDecimal {
                value: right,
                seen: right_seen,
                ..
            },
        ) => {
            *left = left
                .checked_add(right)
                .ok_or_else(|| Error::Execution("sum overflowed i128".into()))?;
            *left_seen |= right_seen;
        }
        (
            AggregateState::SumUnsigned {
                value: left,
                seen: left_seen,
                ..
            },
            AggregateState::SumUnsigned {
                value: right,
                seen: right_seen,
                ..
            },
        ) => {
            *left = left
                .checked_add(right)
                .ok_or_else(|| Error::Execution("sum overflowed u128".into()))?;
            *left_seen |= right_seen;
        }
        (
            AggregateState::SumFloat {
                value: left,
                seen: left_seen,
            },
            AggregateState::SumFloat {
                value: right,
                seen: right_seen,
            },
        ) => {
            *left += right;
            *left_seen |= right_seen;
        }
        _ => {
            return Err(Error::Internal(format!(
                "incompatible partial states for {}",
                aggregate.display_name
            )));
        }
    }
    Ok(())
}

pub(in crate::execution) struct SelectionTarget<'a> {
    sink: &'a mut JoinAggregateSink,
}

impl<'a> SelectionTarget<'a> {
    pub(in crate::execution) fn new(sink: &'a mut JoinAggregateSink) -> Self {
        Self { sink }
    }
}

impl crate::execution::join::output::JoinOutputTarget for SelectionTarget<'_> {
    fn workspace_bytes(
        &self,
        _selection: &crate::execution::join::output::JoinSelection<'_>,
    ) -> Result<usize> {
        Ok(0)
    }

    fn consume(
        &mut self,
        selection: crate::execution::join::output::JoinSelection<'_>,
        _workspace: MemoryReservation,
    ) -> Result<crate::execution::join::output::JoinEmission> {
        if selection.join_type != crate::sql::JoinType::Inner {
            return Err(Error::Internal(
                "non-inner join reached the selection aggregate target".into(),
            ));
        }
        self.sink.consume_inner_selection(
            selection.left,
            selection.right,
            selection.left_indices,
            selection.right_indices,
        )?;
        Ok(crate::execution::join::output::JoinEmission::Consumed {
            rows: selection.left_indices.len(),
        })
    }
}

#[cfg(test)]
#[path = "join_sink/tests.rs"]
mod tests;

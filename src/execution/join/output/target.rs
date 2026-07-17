use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::{
    Result,
    runtime::{BatchEnvelope, MemoryReservation},
    sql::JoinType,
};

use super::{build_output, output_workspace_bytes};

pub(in crate::execution) struct JoinSelection<'a> {
    pub(in crate::execution) left: &'a RecordBatch,
    pub(in crate::execution) right: &'a RecordBatch,
    pub(in crate::execution) left_indices: &'a [u32],
    pub(in crate::execution) right_indices: &'a [Option<u32>],
    pub(in crate::execution) markers: Option<&'a [Option<bool>]>,
    pub(in crate::execution) join_type: JoinType,
    pub(in crate::execution) schema: &'a SchemaRef,
}

pub(in crate::execution) enum JoinEmission {
    Batch(BatchEnvelope),
    // A selection-aware sink consumes rows without producing an Arrow batch.
    // Keeping this distinct from Exhausted prevents callers from ending probe
    // iteration when the future aggregate target accepts one selection.
    #[allow(dead_code)]
    Consumed {
        rows: usize,
    },
    Exhausted,
}

pub(in crate::execution) trait JoinOutputTarget {
    fn workspace_bytes(&self, selection: &JoinSelection<'_>) -> Result<usize>;

    fn consume(
        &mut self,
        selection: JoinSelection<'_>,
        workspace: MemoryReservation,
    ) -> Result<JoinEmission>;
}

#[derive(Default)]
pub(in crate::execution::join) struct BatchOutputTarget;

impl JoinOutputTarget for BatchOutputTarget {
    fn workspace_bytes(&self, selection: &JoinSelection<'_>) -> Result<usize> {
        output_workspace_bytes(
            selection.left,
            selection.right,
            selection.left_indices,
            selection.right_indices,
            selection.join_type,
            selection.schema,
        )
    }

    fn consume(
        &mut self,
        selection: JoinSelection<'_>,
        workspace: MemoryReservation,
    ) -> Result<JoinEmission> {
        let output = build_output(
            selection.left,
            selection.right,
            selection.left_indices,
            selection.right_indices,
            selection.markers,
            selection.join_type,
            Arc::clone(selection.schema),
        )?;
        Ok(JoinEmission::Batch(BatchEnvelope::from_reservation(
            output,
            workspace,
            "join output",
        )?))
    }
}

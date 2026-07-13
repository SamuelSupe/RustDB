mod aggregate;
mod expr;
mod functions;
mod join;
mod pipeline;
mod repeat;
mod runner;
mod runtime_filter;
mod scalar;
mod scan;
mod sort;
mod value;
mod window;

use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::Schema,
    record_batch::{RecordBatch, RecordBatchOptions},
};

#[cfg(test)]
use futures::StreamExt;

#[cfg(test)]
use crate::runtime::{RecordBatchStream, boxed_record_batch_stream};
use crate::{
    Result,
    runtime::{MemoryBatchStream, QueryContext},
    sql::{LogicalPlan, StatementPlan},
};

pub(crate) fn evaluate_constant_expression(expr: &crate::sql::BoundExpr) -> Result<ArrayRef> {
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;
    expr::evaluate(expr, &batch)
}

/// Builds a lazy record-batch stream. Work starts when the caller polls it.
#[cfg(test)]
pub async fn execute(plan: StatementPlan, context: Arc<QueryContext>) -> Result<RecordBatchStream> {
    let mut input = runner::execute(plan, context).await?;
    Ok(boxed_record_batch_stream(async_stream::try_stream! {
        while let Some(batch) = input.next().await {
            yield batch?.into_public();
        }
    }))
}

pub(crate) async fn execute_internal(
    plan: StatementPlan,
    context: Arc<QueryContext>,
) -> Result<MemoryBatchStream> {
    runner::execute(plan, context).await
}

pub(crate) async fn prepare_plan(plan: &LogicalPlan, context: Arc<QueryContext>) -> Result<()> {
    runner::prepare_plan(plan, context).await
}

#[cfg(test)]
mod tests;

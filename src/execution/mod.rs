mod aggregate;
mod expr;
mod join;
mod pipeline;
mod runner;
mod scalar;
mod scan;
mod sort;
mod value;

use std::sync::Arc;

#[cfg(test)]
use futures::StreamExt;

#[cfg(test)]
use crate::runtime::{RecordBatchStream, boxed_record_batch_stream};
use crate::{
    Result,
    runtime::{MemoryBatchStream, QueryContext},
    sql::{LogicalPlan, StatementPlan},
};

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

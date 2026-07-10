mod aggregate;
mod expr;
mod join;
mod runner;
mod scalar;
mod scan;
mod sort;
mod value;

use std::sync::Arc;

use crate::{
    Result,
    runtime::{QueryContext, RecordBatchStream},
    sql::{LogicalPlan, StatementPlan},
};

/// Builds a lazy record-batch stream. Work starts when the caller polls it.
pub async fn execute(plan: StatementPlan, context: Arc<QueryContext>) -> Result<RecordBatchStream> {
    runner::execute(plan, context).await
}

pub(crate) async fn prepare_plan(plan: &LogicalPlan, context: Arc<QueryContext>) -> Result<()> {
    runner::prepare_plan(plan, context).await
}

#[cfg(test)]
mod tests;

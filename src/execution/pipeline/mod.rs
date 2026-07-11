mod plan;
mod run;

use crate::{runtime::MemoryBatchStream, sql::LogicalPlan};

use plan::{FusedPipeline, PipelineOperator};

pub(super) fn try_execute(
    plan: LogicalPlan,
    context: std::sync::Arc<crate::runtime::QueryContext>,
) -> std::result::Result<MemoryBatchStream, Box<LogicalPlan>> {
    plan::compile(plan).map(|pipeline| run::execute(pipeline, context))
}

#[cfg(test)]
mod tests;

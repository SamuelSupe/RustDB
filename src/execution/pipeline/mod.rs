mod compact;
mod plan;
mod profile;
mod run;

use crate::{runtime::MemoryBatchStream, sql::LogicalPlan};

use plan::{FusedPipeline, PipelineOperator};

pub(super) fn try_execute(
    plan: LogicalPlan,
    context: std::sync::Arc<crate::runtime::QueryContext>,
    parent_id: Option<u64>,
) -> std::result::Result<MemoryBatchStream, Box<LogicalPlan>> {
    plan::compile(plan).map(|pipeline| run::execute(pipeline, context, parent_id))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compact_pipeline_tests;

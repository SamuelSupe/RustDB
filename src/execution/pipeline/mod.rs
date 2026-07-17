mod compact;
mod dictionary;
mod plan;
mod profile;
mod run;

use crate::{
    runtime::MemoryBatchStream,
    sql::{AggregateExpr, BoundExpr, LogicalPlan},
};

use plan::{FusedPipeline, PipelineOperator};

const PRIVATE_BLOCKING_DECODE_BATCH_SIZE: usize = 65_536;
const CONCURRENT_PRIVATE_DECODE_BATCH_SIZE: usize = 32_768;

pub(super) fn try_execute(
    plan: LogicalPlan,
    context: std::sync::Arc<crate::runtime::QueryContext>,
    parent_id: Option<u64>,
) -> std::result::Result<MemoryBatchStream, Box<LogicalPlan>> {
    plan::compile(plan).map(|pipeline| {
        run::execute(
            pipeline,
            dictionary::GroupDictionaryPlan::default(),
            None,
            context,
            parent_id,
        )
    })
}

/// Executes an Aggregate input with query-local physical scan hints. They never
/// change the logical or public schema and are limited to proven private sinks.
pub(super) fn try_execute_grouped(
    plan: LogicalPlan,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    context: std::sync::Arc<crate::runtime::QueryContext>,
    parent_id: Option<u64>,
) -> std::result::Result<MemoryBatchStream, Box<LogicalPlan>> {
    plan::compile(plan).map(|pipeline| {
        let dictionaries = dictionary::plan(&pipeline, groups, aggregates);
        let decode_batch_size = dictionaries
            .enabled()
            .then_some(PRIVATE_BLOCKING_DECODE_BATCH_SIZE);
        run::execute(
            pipeline,
            dictionaries,
            decode_batch_size,
            context,
            parent_id,
        )
    })
}

/// Join build/probe inputs are internal pipeline boundaries, so a larger
/// admitted Parquet batch does not change the public result stream.
pub(super) fn try_execute_join_input(
    plan: LogicalPlan,
    context: std::sync::Arc<crate::runtime::QueryContext>,
    parent_id: Option<u64>,
) -> std::result::Result<MemoryBatchStream, Box<LogicalPlan>> {
    plan::compile(plan).map(|pipeline| {
        let decode_batch_size = join_decode_batch_size(context.configured_query_concurrency());
        run::execute(
            pipeline,
            dictionary::GroupDictionaryPlan::default(),
            Some(decode_batch_size),
            context,
            parent_id,
        )
    })
}

fn join_decode_batch_size(configured_query_concurrency: usize) -> usize {
    if configured_query_concurrency > 1 {
        CONCURRENT_PRIVATE_DECODE_BATCH_SIZE
    } else {
        PRIVATE_BLOCKING_DECODE_BATCH_SIZE
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compact_pipeline_tests;

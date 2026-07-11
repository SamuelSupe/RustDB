use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::{
    datasource::TableProvider,
    sql::{BoundExpr, LogicalPlan},
};

pub(super) struct FusedPipeline {
    pub(super) scan: ScanStage,
    pub(super) operators: Vec<PipelineOperator>,
}

pub(super) struct ScanStage {
    pub(super) provider: Arc<dyn TableProvider>,
    pub(super) projection: Option<Vec<usize>>,
    pub(super) pushed_filter: Option<BoundExpr>,
    pub(super) limit: Option<usize>,
    pub(super) schema: SchemaRef,
}

#[derive(Clone)]
pub(super) enum PipelineOperator {
    Filter(BoundExpr),
    Projection {
        expressions: Vec<BoundExpr>,
        schema: SchemaRef,
    },
}

pub(super) fn compile(plan: LogicalPlan) -> std::result::Result<FusedPipeline, Box<LogicalPlan>> {
    if !is_fusable(&plan) {
        return Err(Box::new(plan));
    }
    Ok(build(plan))
}

fn is_fusable(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan { .. } => true,
        LogicalPlan::Filter { input, .. } | LogicalPlan::Projection { input, .. } => {
            is_fusable(input)
        }
        _ => false,
    }
}

fn build(plan: LogicalPlan) -> FusedPipeline {
    match plan {
        LogicalPlan::Scan {
            provider,
            projection,
            pushed_filter,
            limit,
            schema,
            ..
        } => FusedPipeline {
            scan: ScanStage {
                provider,
                projection,
                pushed_filter,
                limit,
                schema: Arc::clone(schema.arrow()),
            },
            operators: Vec::new(),
        },
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            let mut pipeline = build(*input);
            pipeline.operators.push(PipelineOperator::Filter(predicate));
            pipeline
        }
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => {
            let mut pipeline = build(*input);
            pipeline.operators.push(PipelineOperator::Projection {
                expressions,
                schema: Arc::clone(schema.arrow()),
            });
            pipeline
        }
        _ => unreachable!("is_fusable accepts only Scan/Filter/Projection chains"),
    }
}

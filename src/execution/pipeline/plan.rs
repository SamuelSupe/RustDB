use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::{
    datasource::TableProvider,
    sql::{BinaryOp, BoundExpr, ExprKind, LogicalPlan},
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
            push_filter_operators(&mut pipeline.operators, predicate);
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

/// Breaks an infallible conjunction into sequential filters so selective
/// terms shrink the batch before later kernels run. The logical predicate is
/// left intact for scan pruning and EXPLAIN, and fallible expressions retain
/// SQL short-circuit evaluation in one operator.
fn push_filter_operators(operators: &mut Vec<PipelineOperator>, predicate: BoundExpr) {
    if predicate.is_structurally_infallible()
        && let ExprKind::Binary {
            left,
            op: BinaryOp::And,
            right,
        } = predicate.kind
    {
        push_filter_operators(operators, *left);
        push_filter_operators(operators, *right);
    } else {
        operators.push(PipelineOperator::Filter(predicate));
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{PipelineOperator, push_filter_operators};
    use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

    #[test]
    fn splits_infallible_conjunctions_left_to_right() {
        let mut operators = Vec::new();
        push_filter_operators(
            &mut operators,
            binary(comparison(1), BinaryOp::And, comparison(2)),
        );

        assert_eq!(operators.len(), 2);
        let PipelineOperator::Filter(left) = &operators[0] else {
            panic!("expected left filter")
        };
        let PipelineOperator::Filter(right) = &operators[1] else {
            panic!("expected right filter")
        };
        assert_eq!(left.display_name, "value = 1");
        assert_eq!(right.display_name, "value = 2");
    }

    #[test]
    fn keeps_or_and_fallible_and_as_single_operators() {
        let division = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(BoundExpr::column(0, DataType::Int64, "value")),
                op: BinaryOp::Divide,
                right: Box::new(BoundExpr::literal(ScalarValue::Int64(0))),
            },
            data_type: DataType::Int64,
            display_name: "value / 0".into(),
        };
        let fallible = binary(
            BoundExpr::literal(ScalarValue::Boolean(false)),
            BinaryOp::And,
            binary(
                division,
                BinaryOp::Eq,
                BoundExpr::literal(ScalarValue::Int64(1)),
            ),
        );
        for predicate in [binary(comparison(1), BinaryOp::Or, comparison(2)), fallible] {
            let mut operators = Vec::new();
            push_filter_operators(&mut operators, predicate);
            assert_eq!(operators.len(), 1);
        }
    }

    fn comparison(value: i64) -> BoundExpr {
        binary(
            BoundExpr::column(0, DataType::Int64, "value"),
            BinaryOp::Eq,
            BoundExpr::literal(ScalarValue::Int64(value)),
        )
    }

    fn binary(left: BoundExpr, op: BinaryOp, right: BoundExpr) -> BoundExpr {
        BoundExpr {
            display_name: format!("{} {op} {}", left.display_name, right.display_name),
            kind: ExprKind::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
        }
    }
}

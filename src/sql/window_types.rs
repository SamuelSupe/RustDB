use arrow::datatypes::DataType;

use super::{AggregateExpr, BoundExpr, SortExpr};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Ntile(u64),
    PercentRank,
    CumeDist,
    Lead {
        expr: BoundExpr,
        offset: u64,
        default: BoundExpr,
    },
    Lag {
        expr: BoundExpr,
        offset: u64,
        default: BoundExpr,
    },
    FirstValue(BoundExpr),
    LastValue(BoundExpr),
    Aggregate(AggregateExpr),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowFrame {
    pub(crate) units: WindowFrameUnits,
    pub(crate) start: WindowFrameBound,
    pub(crate) end: WindowFrameBound,
}

impl WindowFrame {
    pub(crate) fn whole_partition() -> Self {
        Self {
            units: WindowFrameUnits::Rows,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::UnboundedFollowing,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WindowExpr {
    pub(crate) function: WindowFunction,
    pub(crate) partition_by: Vec<BoundExpr>,
    pub(crate) order_by: Vec<SortExpr>,
    pub(crate) frame: WindowFrame,
    pub(crate) data_type: DataType,
    pub(crate) display_name: String,
}

impl WindowExpr {
    pub(crate) fn referenced_columns(&self, output: &mut Vec<usize>) {
        match &self.function {
            WindowFunction::Aggregate(aggregate) => {
                if let Some(expression) = &aggregate.expr {
                    expression.referenced_columns(output);
                }
            }
            WindowFunction::Lead { expr, default, .. }
            | WindowFunction::Lag { expr, default, .. } => {
                expr.referenced_columns(output);
                default.referenced_columns(output);
            }
            WindowFunction::FirstValue(expr) | WindowFunction::LastValue(expr) => {
                expr.referenced_columns(output);
            }
            WindowFunction::RowNumber
            | WindowFunction::Rank
            | WindowFunction::DenseRank
            | WindowFunction::Ntile(_)
            | WindowFunction::PercentRank
            | WindowFunction::CumeDist => {}
        }
        for expression in &self.partition_by {
            expression.referenced_columns(output);
        }
        for expression in &self.order_by {
            expression.expr.referenced_columns(output);
        }
    }
}

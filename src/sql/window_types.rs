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
    Aggregate(AggregateExpr),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowFrameUnits {
    Rows,
    Range,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowFrameBound {
    UnboundedPreceding,
    CurrentRow,
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
        if let WindowFunction::Aggregate(aggregate) = &self.function
            && let Some(expression) = &aggregate.expr
        {
            expression.referenced_columns(output);
        }
        for expression in &self.partition_by {
            expression.referenced_columns(output);
        }
        for expression in &self.order_by {
            expression.expr.referenced_columns(output);
        }
    }
}

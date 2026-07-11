mod aggregate;
mod binder;
mod coercion;
mod expr;
mod literal;
mod name_resolution;
mod plan;
mod planner;
mod relation;
mod scalar_subquery;
mod subquery;

pub use expr::{
    AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, ExprKind, ScalarValue, SortExpr, UnaryOp,
};
pub use plan::{JoinType, LogicalPlan, PlanSchema, StatementPlan};
#[cfg(test)]
pub use planner::plan_sql;
pub(crate) use planner::{bind_statement, optimize_statement};

#[cfg(test)]
mod correctness_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tpch_queries;
#[cfg(test)]
mod tpch_tests;

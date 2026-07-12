mod aggregate;
mod binder;
mod coercion;
mod expr;
mod functions;
mod literal;
mod name_resolution;
mod plan;
mod planner;
mod relation;
pub(crate) mod scalar_subquery;
mod subquery;
pub(crate) mod temporal;

pub use expr::{
    AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, DateTimePart, ExprKind, ScalarFunction,
    ScalarValue, SortExpr, UnaryOp,
};
pub use plan::{DependentJoinKind, JoinType, LogicalPlan, PlanSchema, StatementPlan};
#[cfg(test)]
pub use planner::plan_sql;
pub(crate) use planner::{bind_statement, optimize_statement};

#[cfg(test)]
mod correctness_tests;
#[cfg(test)]
mod correlation_aggregate_tests;
#[cfg(test)]
mod correlation_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod timezone_tests;
#[cfg(test)]
mod tpch_queries;
#[cfg(test)]
mod tpch_tests;

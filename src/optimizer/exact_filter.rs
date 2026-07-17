use crate::sql::LogicalPlan;

mod lower;

use lower::lower;

/// Removes a direct Filter -> Scan only when the provider accepts the complete
/// predicate as its semantic filter. No expression normalization happens here:
/// constant folding has already run, and casts remain an explicit rejection.
pub(super) fn apply(plan: LogicalPlan) -> LogicalPlan {
    let plan = rewrite_children(plan);
    let LogicalPlan::Filter {
        input,
        predicate,
        schema: filter_schema,
    } = plan
    else {
        return plan;
    };
    let LogicalPlan::Scan {
        table_name,
        provider,
        statistics,
        projection,
        mut pushed_filter,
        exact_filter: None,
        limit,
        schema: scan_schema,
    } = *input
    else {
        return LogicalPlan::Filter {
            input,
            predicate,
            schema: filter_schema,
        };
    };
    let Some(exact) = lower(&predicate, scan_schema.arrow()) else {
        return LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table_name,
                provider,
                statistics,
                projection,
                pushed_filter,
                exact_filter: None,
                limit,
                schema: scan_schema,
            }),
            predicate,
            schema: filter_schema,
        };
    };
    if !provider.supports_exact_filter(&exact) {
        return LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table_name,
                provider,
                statistics,
                projection,
                pushed_filter,
                exact_filter: None,
                limit,
                schema: scan_schema,
            }),
            predicate,
            schema: filter_schema,
        };
    }
    pushed_filter.get_or_insert(predicate);
    LogicalPlan::Scan {
        table_name,
        provider,
        statistics,
        projection,
        pushed_filter,
        exact_filter: Some(exact),
        limit,
        schema: scan_schema,
    }
}

fn rewrite_children(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => LogicalPlan::Filter {
            input: Box::new(apply(*input)),
            predicate,
            schema,
        },
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => LogicalPlan::Projection {
            input: Box::new(apply(*input)),
            expressions,
            schema,
        },
        LogicalPlan::Scalarize { input, schema } => LogicalPlan::Scalarize {
            input: Box::new(apply(*input)),
            schema,
        },
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(apply(*input)),
            group_exprs,
            aggregate_exprs,
            schema,
        },
        LogicalPlan::Append { inputs, schema } => LogicalPlan::Append {
            inputs: inputs.into_iter().map(apply).collect(),
            schema,
        },
        LogicalPlan::Repeat {
            input,
            count,
            schema,
        } => LogicalPlan::Repeat {
            input: Box::new(apply(*input)),
            count,
            schema,
        },
        LogicalPlan::Window {
            input,
            expressions,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(apply(*input)),
            expressions,
            schema,
        },
        LogicalPlan::Sort {
            input,
            expressions,
            fetch,
            schema,
        } => LogicalPlan::Sort {
            input: Box::new(apply(*input)),
            expressions,
            fetch,
            schema,
        },
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            schema,
        } => LogicalPlan::Limit {
            input: Box::new(apply(*input)),
            offset,
            limit,
            schema,
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(apply(*left)),
            right: Box::new(apply(*right)),
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        },
        LogicalPlan::DependentJoin {
            left,
            right,
            kind,
            guard,
            schema,
        } => LogicalPlan::DependentJoin {
            left: Box::new(apply(*left)),
            right: Box::new(apply(*right)),
            kind,
            guard,
            schema,
        },
        leaf @ (LogicalPlan::Empty { .. } | LogicalPlan::Scan { .. }) => leaf,
    }
}

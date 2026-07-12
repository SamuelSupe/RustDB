use std::{fmt, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};

use crate::{
    datasource::{TableProvider, TableStatistics},
    runtime::QueryContext,
};

use super::{AggregateExpr, BoundExpr, SortExpr};

#[derive(Clone, Debug)]
pub struct PlanSchema {
    arrow: SchemaRef,
    qualifiers: Arc<[Option<String>]>,
}

impl PlanSchema {
    pub fn new(arrow: SchemaRef, qualifiers: Vec<Option<String>>) -> Self {
        debug_assert_eq!(arrow.fields().len(), qualifiers.len());
        Self {
            arrow,
            qualifiers: qualifiers.into(),
        }
    }

    pub fn unqualified(arrow: SchemaRef) -> Self {
        let qualifiers = vec![None; arrow.fields().len()];
        Self::new(arrow, qualifiers)
    }

    pub fn empty() -> Self {
        Self::unqualified(Arc::new(Schema::empty()))
    }

    pub fn arrow(&self) -> &SchemaRef {
        &self.arrow
    }

    pub fn qualifier(&self, index: usize) -> Option<&str> {
        self.qualifiers[index].as_deref()
    }

    pub fn join(left: &Self, right: &Self) -> Self {
        Self::join_with_right_nullability(left, right, false)
    }

    pub fn left_join(left: &Self, right: &Self) -> Self {
        Self::join_with_right_nullability(left, right, true)
    }

    fn join_with_right_nullability(left: &Self, right: &Self, right_nullable: bool) -> Self {
        let fields: Vec<Arc<Field>> = left
            .arrow
            .fields()
            .iter()
            .cloned()
            .chain(right.arrow.fields().iter().map(|field| {
                if right_nullable {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                } else {
                    Arc::clone(field)
                }
            }))
            .collect();
        let qualifiers = left
            .qualifiers
            .iter()
            .chain(right.qualifiers.iter())
            .cloned()
            .collect();
        Self::new(Arc::new(Schema::new(fields)), qualifiers)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Semi,
    Anti,
    /// A scalar-subquery join. Execution must fail when more than one right
    /// row matches a left row.
    LeftSingle,
    /// Appends one boolean marker instead of right-side columns. A missing
    /// `null_aware` comparison means EXISTS semantics; a present comparison
    /// means SQL IN semantics.
    Mark,
    /// Filters the left input using SQL NOT IN semantics, including RHS NULLs.
    NullAwareAnti,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependentJoinKind {
    Scalar,
    GuardedScalar {
        expression: BoundExpr,
    },
    Exists,
    In {
        needle: BoundExpr,
    },
    /// A top-level correlated EXISTS predicate that can be decorrelated
    /// directly to a Semi/Anti join without materializing a marker column.
    ExistsFilter {
        negated: bool,
    },
    /// A top-level correlated IN predicate that can be decorrelated directly
    /// to a Semi/NullAwareAnti join without materializing a marker column.
    InFilter {
        needle: BoundExpr,
        negated: bool,
    },
}

#[derive(Clone)]
pub enum LogicalPlan {
    Empty {
        produce_one_row: bool,
        schema: PlanSchema,
    },
    Scan {
        table_name: String,
        provider: Arc<dyn TableProvider>,
        statistics: TableStatistics,
        projection: Option<Vec<usize>>,
        pushed_filter: Option<BoundExpr>,
        limit: Option<usize>,
        schema: PlanSchema,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: BoundExpr,
        schema: PlanSchema,
    },
    Projection {
        input: Box<LogicalPlan>,
        expressions: Vec<BoundExpr>,
        schema: PlanSchema,
    },
    /// Converts a one-column relation into exactly one nullable row. More than
    /// one input row is an execution error, matching SQL scalar-subquery rules.
    Scalarize {
        input: Box<LogicalPlan>,
        schema: PlanSchema,
    },
    /// Temporary correlated-subquery representation. The optimizer must
    /// decorrelate every instance before physical execution.
    DependentJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        kind: DependentJoinKind,
        /// Predicate over the left input that determines whether this
        /// attachment is semantically evaluated (CASE/AND/OR short-circuit).
        guard: Option<BoundExpr>,
        schema: PlanSchema,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_exprs: Vec<BoundExpr>,
        aggregate_exprs: Vec<AggregateExpr>,
        schema: PlanSchema,
    },
    Sort {
        input: Box<LogicalPlan>,
        expressions: Vec<SortExpr>,
        fetch: Option<usize>,
        schema: PlanSchema,
    },
    Limit {
        input: Box<LogicalPlan>,
        offset: usize,
        limit: Option<usize>,
        schema: PlanSchema,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Vec<(BoundExpr, BoundExpr)>,
        /// Predicate over the concatenated left-then-right input schema.
        residual: Option<BoundExpr>,
        /// Separate probe/build comparison for IN/NOT IN null semantics.
        null_aware: Option<(BoundExpr, BoundExpr)>,
        join_type: JoinType,
        schema: PlanSchema,
    },
}

impl LogicalPlan {
    pub fn schema(&self) -> &PlanSchema {
        match self {
            Self::Empty { schema, .. }
            | Self::Scan { schema, .. }
            | Self::Filter { schema, .. }
            | Self::Projection { schema, .. }
            | Self::Scalarize { schema, .. }
            | Self::DependentJoin { schema, .. }
            | Self::Aggregate { schema, .. }
            | Self::Sort { schema, .. }
            | Self::Limit { schema, .. }
            | Self::Join { schema, .. } => schema,
        }
    }

    pub fn explain(&self) -> String {
        self.explain_with_lane_limit(None)
    }

    pub(crate) fn explain_for_query(&self, context: &QueryContext) -> String {
        self.explain_with_lane_limit(Some(context.scheduler.configured_lanes()))
    }

    fn explain_with_lane_limit(&self, lane_limit: Option<usize>) -> String {
        let mut output = String::new();
        self.write_explain(0, lane_limit, &mut output);
        output
    }

    pub(crate) fn freeze_query_statistics(&mut self, context: &QueryContext) {
        match self {
            Self::Empty { .. } => {}
            Self::Scan {
                provider,
                statistics,
                ..
            } => *statistics = provider.query_statistics(context),
            Self::Filter { input, .. }
            | Self::Projection { input, .. }
            | Self::Scalarize { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. } => input.freeze_query_statistics(context),
            Self::Join { left, right, .. } | Self::DependentJoin { left, right, .. } => {
                left.freeze_query_statistics(context);
                right.freeze_query_statistics(context);
            }
        }
    }

    pub(crate) fn collect_scan_providers(&self, providers: &mut Vec<Arc<dyn TableProvider>>) {
        match self {
            Self::Empty { .. } => {}
            Self::Scan { provider, .. } => providers.push(Arc::clone(provider)),
            Self::Filter { input, .. }
            | Self::Projection { input, .. }
            | Self::Scalarize { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. } => input.collect_scan_providers(providers),
            Self::Join { left, right, .. } | Self::DependentJoin { left, right, .. } => {
                left.collect_scan_providers(providers);
                right.collect_scan_providers(providers);
            }
        }
    }

    fn write_explain(&self, depth: usize, lane_limit: Option<usize>, output: &mut String) {
        let indent = "  ".repeat(depth);
        let lanes = lane_limit
            .map(|lanes| lanes.to_string())
            .unwrap_or_else(|| "runtime".to_owned());
        match self {
            Self::Empty { .. } => output.push_str(&format!("{indent}Empty\n")),
            Self::Scan {
                table_name,
                statistics,
                projection,
                limit,
                ..
            } => {
                output.push_str(&format!(
                    "{indent}Scan table={table_name} projection={projection:?} limit={limit:?} rows={:?} bytes={:?} files={} pipeline=fused lane_limit={lanes}\n",
                    statistics.row_count,
                    statistics.total_byte_size,
                    statistics.file_count,
                ));
            }
            Self::Filter {
                input, predicate, ..
            } => {
                output.push_str(&format!("{indent}Filter {}\n", predicate.display_name));
                input.write_explain(depth + 1, lane_limit, output);
            }
            Self::Projection {
                input, expressions, ..
            } => {
                let names: Vec<_> = expressions
                    .iter()
                    .map(|expr| expr.display_name.as_str())
                    .collect();
                output.push_str(&format!("{indent}Projection {names:?}\n"));
                input.write_explain(depth + 1, lane_limit, output);
            }
            Self::Scalarize { input, .. } => {
                output.push_str(&format!("{indent}Scalarize\n"));
                input.write_explain(depth + 1, lane_limit, output);
            }
            Self::DependentJoin {
                left,
                right,
                kind,
                guard,
                ..
            } => {
                output.push_str(&format!(
                    "{indent}DependentJoin kind={kind:?} guarded={} decorrelate=pending\n",
                    guard.is_some()
                ));
                left.write_explain(depth + 1, lane_limit, output);
                right.write_explain(depth + 1, lane_limit, output);
            }
            Self::Aggregate {
                input,
                group_exprs,
                aggregate_exprs,
                ..
            } => {
                let groups: Vec<_> = group_exprs
                    .iter()
                    .map(|expr| expr.display_name.as_str())
                    .collect();
                let aggregates: Vec<_> = aggregate_exprs
                    .iter()
                    .map(|expr| expr.display_name.as_str())
                    .collect();
                let distinct = aggregate_exprs.iter().filter(|expr| expr.distinct).count();
                let distinct_stage = if distinct == 0 && aggregate_exprs.is_empty() {
                    "distinct=group_key_dedup".to_owned()
                } else if distinct == 0 {
                    "distinct=none".to_owned()
                } else {
                    format!(
                        "distinct=count:{distinct},dedup:tagged_full_key,partition:(group,aggregate_id,value),spill:recursive_hash"
                    )
                };
                output.push_str(&format!(
                    "{indent}Aggregate groups={groups:?} aggregates={aggregates:?} {distinct_stage} partial_lane_limit={lanes} final=merge spill=recursive_hash\n"
                ));
                input.write_explain(depth + 1, lane_limit, output);
            }
            Self::Sort {
                input,
                expressions,
                fetch,
                ..
            } => {
                let names: Vec<_> = expressions
                    .iter()
                    .map(|expr| expr.expr.display_name.as_str())
                    .collect();
                output.push_str(&format!(
                    "{indent}Sort {names:?} fetch={fetch:?} run_lane_limit={lanes} merge=kway spill=external_ipc_lz4\n"
                ));
                input.write_explain(depth + 1, lane_limit, output);
            }
            Self::Limit {
                input,
                offset,
                limit,
                ..
            } => {
                output.push_str(&format!("{indent}Limit offset={offset} limit={limit:?}\n"));
                input.write_explain(depth + 1, lane_limit, output);
            }
            Self::Join {
                left,
                right,
                join_type,
                on,
                residual,
                null_aware,
                ..
            } => {
                if on.is_empty() && matches!(right.as_ref(), Self::Scalarize { .. }) {
                    output.push_str(&format!("{indent}ScalarBroadcast build=right\n"));
                } else {
                    output.push_str(&format!(
                        "{indent}{join_type:?}Join keys={} residual={} null_aware={} build=right decorrelation=complete strategy=hash_partition lane_limit={lanes} partitions=64 spill=grace_hash repartition_seeds=2 fallback=sort_merge\n",
                        on.len(),
                        residual.is_some(),
                        null_aware.is_some(),
                    ));
                }
                left.write_explain(depth + 1, lane_limit, output);
                right.write_explain(depth + 1, lane_limit, output);
            }
        }
    }
}

#[derive(Clone)]
pub enum StatementPlan {
    Query(LogicalPlan),
    Explain(LogicalPlan),
    ExplainAnalyze(LogicalPlan),
}

impl fmt::Debug for StatementPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(plan) => formatter
                .debug_tuple("Query")
                .field(&plan.explain())
                .finish(),
            Self::Explain(plan) => formatter
                .debug_tuple("Explain")
                .field(&plan.explain())
                .finish(),
            Self::ExplainAnalyze(plan) => formatter
                .debug_tuple("ExplainAnalyze")
                .field(&plan.explain())
                .finish(),
        }
    }
}

impl StatementPlan {
    pub(crate) fn logical_plan(&self) -> &LogicalPlan {
        match self {
            Self::Query(plan) | Self::Explain(plan) | Self::ExplainAnalyze(plan) => plan,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Query(plan) => Arc::clone(plan.schema().arrow()),
            Self::Explain(_) | Self::ExplainAnalyze(_) => Arc::new(Schema::new(vec![Field::new(
                "explain_value",
                arrow::datatypes::DataType::Utf8,
                false,
            )])),
        }
    }
}

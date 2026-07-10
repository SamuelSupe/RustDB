use std::{fmt, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};

use crate::datasource::TableProvider;

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
            | Self::Aggregate { schema, .. }
            | Self::Sort { schema, .. }
            | Self::Limit { schema, .. }
            | Self::Join { schema, .. } => schema,
        }
    }

    pub fn explain(&self) -> String {
        let mut output = String::new();
        self.write_explain(0, &mut output);
        output
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
            Self::Join { left, right, .. } => {
                left.collect_scan_providers(providers);
                right.collect_scan_providers(providers);
            }
        }
    }

    fn write_explain(&self, depth: usize, output: &mut String) {
        let indent = "  ".repeat(depth);
        match self {
            Self::Empty { .. } => output.push_str(&format!("{indent}Empty\n")),
            Self::Scan {
                table_name,
                provider,
                projection,
                limit,
                ..
            } => {
                let statistics = provider.statistics();
                output.push_str(&format!(
                    "{indent}Scan table={table_name} projection={projection:?} limit={limit:?} rows={:?} bytes={:?} files={}\n",
                    statistics.row_count,
                    statistics.total_byte_size,
                    statistics.file_count,
                ));
            }
            Self::Filter {
                input, predicate, ..
            } => {
                output.push_str(&format!("{indent}Filter {}\n", predicate.display_name));
                input.write_explain(depth + 1, output);
            }
            Self::Projection {
                input, expressions, ..
            } => {
                let names: Vec<_> = expressions
                    .iter()
                    .map(|expr| expr.display_name.as_str())
                    .collect();
                output.push_str(&format!("{indent}Projection {names:?}\n"));
                input.write_explain(depth + 1, output);
            }
            Self::Scalarize { input, .. } => {
                output.push_str(&format!("{indent}Scalarize\n"));
                input.write_explain(depth + 1, output);
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
                output.push_str(&format!(
                    "{indent}Aggregate groups={groups:?} aggregates={aggregates:?}\n"
                ));
                input.write_explain(depth + 1, output);
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
                output.push_str(&format!("{indent}Sort {names:?} fetch={fetch:?}\n"));
                input.write_explain(depth + 1, output);
            }
            Self::Limit {
                input,
                offset,
                limit,
                ..
            } => {
                output.push_str(&format!("{indent}Limit offset={offset} limit={limit:?}\n"));
                input.write_explain(depth + 1, output);
            }
            Self::Join {
                left,
                right,
                join_type,
                on,
                ..
            } => {
                output.push_str(&format!(
                    "{indent}{join_type:?}Join keys={} build=right\n",
                    on.len()
                ));
                left.write_explain(depth + 1, output);
                right.write_explain(depth + 1, output);
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

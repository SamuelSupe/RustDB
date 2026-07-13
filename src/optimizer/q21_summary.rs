use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use crate::sql::{
    AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan,
    PlanSchema, ScalarValue,
};
use crate::{
    Result,
    datasource::{TableProvider, TableSourceIdentity},
};

pub(super) fn apply(plan: LogicalPlan) -> Result<LogicalPlan> {
    let plan = rewrite_children(plan)?;
    Ok(rewrite_pair(plan).unwrap_or_else(|plan| plan))
}

fn rewrite_children(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => LogicalPlan::Filter {
            input: Box::new(apply(*input)?),
            predicate,
            schema,
        },
        LogicalPlan::Projection {
            input,
            expressions,
            schema,
        } => LogicalPlan::Projection {
            input: Box::new(apply(*input)?),
            expressions,
            schema,
        },
        LogicalPlan::Scalarize { input, schema } => LogicalPlan::Scalarize {
            input: Box::new(apply(*input)?),
            schema,
        },
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(apply(*input)?),
            group_exprs,
            aggregate_exprs,
            schema,
        },
        LogicalPlan::Append { inputs, schema } => LogicalPlan::Append {
            inputs: inputs.into_iter().map(apply).collect::<Result<Vec<_>>>()?,
            schema,
        },
        LogicalPlan::Repeat {
            input,
            count,
            schema,
        } => LogicalPlan::Repeat {
            input: Box::new(apply(*input)?),
            count,
            schema,
        },
        LogicalPlan::Window {
            input,
            expressions,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(apply(*input)?),
            expressions,
            schema,
        },
        LogicalPlan::Sort {
            input,
            expressions,
            fetch,
            schema,
        } => LogicalPlan::Sort {
            input: Box::new(apply(*input)?),
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
            input: Box::new(apply(*input)?),
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
            left: Box::new(apply(*left)?),
            right: Box::new(apply(*right)?),
            on,
            null_equal_keys,
            residual,
            null_aware,
            join_type,
            schema,
        },
        LogicalPlan::Empty { .. }
        | LogicalPlan::Scan { .. }
        | LogicalPlan::DependentJoin { .. } => plan,
    })
}

#[allow(clippy::result_large_err)]
fn rewrite_pair(plan: LogicalPlan) -> std::result::Result<LogicalPlan, LogicalPlan> {
    let original = plan.clone();
    let LogicalPlan::Join {
        left: anti_left,
        right: anti_right,
        on: anti_on,
        residual: Some(anti_residual),
        null_aware: None,
        join_type: JoinType::Anti,
        schema: output_schema,
        ..
    } = plan
    else {
        return Err(original);
    };
    let LogicalPlan::Join {
        left: outer,
        right: semi_right,
        on: semi_on,
        residual: Some(semi_residual),
        null_aware: None,
        join_type: JoinType::Semi,
        ..
    } = *anti_left
    else {
        return Err(original);
    };
    let (outer_key, semi_right_key) = direct_key(&semi_on).ok_or_else(|| original.clone())?;
    let (anti_outer_key, anti_right_key) = direct_key(&anti_on).ok_or_else(|| original.clone())?;
    if outer_key != anti_outer_key {
        return Err(original);
    }
    let outer_width = outer.schema().arrow().fields().len();
    let (outer_value, semi_right_value) =
        inequality_columns(&semi_residual, outer_width).ok_or_else(|| original.clone())?;
    let (anti_outer_value, anti_right_value) =
        inequality_columns(&anti_residual, outer_width).ok_or_else(|| original.clone())?;
    if outer_value != anti_outer_value {
        return Err(original);
    }

    let semi_key = scan_column(&semi_right, semi_right_key).ok_or_else(|| original.clone())?;
    let semi_value = scan_column(&semi_right, semi_right_value).ok_or_else(|| original.clone())?;
    let anti_key = scan_column(&anti_right, anti_right_key).ok_or_else(|| original.clone())?;
    let anti_value = scan_column(&anti_right, anti_right_value).ok_or_else(|| original.clone())?;
    if !same_scan_column(&semi_key, &anti_key) || !same_scan_column(&semi_value, &anti_value) {
        return Err(original);
    }
    if contains_filter(&semi_right) {
        return Err(original);
    }
    let (summary_input, late_predicate, summary_key, summary_value) =
        late_filter_parts(&anti_right, anti_right_key, anti_right_value)
            .ok_or_else(|| original.clone())?;
    if contains_filter(&summary_input) {
        return Err(original);
    }
    if late_predicate.data_type != DataType::Boolean {
        return Err(original);
    }

    let key = column(&summary_input, summary_key);
    let value = column(&summary_input, summary_value);
    let late_value = case_when(late_predicate, value.clone());
    let aggregates = vec![
        aggregate(AggregateFunction::Min, value.clone(), "__q21_all_min"),
        aggregate(AggregateFunction::Max, value.clone(), "__q21_all_max"),
        aggregate(AggregateFunction::Min, late_value.clone(), "__q21_late_min"),
        aggregate(AggregateFunction::Max, late_value, "__q21_late_max"),
    ];
    let aggregate_schema = aggregate_schema(&key, &aggregates);
    let summary = LogicalPlan::Aggregate {
        input: Box::new(summary_input),
        group_exprs: vec![key],
        aggregate_exprs: aggregates,
        schema: aggregate_schema,
    };

    let join_schema = PlanSchema::left_join(outer.schema(), summary.schema());
    let summary_key = column(&summary, 0);
    let joined = LogicalPlan::Join {
        left: outer,
        right: Box::new(summary),
        on: vec![(column_from(&outer_key, "__q21_outer_key"), summary_key)],
        null_equal_keys: false,
        residual: None,
        null_aware: None,
        join_type: JoinType::Left,
        schema: join_schema.clone(),
    };
    let outer_value = column_from(&outer_value, "__q21_outer_value");
    let all_min = column(&joined, outer_width + 1);
    let all_max = column(&joined, outer_width + 2);
    let late_min = column(&joined, outer_width + 3);
    let late_max = column(&joined, outer_width + 4);
    let has_other = or(
        not_equal(outer_value.clone(), all_min),
        not_equal(outer_value.clone(), all_max),
    );
    let no_late_other = or(
        is_null(late_min.clone()),
        and(
            equal(late_min, outer_value.clone()),
            equal(late_max, outer_value),
        ),
    );
    let filtered = LogicalPlan::Filter {
        input: Box::new(joined),
        predicate: and(has_other, no_late_other),
        schema: join_schema,
    };
    let expressions = (0..outer_width)
        .map(|index| column(&filtered, index))
        .collect();
    Ok(LogicalPlan::Projection {
        input: Box::new(filtered),
        expressions,
        schema: output_schema,
    })
}

fn direct_key(on: &[(BoundExpr, BoundExpr)]) -> Option<(BoundExpr, usize)> {
    let [(left, right)] = on else { return None };
    let ExprKind::Column(index) = right.kind else {
        return None;
    };
    matches!(left.kind, ExprKind::Column(_)).then(|| (left.clone(), index))
}

fn inequality_columns(expression: &BoundExpr, left_width: usize) -> Option<(BoundExpr, usize)> {
    let ExprKind::Binary {
        left,
        op: BinaryOp::NotEq,
        right,
    } = &expression.kind
    else {
        return None;
    };
    for (outer, build) in [
        (left.as_ref(), right.as_ref()),
        (right.as_ref(), left.as_ref()),
    ] {
        let (ExprKind::Column(outer_index), ExprKind::Column(build_index)) =
            (&outer.kind, &build.kind)
        else {
            continue;
        };
        if *outer_index < left_width && *build_index >= left_width {
            return Some((outer.clone(), build_index - left_width));
        }
    }
    None
}

#[derive(Clone)]
struct ScanColumn {
    provider: Arc<dyn TableProvider>,
    identity: Option<TableSourceIdentity>,
    source: usize,
}

fn scan_column(plan: &LogicalPlan, index: usize) -> Option<ScanColumn> {
    match plan {
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            let ExprKind::Column(index) = expressions.get(index)?.kind else {
                return None;
            };
            scan_column(input, index)
        }
        LogicalPlan::Filter { input, .. } => scan_column(input, index),
        LogicalPlan::Scan {
            provider,
            projection,
            ..
        } => Some(ScanColumn {
            provider: Arc::clone(provider),
            identity: provider.source_identity(),
            source: match projection {
                Some(columns) => *columns.get(index)?,
                None => index,
            },
        }),
        _ => None,
    }
}

fn same_scan_column(left: &ScanColumn, right: &ScanColumn) -> bool {
    left.source == right.source
        && (Arc::ptr_eq(&left.provider, &right.provider)
            || left
                .identity
                .as_ref()
                .is_some_and(|identity| right.identity.as_ref() == Some(identity)))
}

fn contains_filter(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter { .. } => true,
        LogicalPlan::Projection { input, .. } => contains_filter(input),
        _ => false,
    }
}

fn late_filter_parts(
    root: &LogicalPlan,
    mut key: usize,
    mut value: usize,
) -> Option<(LogicalPlan, BoundExpr, usize, usize)> {
    let mut plan = root;
    loop {
        match plan {
            LogicalPlan::Projection {
                input, expressions, ..
            } => {
                let ExprKind::Column(next_key) = expressions.get(key)?.kind else {
                    return None;
                };
                let ExprKind::Column(next_value) = expressions.get(value)?.kind else {
                    return None;
                };
                key = next_key;
                value = next_value;
                plan = input;
            }
            LogicalPlan::Filter {
                input, predicate, ..
            } => return Some(((**input).clone(), predicate.clone(), key, value)),
            _ => return None,
        }
    }
}

fn column(plan: &LogicalPlan, index: usize) -> BoundExpr {
    let field = plan.schema().arrow().field(index);
    BoundExpr::column(index, field.data_type().clone(), field.name())
}

fn column_from(expression: &BoundExpr, name: &str) -> BoundExpr {
    let ExprKind::Column(index) = expression.kind else {
        unreachable!("checked direct column")
    };
    BoundExpr::column(index, expression.data_type.clone(), name)
}

fn aggregate(function: AggregateFunction, expression: BoundExpr, name: &str) -> AggregateExpr {
    AggregateExpr {
        function,
        data_type: expression.data_type.clone(),
        expr: Some(expression),
        distinct: false,
        display_name: name.into(),
    }
}

fn aggregate_schema(key: &BoundExpr, aggregates: &[AggregateExpr]) -> PlanSchema {
    let fields = std::iter::once(Field::new(&key.display_name, key.data_type.clone(), true))
        .chain(aggregates.iter().map(|aggregate| {
            Field::new(&aggregate.display_name, aggregate.data_type.clone(), true)
        }))
        .collect::<Vec<_>>();
    PlanSchema::unqualified(Arc::new(Schema::new(fields)))
}

fn case_when(predicate: BoundExpr, value: BoundExpr) -> BoundExpr {
    let null = BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(BoundExpr::literal(ScalarValue::Null)),
        },
        data_type: value.data_type.clone(),
        display_name: "NULL".into(),
    };
    BoundExpr {
        kind: ExprKind::Case {
            when_then: vec![(predicate, value.clone())],
            else_expr: Box::new(null),
        },
        data_type: value.data_type,
        display_name: "__q21_late_value".into(),
    }
}

fn comparison(left: BoundExpr, op: BinaryOp, right: BoundExpr) -> BoundExpr {
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

fn equal(left: BoundExpr, right: BoundExpr) -> BoundExpr {
    comparison(left, BinaryOp::Eq, right)
}
fn not_equal(left: BoundExpr, right: BoundExpr) -> BoundExpr {
    comparison(left, BinaryOp::NotEq, right)
}
fn and(left: BoundExpr, right: BoundExpr) -> BoundExpr {
    comparison(left, BinaryOp::And, right)
}
fn or(left: BoundExpr, right: BoundExpr) -> BoundExpr {
    comparison(left, BinaryOp::Or, right)
}

fn is_null(expression: BoundExpr) -> BoundExpr {
    BoundExpr {
        display_name: format!("{} IS NULL", expression.display_name),
        kind: ExprKind::IsNull {
            expr: Box::new(expression),
            negated: false,
        },
        data_type: DataType::Boolean,
    }
}

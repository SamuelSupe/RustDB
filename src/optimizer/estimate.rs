use arrow::datatypes::Schema;

use crate::{
    runtime::estimate_schema_batch_bytes,
    sql::{BinaryOp, BoundExpr, ExprKind, LogicalPlan, ScalarValue, UnaryOp},
};

const SELECTIVITY_SCALE: u64 = 1_000_000;
const EQUALITY_SELECTIVITY: u64 = SELECTIVITY_SCALE / 10;
const RANGE_SELECTIVITY: u64 = SELECTIVITY_SCALE / 3;
const WIDTH_SAMPLE_ROWS: usize = 1_024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Estimate {
    pub(super) rows: Option<u64>,
    pub(super) output_bytes: Option<u64>,
}

pub(super) fn estimate(plan: &LogicalPlan) -> Estimate {
    match plan {
        LogicalPlan::Empty {
            produce_one_row,
            schema,
        } => from_rows(schema.arrow(), u64::from(*produce_one_row)),
        LogicalPlan::Scan {
            statistics,
            pushed_filter,
            exact_filter,
            schema,
            ..
        } => {
            let rows = if exact_filter.is_some() {
                statistics.row_count.map(|rows| {
                    scale_rows(
                        rows,
                        pushed_filter
                            .as_ref()
                            .map_or(SELECTIVITY_SCALE, selectivity),
                    )
                })
            } else {
                statistics.row_count
            };
            let mut estimate = from_optional_rows(schema.arrow(), rows);
            if estimate.output_bytes.is_none() {
                estimate.output_bytes = statistics.total_byte_size;
            }
            estimate
        }
        LogicalPlan::Filter {
            input,
            predicate,
            schema,
        } => {
            let rows = estimate(input)
                .rows
                .map(|rows| scale_rows(rows, selectivity(predicate)));
            from_optional_rows(schema.arrow(), rows)
        }
        LogicalPlan::Projection { input, schema, .. }
        | LogicalPlan::Window { input, schema, .. }
        | LogicalPlan::Sort { input, schema, .. } => {
            from_optional_rows(schema.arrow(), estimate(input).rows)
        }
        LogicalPlan::Append { inputs, schema } => {
            let rows = sum_known(inputs.iter().map(estimate).map(|estimate| estimate.rows));
            from_optional_rows(schema.arrow(), rows)
        }
        LogicalPlan::Limit {
            input,
            offset,
            limit,
            schema,
        } => {
            let rows = estimate(input).rows.map(|rows| {
                rows.saturating_sub(usize_to_u64(*offset))
                    .min(limit.map(usize_to_u64).unwrap_or(u64::MAX))
            });
            from_optional_rows(schema.arrow(), rows)
        }
        LogicalPlan::Scalarize { schema, .. } => from_rows(schema.arrow(), 1),
        LogicalPlan::Aggregate {
            group_exprs,
            schema,
            ..
        } if group_exprs.is_empty() => from_rows(schema.arrow(), 1),
        LogicalPlan::Aggregate { input, schema, .. } => {
            let rows = match estimate(input).rows {
                Some(0) => Some(0),
                Some(_) | None => None,
            };
            from_optional_rows(schema.arrow(), rows)
        }
        // Repetition and joins need value or column statistics that are not
        // available in the current coarse table statistics. Do not invent a
        // cardinality that could make an unsafe build-memory prediction.
        LogicalPlan::Repeat { .. }
        | LogicalPlan::Join { .. }
        | LogicalPlan::DependentJoin { .. } => Estimate::default(),
    }
}

fn from_optional_rows(schema: &Schema, rows: Option<u64>) -> Estimate {
    Estimate {
        rows,
        output_bytes: rows.map(|rows| output_bytes(schema, rows)),
    }
}

fn from_rows(schema: &Schema, rows: u64) -> Estimate {
    from_optional_rows(schema, Some(rows))
}

fn sum_known(mut values: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    values.try_fold(0_u64, |total, value| {
        value.map(|value| total.saturating_add(value))
    })
}

fn output_bytes(schema: &Schema, rows: u64) -> u64 {
    if rows == 0 {
        return 0;
    }
    let fixed = estimate_schema_batch_bytes(schema, 0);
    let sampled = estimate_schema_batch_bytes(schema, WIDTH_SAMPLE_ROWS);
    let variable = sampled.saturating_sub(fixed);
    let scaled = ceil_div(
        (variable as u128).saturating_mul(u128::from(rows)),
        WIDTH_SAMPLE_ROWS as u128,
    );
    u64::try_from((fixed as u128).saturating_add(scaled)).unwrap_or(u64::MAX)
}

fn selectivity(expression: &BoundExpr) -> u64 {
    estimated_selectivity(expression).unwrap_or(SELECTIVITY_SCALE)
}

fn estimated_selectivity(expression: &BoundExpr) -> Option<u64> {
    match &expression.kind {
        ExprKind::Literal(ScalarValue::Boolean(true)) => Some(SELECTIVITY_SCALE),
        ExprKind::Literal(ScalarValue::Boolean(false) | ScalarValue::Null) => Some(0),
        ExprKind::Binary { left, op, right } => match op {
            BinaryOp::And => match (estimated_selectivity(left), estimated_selectivity(right)) {
                (Some(left), Some(right)) => Some(multiply(left, right)),
                (Some(_), None) | (None, Some(_)) | (None, None) => None,
            },
            BinaryOp::Or => match (estimated_selectivity(left), estimated_selectivity(right)) {
                (Some(left), Some(right)) => Some(
                    left.saturating_add(right)
                        .saturating_sub(multiply(left, right))
                        .min(SELECTIVITY_SCALE),
                ),
                (Some(SELECTIVITY_SCALE), None) | (None, Some(SELECTIVITY_SCALE)) => {
                    Some(SELECTIVITY_SCALE)
                }
                (Some(_), None) | (None, Some(_)) | (None, None) => None,
            },
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
                if null_literal(left) || null_literal(right) =>
            {
                Some(0)
            }
            BinaryOp::Eq if literal_comparison(left, right) => Some(EQUALITY_SELECTIVITY),
            BinaryOp::NotEq if literal_comparison(left, right) => {
                Some(SELECTIVITY_SCALE - EQUALITY_SELECTIVITY)
            }
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
                if literal_comparison(left, right) =>
            {
                Some(RANGE_SELECTIVITY)
            }
            _ => None,
        },
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } if always_unknown(expr) => Some(0),
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } => estimated_selectivity(expr)
            .map(|selectivity| SELECTIVITY_SCALE.saturating_sub(selectivity)),
        ExprKind::IsNull { negated, .. } => Some(if *negated {
            SELECTIVITY_SCALE - EQUALITY_SELECTIVITY
        } else {
            EQUALITY_SELECTIVITY
        }),
        _ => None,
    }
}

fn always_unknown(expression: &BoundExpr) -> bool {
    match &expression.kind {
        ExprKind::Literal(ScalarValue::Null) => true,
        ExprKind::Binary {
            left,
            op:
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq,
            right,
        } => null_literal(left) || null_literal(right),
        ExprKind::Unary {
            op: UnaryOp::Not,
            expr,
        } => always_unknown(expr),
        _ => false,
    }
}

fn null_literal(expression: &BoundExpr) -> bool {
    matches!(&expression.kind, ExprKind::Literal(ScalarValue::Null))
}

fn literal_comparison(left: &BoundExpr, right: &BoundExpr) -> bool {
    matches!(&left.kind, ExprKind::Literal(_)) ^ matches!(&right.kind, ExprKind::Literal(_))
}

fn multiply(left: u64, right: u64) -> u64 {
    u64::try_from(u128::from(left) * u128::from(right) / u128::from(SELECTIVITY_SCALE))
        .unwrap_or(SELECTIVITY_SCALE)
}

fn scale_rows(rows: u64, selectivity: u64) -> u64 {
    u64::try_from(ceil_div(
        u128::from(rows) * u128::from(selectivity),
        u128::from(SELECTIVITY_SCALE),
    ))
    .unwrap_or(u64::MAX)
}

fn ceil_div(value: u128, divisor: u128) -> u128 {
    value / divisor + u128::from(!value.is_multiple_of(divisor))
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;

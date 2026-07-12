use std::sync::Arc;

use arrow::{
    array::new_null_array,
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::datasource::{ComparisonOp, PredicateValue, ScanPredicate, ScanRequest, TableProvider};
use crate::runtime::{
    BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream,
    estimate_schema_batch_bytes,
};
use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};
use crate::{Error, Result};

pub(crate) async fn scan(
    provider: Arc<dyn TableProvider>,
    projection: Option<Vec<usize>>,
    predicate: Option<&BoundExpr>,
    limit: Option<usize>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> Result<MemoryBatchStream> {
    let mut request = ScanRequest::new(batch_size);
    request.projection = projection.clone();
    request.predicate = predicate.and_then(to_scan_predicate);
    request.limit = limit;
    let decoded_schema = request.projected_schema(&provider.schema())?;
    let preclaim = estimate_schema_batch_bytes(decoded_schema.as_ref(), batch_size).max(1);
    let mut input = provider.scan(request, Arc::clone(&context)).await?;
    let stream = async_stream::try_stream! {
        use futures::StreamExt;
        loop {
            context.check_cancelled()?;
            // Reserve decoder credit before polling. The exact returned Arrow
            // buffers are reconciled immediately by from_reservation.
            let reservation = context.reserve_memory(preclaim, "scan decode credit").await?;
            let next = tokio::select! {
                _ = context.control.cancelled() => Err(Error::Cancelled),
                next = input.next() => Ok(next),
            }?;
            let Some(batch) = next else {
                break;
            };
            let mut batch = BatchEnvelope::from_reservation(
                batch?,
                reservation,
                "scan decoded",
            )?;
            if let Some(projection) = projection.as_deref().filter(|projection| !projection.is_empty()) {
                let workspace_bytes = estimate_schema_batch_bytes(schema.as_ref(), batch.num_rows());
                let workspace = context
                    .reserve_memory_while_holding(
                        workspace_bytes,
                        batch.memory_size(),
                        "scan projection expansion workspace",
                    )
                    .await?;
                let expanded = expand_projection(batch.batch().clone(), &schema, projection)?;
                batch = batch.replace_with_reservation(
                    expanded,
                    workspace,
                    "scan projection expansion",
                )?;
            }
            yield batch;
        }
    };
    Ok(boxed_memory_batch_stream(stream))
}

pub(super) fn expand_projection(
    batch: RecordBatch,
    full_schema: &SchemaRef,
    projection: &[usize],
) -> Result<RecordBatch> {
    if batch.num_columns() != projection.len() {
        return Err(Error::Execution(format!(
            "scan returned {} columns for a {}-column projection",
            batch.num_columns(),
            projection.len()
        )));
    }
    // An explicit empty projection is a physical zero-column contract, not a
    // request to reconstruct every logical column as NULL.  Keeping the batch
    // empty lets metadata-only scans such as Parquet COUNT(*) flow through
    // fused operators without allocating one validity buffer per source field.
    // Operators above this boundary can still use the row count, and any
    // visible literal projection materializes its own correctly typed output.
    if projection.is_empty() {
        return Ok(batch);
    }
    let mut projected_position = vec![None; full_schema.fields().len()];
    for (position, index) in projection.iter().copied().enumerate() {
        let slot = projected_position.get_mut(index).ok_or_else(|| {
            Error::Internal(format!("scan projection index {index} is out of bounds"))
        })?;
        *slot = Some(position);
    }
    let columns = full_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| match projected_position[index] {
            Some(position) => Arc::clone(batch.column(position)),
            None => new_null_array(field.data_type(), batch.num_rows()),
        })
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(
        full_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if projected_position[index].is_some() {
                    Arc::clone(field)
                } else {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                }
            })
            .collect::<Vec<_>>(),
        full_schema.metadata().clone(),
    ));
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(super) fn to_scan_predicate(expr: &BoundExpr) -> Option<ScanPredicate> {
    match &expr.kind {
        ExprKind::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => Some(ScanPredicate::And(vec![
            to_scan_predicate(left)?,
            to_scan_predicate(right)?,
        ])),
        ExprKind::Binary { left, op, right } => {
            let comparison = comparison_op(*op)?;
            if let (Some(column), Some(value)) = (column_index(left), literal(right)) {
                Some(ScanPredicate::Comparison {
                    column,
                    op: comparison,
                    value,
                })
            } else if let (Some(value), Some(column)) = (literal(left), column_index(right)) {
                Some(ScanPredicate::Comparison {
                    column,
                    op: reverse(comparison),
                    value,
                })
            } else {
                None
            }
        }
        ExprKind::IsNull { expr, negated } => column_index(expr).map(|column| {
            if *negated {
                ScanPredicate::IsNotNull { column }
            } else {
                ScanPredicate::IsNull { column }
            }
        }),
        _ => None,
    }
}

fn column_index(expr: &BoundExpr) -> Option<usize> {
    match &expr.kind {
        ExprKind::Column(index) => Some(*index),
        ExprKind::Cast { expr } => column_index(expr),
        _ => None,
    }
}

fn literal(expr: &BoundExpr) -> Option<PredicateValue> {
    match &expr.kind {
        ExprKind::Literal(value) => predicate_value(value),
        ExprKind::Cast { expr: input } => cast_predicate_value(literal(input)?, &expr.data_type),
        _ => None,
    }
}

fn cast_predicate_value(value: PredicateValue, target: &DataType) -> Option<PredicateValue> {
    let DataType::Decimal128(precision, scale) = target else {
        return Some(value);
    };
    let (value, source_scale) = match value {
        PredicateValue::Int64(value) => (i128::from(value), 0),
        PredicateValue::UInt64(value) => (i128::from(value), 0),
        PredicateValue::Decimal128 { value, scale, .. } => (value, scale),
        _ => return None,
    };
    let value = rescale_decimal(value, source_scale, *scale)?;
    decimal_fits_precision(value, *precision).then_some(PredicateValue::Decimal128 {
        value,
        precision: *precision,
        scale: *scale,
    })
}

fn rescale_decimal(value: i128, source_scale: i8, target_scale: i8) -> Option<i128> {
    let difference = i16::from(target_scale) - i16::from(source_scale);
    if difference == 0 {
        return Some(value);
    }
    let exponent = u32::from(difference.unsigned_abs());
    let factor = 10_i128.checked_pow(exponent)?;
    if difference > 0 {
        value.checked_mul(factor)
    } else if value % factor == 0 {
        Some(value / factor)
    } else {
        None
    }
}

fn decimal_fits_precision(value: i128, precision: u8) -> bool {
    (1..=38).contains(&precision)
        && 10_u128
            .checked_pow(u32::from(precision))
            .is_some_and(|limit| value.unsigned_abs() < limit)
}

fn predicate_value(value: &ScalarValue) -> Option<PredicateValue> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Boolean(value) => Some(PredicateValue::Boolean(*value)),
        ScalarValue::Int64(value) => Some(PredicateValue::Int64(*value)),
        ScalarValue::UInt64(value) => Some(PredicateValue::UInt64(*value)),
        ScalarValue::Float64(value) => Some(PredicateValue::Float64(*value)),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => Some(PredicateValue::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        }),
        ScalarValue::Date32(value) => Some(PredicateValue::Date32(*value)),
        ScalarValue::TimestampMicrosecond(value) => Some(PredicateValue::TimestampMicros(*value)),
        ScalarValue::DayInterval(_) | ScalarValue::MonthInterval(_) => None,
        ScalarValue::Utf8(value) => Some(PredicateValue::Utf8(value.clone())),
    }
}

fn comparison_op(op: BinaryOp) -> Option<ComparisonOp> {
    match op {
        BinaryOp::Eq => Some(ComparisonOp::Eq),
        BinaryOp::NotEq => Some(ComparisonOp::NotEq),
        BinaryOp::Lt => Some(ComparisonOp::Lt),
        BinaryOp::LtEq => Some(ComparisonOp::LtEq),
        BinaryOp::Gt => Some(ComparisonOp::Gt),
        BinaryOp::GtEq => Some(ComparisonOp::GtEq),
        _ => None,
    }
}

fn reverse(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Eq => ComparisonOp::Eq,
        ComparisonOp::NotEq => ComparisonOp::NotEq,
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::LtEq => ComparisonOp::GtEq,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::GtEq => ComparisonOp::LtEq,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::{RecordBatch, RecordBatchOptions},
    };

    use super::{expand_projection, predicate_value, to_scan_predicate};
    use crate::{
        datasource::{ComparisonOp, PredicateValue, ScanPredicate},
        sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue},
    };

    #[test]
    fn empty_projection_stays_zero_column_and_preserves_rows() {
        let physical = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new().with_row_count(Some(7));
        let batch = RecordBatch::try_new_with_options(physical, Vec::new(), &options).unwrap();
        let logical = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
        ]));

        let expanded = expand_projection(batch, &logical, &[]).unwrap();

        assert_eq!(expanded.num_rows(), 7);
        assert_eq!(expanded.num_columns(), 0);
        assert!(expanded.schema().fields().is_empty());
        assert_eq!(expanded.get_array_memory_size(), 0);
    }

    #[test]
    fn decimal_scan_predicate_preserves_precision_and_scale() {
        assert_eq!(
            predicate_value(&ScalarValue::Decimal128 {
                value: 1,
                precision: 1,
                scale: 0,
            }),
            Some(PredicateValue::Decimal128 {
                value: 1,
                precision: 1,
                scale: 0,
            })
        );
    }

    #[test]
    fn decimal_scan_predicate_applies_literal_cast_scale() {
        let column = BoundExpr::column(0, DataType::Decimal128(10, 2), "amount");
        let integer = BoundExpr::literal(ScalarValue::Int64(1));
        let decimal_integer = BoundExpr {
            kind: ExprKind::Cast {
                expr: Box::new(integer),
            },
            data_type: DataType::Decimal128(1, 0),
            display_name: "1".into(),
        };
        let scaled = BoundExpr {
            kind: ExprKind::Cast {
                expr: Box::new(decimal_integer),
            },
            data_type: DataType::Decimal128(3, 2),
            display_name: "1".into(),
        };
        let comparison = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(column),
                op: BinaryOp::Eq,
                right: Box::new(scaled),
            },
            data_type: DataType::Boolean,
            display_name: "amount = 1".into(),
        };

        assert_eq!(
            to_scan_predicate(&comparison),
            Some(ScanPredicate::Comparison {
                column: 0,
                op: ComparisonOp::Eq,
                value: PredicateValue::Decimal128 {
                    value: 100,
                    precision: 3,
                    scale: 2,
                },
            })
        );
    }
}

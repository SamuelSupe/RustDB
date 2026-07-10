use arrow::{datatypes::DataType, record_batch::RecordBatch};

use crate::{
    Error, Result,
    sql::{AggregateExpr, AggregateFunction},
};

use super::super::value::{CellValue, cell};

pub(super) struct GroupState {
    pub(super) key: Vec<CellValue>,
    pub(super) aggregates: Vec<AggregateState>,
}

impl GroupState {
    pub(super) fn new(key: Vec<CellValue>, expressions: &[AggregateExpr]) -> Self {
        Self {
            key,
            aggregates: expressions.iter().map(AggregateState::new).collect(),
        }
    }
}

pub(super) enum AggregateState {
    Count(i64),
    SumSigned {
        value: i128,
        seen: bool,
    },
    SumUnsigned {
        value: u128,
        seen: bool,
    },
    SumFloat {
        value: f64,
        seen: bool,
    },
    SumDecimal {
        value: i128,
        seen: bool,
        precision: u8,
    },
    Min(Option<CellValue>),
    Max(Option<CellValue>),
    Avg {
        sum: f64,
        count: u64,
    },
    AvgDecimal {
        sum: i128,
        count: u64,
        precision: u8,
    },
}

impl AggregateState {
    pub(super) fn new(expression: &AggregateExpr) -> Self {
        match expression.function {
            AggregateFunction::Count => Self::Count(0),
            AggregateFunction::Sum => match expression.data_type {
                DataType::UInt64 => Self::SumUnsigned {
                    value: 0,
                    seen: false,
                },
                DataType::Float64 => Self::SumFloat {
                    value: 0.0,
                    seen: false,
                },
                DataType::Decimal128(precision, _) => Self::SumDecimal {
                    value: 0,
                    seen: false,
                    precision,
                },
                _ => Self::SumSigned {
                    value: 0,
                    seen: false,
                },
            },
            AggregateFunction::Min => Self::Min(None),
            AggregateFunction::Max => Self::Max(None),
            AggregateFunction::Avg => match expression.data_type {
                DataType::Decimal128(precision, _) => Self::AvgDecimal {
                    sum: 0,
                    count: 0,
                    precision,
                },
                _ => Self::Avg { sum: 0.0, count: 0 },
            },
        }
    }

    pub(super) fn update(
        &mut self,
        expression: &AggregateExpr,
        value: Option<CellValue>,
    ) -> Result<()> {
        match self {
            Self::Count(count) => {
                if expression.expr.is_none() || value.as_ref().is_some_and(|value| !value.is_null())
                {
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::Execution("count overflowed INT64".into()))?;
                }
            }
            Self::SumSigned { value: sum, seen } => match value.unwrap_or(CellValue::Null) {
                CellValue::Null => {}
                CellValue::Int64(value) => {
                    *sum = sum
                        .checked_add(i128::from(value))
                        .ok_or_else(|| Error::Execution("signed sum overflow".into()))?;
                    *seen = true;
                }
                other => return Err(unexpected_value(expression, &other)),
            },
            Self::SumUnsigned { value: sum, seen } => match value.unwrap_or(CellValue::Null) {
                CellValue::Null => {}
                CellValue::UInt64(value) => {
                    *sum = sum
                        .checked_add(u128::from(value))
                        .ok_or_else(|| Error::Execution("unsigned sum overflow".into()))?;
                    *seen = true;
                }
                other => return Err(unexpected_value(expression, &other)),
            },
            Self::SumFloat { value: sum, seen } => match value.unwrap_or(CellValue::Null) {
                CellValue::Null => {}
                value => {
                    *sum += value.as_f64()?;
                    *seen = true;
                }
            },
            Self::SumDecimal {
                value: sum, seen, ..
            } => match value.unwrap_or(CellValue::Null) {
                CellValue::Null => {}
                CellValue::Decimal128(value) => {
                    *sum = sum
                        .checked_add(value)
                        .ok_or_else(|| Error::Execution("decimal sum overflowed i128".into()))?;
                    *seen = true;
                }
                other => return Err(unexpected_value(expression, &other)),
            },
            Self::Min(current) => update_extreme(current, value, false)?,
            Self::Max(current) => update_extreme(current, value, true)?,
            Self::Avg { sum, count } => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    *sum += value.as_f64()?;
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::Execution("average count overflow".into()))?;
                }
            }
            Self::AvgDecimal { sum, count, .. } => {
                if let Some(value) = value.filter(|value| !value.is_null()) {
                    let CellValue::Decimal128(value) = value else {
                        return Err(unexpected_value(expression, &value));
                    };
                    *sum = sum.checked_add(value).ok_or_else(|| {
                        Error::Execution("decimal average sum overflowed i128".into())
                    })?;
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::Execution("decimal average count overflow".into()))?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(&self) -> Result<CellValue> {
        match self {
            Self::Count(value) => Ok(CellValue::Int64(*value)),
            Self::SumSigned { seen: false, .. }
            | Self::SumUnsigned { seen: false, .. }
            | Self::SumFloat { seen: false, .. }
            | Self::SumDecimal { seen: false, .. } => Ok(CellValue::Null),
            Self::SumSigned { value, .. } => i64::try_from(*value)
                .map(CellValue::Int64)
                .map_err(|_| Error::Execution("sum overflowed INT64".into())),
            Self::SumUnsigned { value, .. } => u64::try_from(*value)
                .map(CellValue::UInt64)
                .map_err(|_| Error::Execution("sum overflowed UINT64".into())),
            Self::SumFloat { value, .. } => Ok(CellValue::Float64(*value)),
            Self::SumDecimal {
                value, precision, ..
            } => decimal_value(*value, *precision, "sum"),
            Self::Min(value) | Self::Max(value) => Ok(value.clone().unwrap_or(CellValue::Null)),
            Self::Avg { sum, count: 0 } => {
                let _ = sum;
                Ok(CellValue::Null)
            }
            Self::Avg { sum, count } => Ok(CellValue::Float64(*sum / *count as f64)),
            Self::AvgDecimal { count: 0, .. } => Ok(CellValue::Null),
            Self::AvgDecimal {
                sum,
                count,
                precision,
            } => decimal_value(*sum / i128::from(*count), *precision, "average"),
        }
    }

    pub(super) fn partial_values(&self) -> Result<Vec<CellValue>> {
        match self {
            Self::Avg { sum, count } => {
                Ok(vec![CellValue::Float64(*sum), CellValue::UInt64(*count)])
            }
            Self::AvgDecimal { sum, count, .. } => Ok(vec![
                decimal_value(*sum, 38, "partial average sum")?,
                CellValue::UInt64(*count),
            ]),
            Self::SumSigned { value, seen } => Ok(vec![
                CellValue::Int64(
                    i64::try_from(*value)
                        .map_err(|_| Error::Execution("partial sum overflowed INT64".into()))?,
                ),
                CellValue::Boolean(*seen),
            ]),
            Self::SumUnsigned { value, seen } => Ok(vec![
                CellValue::UInt64(
                    u64::try_from(*value)
                        .map_err(|_| Error::Execution("partial sum overflowed UINT64".into()))?,
                ),
                CellValue::Boolean(*seen),
            ]),
            Self::SumFloat { value, seen } => {
                Ok(vec![CellValue::Float64(*value), CellValue::Boolean(*seen)])
            }
            Self::SumDecimal {
                value,
                seen,
                precision,
            } => Ok(vec![
                decimal_value(*value, *precision, "partial sum")?,
                CellValue::Boolean(*seen),
            ]),
            _ => Ok(vec![self.finish()?]),
        }
    }

    pub(super) fn merge_partial(
        &mut self,
        expression: &AggregateExpr,
        batch: &RecordBatch,
        row: usize,
        column: &mut usize,
    ) -> Result<()> {
        match self {
            Self::Count(current) => {
                let value = cell(batch.column(*column), row)?;
                *column += 1;
                let CellValue::Int64(value) = value else {
                    return Err(unexpected_value(expression, &value));
                };
                *current = current.checked_add(value).ok_or_else(|| {
                    Error::Execution("count overflowed INT64 while merging spill partitions".into())
                })?;
            }
            Self::Avg { sum, count } => {
                let partial_sum = cell(batch.column(*column), row)?;
                let partial_count = cell(batch.column(*column + 1), row)?;
                *column += 2;
                let CellValue::Float64(partial_sum) = partial_sum else {
                    return Err(unexpected_value(expression, &partial_sum));
                };
                let CellValue::UInt64(partial_count) = partial_count else {
                    return Err(unexpected_value(expression, &partial_count));
                };
                *sum += partial_sum;
                *count = count.checked_add(partial_count).ok_or_else(|| {
                    Error::Execution("average count overflow while merging spill partitions".into())
                })?;
            }
            Self::AvgDecimal { sum, count, .. } => {
                let partial_sum = cell(batch.column(*column), row)?;
                let partial_count = cell(batch.column(*column + 1), row)?;
                *column += 2;
                let CellValue::Decimal128(partial_sum) = partial_sum else {
                    return Err(unexpected_value(expression, &partial_sum));
                };
                let CellValue::UInt64(partial_count) = partial_count else {
                    return Err(unexpected_value(expression, &partial_count));
                };
                *sum = sum.checked_add(partial_sum).ok_or_else(|| {
                    Error::Execution(
                        "decimal average overflow while merging spill partitions".into(),
                    )
                })?;
                *count = count.checked_add(partial_count).ok_or_else(|| {
                    Error::Execution(
                        "decimal average count overflow while merging spill partitions".into(),
                    )
                })?;
            }
            Self::SumSigned { value, seen } => {
                let partial = cell(batch.column(*column), row)?;
                let partial_seen = cell(batch.column(*column + 1), row)?;
                *column += 2;
                let CellValue::Int64(partial) = partial else {
                    return Err(unexpected_value(expression, &partial));
                };
                let CellValue::Boolean(partial_seen) = partial_seen else {
                    return Err(unexpected_value(expression, &partial_seen));
                };
                *value = value.checked_add(i128::from(partial)).ok_or_else(|| {
                    Error::Execution("sum overflow while merging spill partitions".into())
                })?;
                *seen |= partial_seen;
            }
            Self::SumUnsigned { value, seen } => {
                let partial = cell(batch.column(*column), row)?;
                let partial_seen = cell(batch.column(*column + 1), row)?;
                *column += 2;
                let CellValue::UInt64(partial) = partial else {
                    return Err(unexpected_value(expression, &partial));
                };
                let CellValue::Boolean(partial_seen) = partial_seen else {
                    return Err(unexpected_value(expression, &partial_seen));
                };
                *value = value.checked_add(u128::from(partial)).ok_or_else(|| {
                    Error::Execution("sum overflow while merging spill partitions".into())
                })?;
                *seen |= partial_seen;
            }
            Self::SumFloat { value, seen } => {
                let partial = cell(batch.column(*column), row)?;
                let partial_seen = cell(batch.column(*column + 1), row)?;
                *column += 2;
                let CellValue::Float64(partial) = partial else {
                    return Err(unexpected_value(expression, &partial));
                };
                let CellValue::Boolean(partial_seen) = partial_seen else {
                    return Err(unexpected_value(expression, &partial_seen));
                };
                *value += partial;
                *seen |= partial_seen;
            }
            Self::SumDecimal { value, seen, .. } => {
                let partial = cell(batch.column(*column), row)?;
                let partial_seen = cell(batch.column(*column + 1), row)?;
                *column += 2;
                let CellValue::Decimal128(partial) = partial else {
                    return Err(unexpected_value(expression, &partial));
                };
                let CellValue::Boolean(partial_seen) = partial_seen else {
                    return Err(unexpected_value(expression, &partial_seen));
                };
                *value = value.checked_add(partial).ok_or_else(|| {
                    Error::Execution("decimal sum overflow while merging spill partitions".into())
                })?;
                *seen |= partial_seen;
            }
            _ => {
                let value = cell(batch.column(*column), row)?;
                *column += 1;
                self.update(expression, Some(value))?;
            }
        }
        Ok(())
    }
}

fn update_extreme(
    current: &mut Option<CellValue>,
    value: Option<CellValue>,
    maximum: bool,
) -> Result<()> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let replace = current
        .as_ref()
        .map(|current| {
            current.compare(&value).map(|ordering| {
                if maximum {
                    ordering.is_lt()
                } else {
                    ordering.is_gt()
                }
            })
        })
        .transpose()?
        .unwrap_or(true);
    if replace {
        *current = Some(value);
    }
    Ok(())
}

fn decimal_value(value: i128, precision: u8, operation: &str) -> Result<CellValue> {
    let limit = 10_i128
        .checked_pow(u32::from(precision))
        .ok_or_else(|| Error::Execution(format!("decimal {operation} precision overflow")))?;
    if value <= -limit || value >= limit {
        return Err(Error::Execution(format!(
            "decimal {operation} exceeds precision {precision}"
        )));
    }
    Ok(CellValue::Decimal128(value))
}

fn unexpected_value(expression: &AggregateExpr, value: &CellValue) -> Error {
    Error::Internal(format!(
        "aggregate {} received incompatible value {value:?}",
        expression.display_name
    ))
}

pub(super) fn estimate_group_bytes(state: &GroupState) -> usize {
    96 + state
        .key
        .iter()
        .map(|value| match value {
            CellValue::Utf8(value) => value.len(),
            CellValue::Binary(value) => value.len(),
            _ => 16,
        })
        .sum::<usize>()
        + state.aggregates.len() * 32
}

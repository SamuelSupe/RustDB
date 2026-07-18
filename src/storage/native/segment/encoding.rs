use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray,
    },
    datatypes::{
        DataType, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, Schema, SchemaRef,
    },
    record_batch::RecordBatch,
};

use crate::{Error, Result};

const WIDTH: i32 = 16;

pub(crate) fn required(schema: &SchemaRef) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Interval(_)))
}

pub(crate) fn physical_schema(logical: &SchemaRef) -> SchemaRef {
    if !required(logical) {
        return Arc::clone(logical);
    }
    Arc::new(Schema::new_with_metadata(
        logical
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Interval(_) => Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::FixedSizeBinary(WIDTH)),
                ),
                _ => Arc::clone(field),
            })
            .collect::<Vec<_>>(),
        logical.metadata().clone(),
    ))
}

pub(crate) fn encode(batch: &RecordBatch, logical: &SchemaRef) -> Result<RecordBatch> {
    if !required(logical) {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .zip(logical.fields())
        .map(|(array, field)| encode_array(array, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(physical_schema(logical), columns)?)
}

pub(crate) fn decode(batch: &RecordBatch, logical: &SchemaRef) -> Result<RecordBatch> {
    if !required(logical) {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .zip(logical.fields())
        .map(|(array, field)| decode_array(array, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(logical), columns)?)
}

fn encode_array(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(array.len(), WIDTH);
    match data_type {
        DataType::Interval(IntervalUnit::YearMonth) => {
            let values = downcast::<IntervalYearMonthArray>(array, data_type)?;
            for row in 0..values.len() {
                append(&mut builder, values, row, |row| {
                    let mut bytes = [0; WIDTH as usize];
                    bytes[..4].copy_from_slice(&values.value(row).to_le_bytes());
                    bytes
                })?;
            }
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let values = downcast::<IntervalDayTimeArray>(array, data_type)?;
            for row in 0..values.len() {
                append(&mut builder, values, row, |row| {
                    let (days, millis) = IntervalDayTimeType::to_parts(values.value(row));
                    let mut bytes = [0; WIDTH as usize];
                    bytes[..4].copy_from_slice(&days.to_le_bytes());
                    bytes[4..8].copy_from_slice(&millis.to_le_bytes());
                    bytes
                })?;
            }
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let values = downcast::<IntervalMonthDayNanoArray>(array, data_type)?;
            for row in 0..values.len() {
                append(&mut builder, values, row, |row| {
                    let (months, days, nanos) =
                        IntervalMonthDayNanoType::to_parts(values.value(row));
                    let mut bytes = [0; WIDTH as usize];
                    bytes[..4].copy_from_slice(&months.to_le_bytes());
                    bytes[4..8].copy_from_slice(&days.to_le_bytes());
                    bytes[8..].copy_from_slice(&nanos.to_le_bytes());
                    bytes
                })?;
            }
        }
        _ => return Ok(Arc::clone(array)),
    }
    Ok(Arc::new(builder.finish()))
}

fn decode_array(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    if !matches!(data_type, DataType::Interval(_)) {
        return Ok(Arc::clone(array));
    }
    let values = downcast::<FixedSizeBinaryArray>(array, &DataType::FixedSizeBinary(WIDTH))?;
    Ok(match data_type {
        DataType::Interval(IntervalUnit::YearMonth) => Arc::new(IntervalYearMonthArray::from(
            decode_values(values, |bytes| {
                i32::from_le_bytes(bytes[..4].try_into().unwrap())
            }),
        )),
        DataType::Interval(IntervalUnit::DayTime) => {
            Arc::new(IntervalDayTimeArray::from(decode_values(values, |bytes| {
                IntervalDayTimeType::make_value(
                    i32::from_le_bytes(bytes[..4].try_into().unwrap()),
                    i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                )
            })))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => Arc::new(
            IntervalMonthDayNanoArray::from(decode_values(values, |bytes| {
                IntervalMonthDayNanoType::make_value(
                    i32::from_le_bytes(bytes[..4].try_into().unwrap()),
                    i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                    i64::from_le_bytes(bytes[8..].try_into().unwrap()),
                )
            })),
        ),
        _ => unreachable!("interval units are exhaustive"),
    })
}

fn append<T: Array>(
    builder: &mut FixedSizeBinaryBuilder,
    array: &T,
    row: usize,
    value: impl FnOnce(usize) -> [u8; WIDTH as usize],
) -> Result<()> {
    if array.is_null(row) {
        builder.append_null();
    } else {
        builder.append_value(value(row))?;
    }
    Ok(())
}

fn decode_values<T>(values: &FixedSizeBinaryArray, decode: impl Fn(&[u8]) -> T) -> Vec<Option<T>> {
    (0..values.len())
        .map(|row| (!values.is_null(row)).then(|| decode(values.value(row))))
        .collect()
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, expected: &DataType) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::Internal(format!(
            "native interval codec expected {expected}, found {}",
            array.data_type()
        ))
    })
}

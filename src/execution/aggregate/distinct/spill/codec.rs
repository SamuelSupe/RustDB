use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{Array, BinaryArray, UInt32Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext},
};

use super::DistinctKey;
use crate::execution::value::CellValue;

pub(super) fn try_build_batch(
    keys: &[DistinctKey],
    schema: SchemaRef,
    context: &QueryContext,
) -> Result<Option<(RecordBatch, MemoryReservation)>> {
    let estimate = keys
        .iter()
        .map(DistinctKey::memory_size)
        .fold(1024usize, usize::saturating_add)
        .saturating_mul(3)
        .max(1);
    let Ok(mut memory) = context.memory.try_reserve(estimate) else {
        return Ok(None);
    };
    let batch = build_batch(keys, schema)?;
    let actual = batch.get_array_memory_size().max(1);
    if memory.try_resize(actual).is_err() {
        return Ok(None);
    }
    Ok(Some((batch, memory)))
}

pub(super) fn spill_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("__distinct_group", DataType::Binary, false),
        Field::new("__distinct_aggregate", DataType::UInt32, false),
        Field::new("__distinct_value", DataType::Binary, false),
    ]))
}

pub(super) fn decode_row(batch: &RecordBatch, row: usize) -> Result<DistinctKey> {
    if batch.num_columns() != 3 {
        return Err(corrupt("expected three columns"));
    }
    let groups = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| corrupt("group column is not Binary"))?;
    let aggregates = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| corrupt("aggregate-id column is not UInt32"))?;
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| corrupt("value column is not Binary"))?;
    if groups.is_null(row) || aggregates.is_null(row) || values.is_null(row) {
        return Err(corrupt("identity columns cannot contain NULL"));
    }
    Ok(DistinctKey {
        group: decode_group(groups.value(row))?,
        aggregate: aggregates.value(row) as usize,
        value: decode_value(values.value(row))?,
    })
}

pub(super) fn decoded_batch_estimate(batch: &RecordBatch) -> Result<usize> {
    (0..batch.num_rows()).try_fold(1usize, |bytes, row| {
        Ok(bytes.saturating_add(decoded_row_estimate(batch, row)?))
    })
}

pub(super) fn decoded_row_estimate(batch: &RecordBatch, row: usize) -> Result<usize> {
    let groups = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| corrupt("group column is not Binary"))?;
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| corrupt("value column is not Binary"))?;
    let group_bytes = groups.value(row);
    let group_count = encoded_group_count(group_bytes)?;
    Ok((group_bytes.len())
        .saturating_add(values.value_length(row).max(0) as usize)
        .saturating_add(group_count.saturating_mul(size_of::<CellValue>()))
        .saturating_add(256))
}

pub(super) fn corrupt(message: &str) -> Error {
    Error::Execution(format!("corrupt DISTINCT spill: {message}"))
}

fn build_batch(keys: &[DistinctKey], schema: SchemaRef) -> Result<RecordBatch> {
    let groups = keys
        .iter()
        .map(|key| encode_group(&key.group))
        .collect::<Result<Vec<_>>>()?;
    let values = keys
        .iter()
        .map(|key| encode_value(&key.value))
        .collect::<Result<Vec<_>>>()?;
    let aggregate_ids = keys
        .iter()
        .map(|key| {
            u32::try_from(key.aggregate).map_err(|_| {
                Error::ResourceExhausted("DISTINCT aggregate id exceeds UINT32_MAX".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let groups = BinaryArray::from_iter_values(groups.iter().map(Vec::as_slice));
    let aggregate_ids = UInt32Array::from(aggregate_ids);
    let values = BinaryArray::from_iter_values(values.iter().map(Vec::as_slice));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(groups), Arc::new(aggregate_ids), Arc::new(values)],
    )?)
}

fn encode_group(values: &[CellValue]) -> Result<Vec<u8>> {
    let count = u32::try_from(values.len())
        .map_err(|_| Error::ResourceExhausted("DISTINCT group has too many columns".into()))?;
    let mut output = Vec::new();
    output.extend_from_slice(&count.to_le_bytes());
    for value in values {
        encode_cell(value, &mut output)?;
    }
    Ok(output)
}

fn decode_group(bytes: &[u8]) -> Result<Vec<CellValue>> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.read_u32()? as usize;
    if count > cursor.remaining() {
        return Err(corrupt("group column count exceeds encoded bytes"));
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decode_cell(&mut cursor)?);
    }
    cursor.finish()?;
    Ok(values)
}

fn encode_value(value: &CellValue) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    encode_cell(value, &mut output)?;
    Ok(output)
}

fn decode_value(bytes: &[u8]) -> Result<CellValue> {
    let mut cursor = Cursor::new(bytes);
    let value = decode_cell(&mut cursor)?;
    cursor.finish()?;
    Ok(value)
}

fn encode_cell(value: &CellValue, output: &mut Vec<u8>) -> Result<()> {
    match value {
        CellValue::Null => output.push(0),
        CellValue::Boolean(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        CellValue::Int64(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        CellValue::UInt64(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_le_bytes());
        }
        CellValue::Float64(value) => {
            output.push(4);
            let bits = if *value == 0.0 {
                0
            } else if value.is_nan() {
                f64::NAN.to_bits()
            } else {
                value.to_bits()
            };
            output.extend_from_slice(&bits.to_le_bytes());
        }
        CellValue::Utf8(value) => {
            output.push(5);
            encode_bytes(value.as_bytes(), output)?;
        }
        CellValue::Binary(value) => {
            output.push(6);
            encode_bytes(value, output)?;
        }
        CellValue::Decimal128(value) => {
            output.push(7);
            output.extend_from_slice(&value.to_le_bytes());
        }
        CellValue::IntervalYearMonth(months) => {
            output.push(8);
            output.extend_from_slice(&months.to_le_bytes());
        }
        CellValue::IntervalDayTime(days, millis) => {
            output.push(9);
            output.extend_from_slice(&days.to_le_bytes());
            output.extend_from_slice(&millis.to_le_bytes());
        }
        CellValue::IntervalMonthDayNano(months, days, nanos) => {
            output.push(10);
            output.extend_from_slice(&months.to_le_bytes());
            output.extend_from_slice(&days.to_le_bytes());
            output.extend_from_slice(&nanos.to_le_bytes());
        }
    }
    Ok(())
}

fn decode_cell(cursor: &mut Cursor<'_>) -> Result<CellValue> {
    Ok(match cursor.read_u8()? {
        0 => CellValue::Null,
        1 => match cursor.read_u8()? {
            0 => CellValue::Boolean(false),
            1 => CellValue::Boolean(true),
            _ => return Err(corrupt("invalid boolean value")),
        },
        2 => CellValue::Int64(i64::from_le_bytes(cursor.read_array()?)),
        3 => CellValue::UInt64(u64::from_le_bytes(cursor.read_array()?)),
        4 => CellValue::Float64(f64::from_bits(u64::from_le_bytes(cursor.read_array()?))),
        5 => CellValue::Utf8(
            String::from_utf8(cursor.read_bytes()?.to_vec())
                .map_err(|_| corrupt("invalid UTF-8 value"))?,
        ),
        6 => CellValue::Binary(cursor.read_bytes()?.to_vec()),
        7 => CellValue::Decimal128(i128::from_le_bytes(cursor.read_array()?)),
        8 => CellValue::IntervalYearMonth(i32::from_le_bytes(cursor.read_array()?)),
        9 => CellValue::IntervalDayTime(
            i32::from_le_bytes(cursor.read_array()?),
            i32::from_le_bytes(cursor.read_array()?),
        ),
        10 => CellValue::IntervalMonthDayNano(
            i32::from_le_bytes(cursor.read_array()?),
            i32::from_le_bytes(cursor.read_array()?),
            i64::from_le_bytes(cursor.read_array()?),
        ),
        tag => return Err(corrupt(&format!("unknown value tag {tag}"))),
    })
}

fn encode_bytes(bytes: &[u8], output: &mut Vec<u8>) -> Result<()> {
    let len = u64::try_from(bytes.len())
        .map_err(|_| Error::ResourceExhausted("DISTINCT value is too large".into()))?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.read_exact(N)?
            .try_into()
            .map_err(|_| corrupt("truncated fixed-width value"))
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = u64::from_le_bytes(self.read_array()?);
        let len = usize::try_from(len).map_err(|_| corrupt("value length exceeds usize"))?;
        self.read_exact(len)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| corrupt("value length overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| corrupt("truncated value"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn finish(self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(corrupt("trailing bytes"))
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }
}

fn encoded_group_count(bytes: &[u8]) -> Result<usize> {
    let count = bytes
        .get(..size_of::<u32>())
        .ok_or_else(|| corrupt("truncated group column count"))?;
    let count: [u8; 4] = count
        .try_into()
        .map_err(|_| corrupt("invalid group column count"))?;
    Ok(u32::from_le_bytes(count) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_encoding_round_trips_all_supported_cells() {
        let group = vec![
            CellValue::Null,
            CellValue::Boolean(true),
            CellValue::Int64(-12),
            CellValue::UInt64(14),
            CellValue::Float64(-0.0),
            CellValue::Utf8("hello".into()),
            CellValue::Binary(vec![0, 1, 255]),
            CellValue::Decimal128(-123456789),
        ];
        let encoded = encode_group(&group).unwrap();
        let decoded = decode_group(&encoded).unwrap();
        assert_eq!(decoded, group);
    }
}

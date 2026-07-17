use std::mem::size_of;

use arrow::{
    array::{Array, ArrayRef, BinaryArray, Decimal128Array, DictionaryArray, StringArray},
    datatypes::{DataType, UInt32Type, i256},
};

use crate::{
    Error, Result,
    sql::{AggregateExpr, AggregateFunction, ExprKind},
};

use super::{
    super::value::CellValue,
    key::{EncodedGroupRows, GroupKey},
    state::{AggregateState, GroupState},
};

const MAX_DENSE_SLOTS: usize = 16;
const ALLOCATION_SLACK: usize = 32;

// The slot arrays stay inline deliberately: this path is bounded to sixteen
// slots and avoids one heap allocation for every aggregate in the hot loop.
#[allow(clippy::large_enum_variant)]
enum DensePartial {
    Count([i64; MAX_DENSE_SLOTS]),
    DecimalSum([DecimalPartial; MAX_DENSE_SLOTS]),
}

pub(super) struct DecimalPartial {
    total: i128,
    min_prefix: i128,
    max_prefix: i128,
    seen: bool,
    wide: Option<Box<WideDecimalPartial>>,
}

struct WideDecimalPartial {
    total: i256,
    min_prefix: i256,
    max_prefix: i256,
}

impl DecimalPartial {
    const EMPTY: Self = Self {
        total: 0,
        min_prefix: 0,
        max_prefix: 0,
        seen: false,
        wide: None,
    };

    fn push(&mut self, value: i128) -> Result<()> {
        if let Some(wide) = &mut self.wide {
            wide.push(value)?;
            self.seen = true;
            return Ok(());
        }
        let Some(next) = self.total.checked_add(value) else {
            let next = i256::from_i128(self.total)
                .checked_add(i256::from_i128(value))
                .ok_or_else(decimal_i256_overflow)?;
            self.wide = Some(Box::new(WideDecimalPartial {
                total: next,
                min_prefix: i256::from_i128(self.min_prefix).min(next),
                max_prefix: i256::from_i128(self.max_prefix).max(next),
            }));
            self.seen = true;
            return Ok(());
        };
        self.total = next;
        self.min_prefix = self.min_prefix.min(next);
        self.max_prefix = self.max_prefix.max(next);
        self.seen = true;
        Ok(())
    }

    pub(super) fn apply_to(&self, current: i128) -> Result<Option<i128>> {
        if !self.seen {
            return Ok(None);
        }
        if let Some(wide) = &self.wide {
            return wide.apply_to(current).map(Some);
        }
        current
            .checked_add(self.min_prefix)
            .ok_or_else(decimal_i128_overflow)?;
        current
            .checked_add(self.max_prefix)
            .ok_or_else(decimal_i128_overflow)?;
        current
            .checked_add(self.total)
            .map(Some)
            .ok_or_else(decimal_i128_overflow)
    }

    #[cfg(test)]
    fn is_wide(&self) -> bool {
        self.wide.is_some()
    }
}

impl WideDecimalPartial {
    fn push(&mut self, value: i128) -> Result<()> {
        self.total = self
            .total
            .checked_add(i256::from_i128(value))
            .ok_or_else(decimal_i256_overflow)?;
        self.min_prefix = self.min_prefix.min(self.total);
        self.max_prefix = self.max_prefix.max(self.total);
        Ok(())
    }

    fn apply_to(&self, current: i128) -> Result<i128> {
        let current = i256::from_i128(current);
        let min = current.checked_add(self.min_prefix);
        let max = current.checked_add(self.max_prefix);
        if min.is_none_or(|value| value < i256::from_i128(i128::MIN))
            || max.is_none_or(|value| value > i256::from_i128(i128::MAX))
        {
            return Err(decimal_i128_overflow());
        }
        current
            .checked_add(self.total)
            .and_then(i256::to_i128)
            .ok_or_else(decimal_i128_overflow)
    }
}

fn decimal_i128_overflow() -> Error {
    Error::Execution("decimal sum overflowed i128".into())
}

fn decimal_i256_overflow() -> Error {
    Error::Execution("decimal sum overflowed i256".into())
}

pub(super) fn workspace_estimate(aggregate_count: usize) -> usize {
    let per_aggregate = size_of::<DensePartial>()
        .saturating_add(size_of::<Option<&Decimal128Array>>())
        .saturating_add(
            MAX_DENSE_SLOTS.saturating_mul(
                size_of::<WideDecimalPartial>()
                    .saturating_add(size_of::<AggregateState>())
                    .saturating_add(ALLOCATION_SLACK),
            ),
        );
    let per_slot = size_of::<(GroupKey, GroupState)>()
        .saturating_add(size_of::<Option<usize>>().saturating_mul(2))
        .saturating_add(ALLOCATION_SLACK);

    size_of::<DenseDictionaryBatch>()
        .saturating_add(aggregate_count.saturating_mul(per_aggregate))
        .saturating_add(MAX_DENSE_SLOTS.saturating_mul(per_slot))
        .max(1)
}

pub(super) struct DenseDictionaryBatch {
    partials: Vec<DensePartial>,
    representatives: [Option<usize>; MAX_DENSE_SLOTS],
    slots: usize,
}

impl DenseDictionaryBatch {
    pub(super) fn try_new(
        groups: &EncodedGroupRows,
        group_arrays: &[ArrayRef],
        expressions: &[AggregateExpr],
        arrays: &[Option<ArrayRef>],
        rows: usize,
    ) -> Result<Option<Self>> {
        if expressions.is_empty() || !supported_group_arrays(group_arrays) {
            return Ok(None);
        }
        if expressions.len() != arrays.len() {
            return Err(Error::Internal(format!(
                "dense dictionary aggregate expected {} input arrays, received {}",
                expressions.len(),
                arrays.len()
            )));
        }
        let Some((group_rows, slots)) = groups.dense_dictionary_shape() else {
            return Ok(None);
        };
        if group_rows != rows {
            return Err(Error::Internal(format!(
                "dense dictionary aggregate expected {rows} group rows, received {group_rows}"
            )));
        }
        if !(1..=MAX_DENSE_SLOTS).contains(&slots) {
            return Ok(None);
        }

        let mut partials = Vec::with_capacity(expressions.len());
        let mut decimal_arrays = Vec::with_capacity(expressions.len());
        for (expression, array) in expressions.iter().zip(arrays) {
            if expression.distinct {
                return Ok(None);
            }
            match (&expression.function, &expression.expr, array) {
                (AggregateFunction::Count, None, None)
                    if expression.data_type == DataType::Int64 =>
                {
                    partials.push(DensePartial::Count([0; MAX_DENSE_SLOTS]));
                    decimal_arrays.push(None);
                }
                (AggregateFunction::Sum, Some(input), Some(array))
                    if matches!(input.kind, ExprKind::Column(_))
                        && matches!(input.data_type, DataType::Decimal128(_, _))
                        && matches!(expression.data_type, DataType::Decimal128(_, _))
                        && array.data_type() == &input.data_type =>
                {
                    let Some(array) = array.as_any().downcast_ref::<Decimal128Array>() else {
                        return Err(Error::Internal(
                            "dense dictionary decimal SUM array type mismatch".into(),
                        ));
                    };
                    if array.len() != rows {
                        return Err(Error::Internal(format!(
                            "dense dictionary decimal SUM expected {rows} rows, received {}",
                            array.len()
                        )));
                    }
                    partials.push(DensePartial::DecimalSum(std::array::from_fn(|_| {
                        DecimalPartial::EMPTY
                    })));
                    decimal_arrays.push(Some(array));
                }
                _ => return Ok(None),
            }
        }

        let mut representatives = [None; MAX_DENSE_SLOTS];
        for row in 0..rows {
            let slot = groups.dense_dictionary_slot(row).ok_or_else(|| {
                Error::Internal("dense dictionary group rows lost their slot mapping".into())
            })?;
            if slot >= slots {
                return Err(Error::Internal(format!(
                    "dense dictionary group slot {slot} exceeded slot count {slots}"
                )));
            }
            representatives[slot].get_or_insert(row);
            for (partial, decimal) in partials.iter_mut().zip(&decimal_arrays) {
                match (partial, decimal) {
                    (DensePartial::Count(counts), None) => {
                        counts[slot] = counts[slot]
                            .checked_add(1)
                            .ok_or_else(|| Error::Execution("count overflowed INT64".into()))?;
                    }
                    (DensePartial::DecimalSum(values), Some(array)) if array.is_valid(row) => {
                        values[slot].push(array.value(row))?;
                    }
                    (DensePartial::DecimalSum(_), Some(_)) => {}
                    _ => {
                        return Err(Error::Internal(
                            "dense dictionary aggregate partial lost its typed input".into(),
                        ));
                    }
                }
            }
        }

        Ok(Some(Self {
            partials,
            representatives,
            slots,
        }))
    }

    pub(super) fn representatives(&self) -> &[Option<usize>] {
        &self.representatives[..self.slots]
    }

    pub(super) fn dynamic_workspace_estimate(
        &self,
        groups: &EncodedGroupRows,
        arrays: &[ArrayRef],
    ) -> Result<usize> {
        let mut bytes = 0usize;
        for row in self.representatives().iter().flatten() {
            bytes = bytes
                .saturating_add(arrays.len().saturating_mul(size_of::<CellValue>()))
                .saturating_add(ALLOCATION_SLACK);
            let key_bytes = groups.borrowed_key(*row).ok_or_else(|| {
                Error::Internal("dense dictionary group lost its encoded key".into())
            })?;
            bytes = bytes.saturating_add(owned_payload_bytes(key_bytes.len()));
            for array in arrays {
                bytes =
                    bytes.saturating_add(owned_payload_bytes(dictionary_payload_len(array, *row)?));
            }
        }
        Ok(bytes)
    }

    #[cfg(test)]
    fn has_wide_decimal(&self) -> bool {
        self.partials.iter().any(|partial| {
            matches!(partial, DensePartial::DecimalSum(values) if values.iter().any(|value| value.is_wide()))
        })
    }

    pub(super) fn apply(
        &self,
        state_by_slot: &[Option<usize>],
        states: &mut [GroupState],
    ) -> Result<()> {
        if state_by_slot.len() != self.slots {
            return Err(Error::Internal(format!(
                "dense dictionary aggregate expected {} state slots, received {}",
                self.slots,
                state_by_slot.len()
            )));
        }
        for (slot, representative) in self.representatives().iter().enumerate() {
            if representative.is_none() {
                continue;
            }
            let state_index = state_by_slot[slot].ok_or_else(|| {
                Error::Internal(format!(
                    "dense dictionary group slot {slot} has no resolved aggregate state"
                ))
            })?;
            let state = states.get_mut(state_index).ok_or_else(|| {
                Error::Internal(format!(
                    "dense dictionary aggregate state {state_index} is out of bounds"
                ))
            })?;
            if state.aggregates.len() != self.partials.len() {
                return Err(Error::Internal(
                    "dense dictionary aggregate state width changed after resolution".into(),
                ));
            }
            for (partial, state) in self.partials.iter().zip(&mut state.aggregates) {
                match partial {
                    DensePartial::Count(counts) => {
                        state.update_dense_count_star(counts[slot])?;
                    }
                    DensePartial::DecimalSum(values) => {
                        state.update_dense_decimal_sum(&values[slot])?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn dictionary_payload_len(array: &ArrayRef, row: usize) -> Result<usize> {
    let dictionary = array
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
        .ok_or_else(|| Error::Internal("dense dictionary payload type mismatch".into()))?;
    let Some(index) = dictionary.key(row) else {
        return Ok(0);
    };
    let values = dictionary.values();
    if values.is_null(index) {
        return Ok(0);
    }
    match values.data_type() {
        DataType::Utf8 => Ok(values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::Internal("dense Utf8 dictionary value mismatch".into()))?
            .value(index)
            .len()),
        DataType::Binary => Ok(values
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| Error::Internal("dense Binary dictionary value mismatch".into()))?
            .value(index)
            .len()),
        other => Err(Error::Internal(format!(
            "dense dictionary payload does not support {other}"
        ))),
    }
}

fn owned_payload_bytes(bytes: usize) -> usize {
    if bytes == 0 {
        0
    } else {
        bytes.saturating_add(ALLOCATION_SLACK)
    }
}

fn supported_group_arrays(arrays: &[ArrayRef]) -> bool {
    (1..=2).contains(&arrays.len())
        && arrays.iter().all(|array| {
            matches!(
                array.data_type(),
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::UInt32
                        && matches!(value.as_ref(), DataType::Utf8 | DataType::Binary)
            )
        })
}

#[cfg(test)]
#[path = "dense_dictionary_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "dense_dictionary_stream_tests.rs"]
mod stream_tests;

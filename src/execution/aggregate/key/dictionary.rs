use std::mem::size_of;

use arrow::{
    array::{Array, ArrayRef, DictionaryArray, UInt32Array},
    compute::take,
    datatypes::{DataType, UInt32Type},
    row::{RowConverter, Rows},
};

use crate::{Error, Result};

const MAX_CARTESIAN_KEYS: usize = 16;

pub(in crate::execution::aggregate) struct DictionaryGroupRows {
    rows: Rows,
    keys: DictionaryKeys,
    widths: [usize; 2],
    raw_to_canonical: [usize; MAX_CARTESIAN_KEYS],
    canonical_to_raw: [usize; MAX_CARTESIAN_KEYS],
    slot_count: usize,
}

impl DictionaryGroupRows {
    pub(super) fn key(&self, row: usize) -> &[u8] {
        self.rows.row(self.canonical_to_raw[self.slot(row)]).data()
    }

    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(super) fn slot(&self, row: usize) -> usize {
        self.raw_to_canonical[self.keys.slot(row, self.widths)]
    }

    pub(super) fn slot_count(&self) -> usize {
        self.slot_count
    }

    pub(super) fn memory_size(&self) -> usize {
        self.rows.size().saturating_add(size_of::<Self>())
    }
}

enum DictionaryKeys {
    One(UInt32Array),
    Two(UInt32Array, UInt32Array),
}

impl DictionaryKeys {
    fn len(&self) -> usize {
        match self {
            Self::One(keys) | Self::Two(keys, _) => keys.len(),
        }
    }

    fn slot(&self, row: usize, widths: [usize; 2]) -> usize {
        match self {
            Self::One(keys) => raw_slot(keys, row, widths[0]),
            Self::Two(left, right) => raw_slot(left, row, widths[0])
                .saturating_mul(widths[1])
                .saturating_add(raw_slot(right, row, widths[1])),
        }
    }
}

pub(super) fn try_encode(
    logical_types: &[DataType],
    converter: &RowConverter,
    arrays: &[ArrayRef],
) -> Result<Option<DictionaryGroupRows>> {
    if !(1..=2).contains(&arrays.len()) || logical_types.len() != arrays.len() {
        return Ok(None);
    }

    let mut dictionaries = Vec::with_capacity(arrays.len());
    let mut widths = [1, 1];
    let mut product = 1usize;
    for (column, (logical, array)) in logical_types.iter().zip(arrays).enumerate() {
        if !eligible(logical, array.data_type()) {
            return Ok(None);
        }
        let dictionary = array
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
            .ok_or_else(|| Error::Internal("aggregate UInt32 dictionary type mismatch".into()))?;
        if dictionaries
            .first()
            .is_some_and(|first: &&DictionaryArray<UInt32Type>| first.len() != dictionary.len())
        {
            return Err(Error::Internal(
                "aggregate dictionary group arrays have different lengths".into(),
            ));
        }
        let Some(width) = dictionary.values().len().checked_add(1) else {
            return Ok(None);
        };
        let Some(next_product) = product.checked_mul(width) else {
            return Ok(None);
        };
        if next_product > MAX_CARTESIAN_KEYS {
            return Ok(None);
        }
        widths[column] = width;
        product = next_product;
        dictionaries.push(dictionary);
    }

    let logical_values = dictionaries
        .iter()
        .enumerate()
        .map(|(column, dictionary)| cartesian_values(dictionary, widths, column, product))
        .collect::<Result<Vec<_>>>()?;
    let rows = converter.convert_columns(&logical_values)?;
    let (raw_to_canonical, canonical_to_raw, slot_count) = canonicalize_slots(&rows, product);
    let keys = match dictionaries.as_slice() {
        [one] => DictionaryKeys::One(one.keys().clone()),
        [left, right] => DictionaryKeys::Two(left.keys().clone(), right.keys().clone()),
        _ => unreachable!("one or two dictionaries were required"),
    };
    Ok(Some(DictionaryGroupRows {
        rows,
        keys,
        widths,
        raw_to_canonical,
        canonical_to_raw,
        slot_count,
    }))
}

fn canonicalize_slots(
    rows: &Rows,
    raw_slots: usize,
) -> (
    [usize; MAX_CARTESIAN_KEYS],
    [usize; MAX_CARTESIAN_KEYS],
    usize,
) {
    let mut raw_to_canonical = [0; MAX_CARTESIAN_KEYS];
    let mut canonical_to_raw = [0; MAX_CARTESIAN_KEYS];
    let mut canonical_slots = 0;

    for (raw_slot, canonical_slot) in raw_to_canonical.iter_mut().enumerate().take(raw_slots) {
        let key = rows.row(raw_slot).data();
        let existing =
            (0..canonical_slots).find(|slot| rows.row(canonical_to_raw[*slot]).data() == key);
        *canonical_slot = existing.unwrap_or_else(|| {
            let slot = canonical_slots;
            canonical_to_raw[slot] = raw_slot;
            canonical_slots += 1;
            slot
        });
    }

    (raw_to_canonical, canonical_to_raw, canonical_slots)
}

fn eligible(logical: &DataType, physical: &DataType) -> bool {
    let DataType::Dictionary(key, value) = physical else {
        return false;
    };
    key.as_ref() == &DataType::UInt32
        && logical == value.as_ref()
        && matches!(
            logical,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
        )
}

fn cartesian_values(
    dictionary: &DictionaryArray<UInt32Type>,
    widths: [usize; 2],
    column: usize,
    product: usize,
) -> Result<ArrayRef> {
    let stride = if column == 0 { widths[1] } else { 1 };
    let indices = (0..product)
        .map(|row| {
            let slot = row / stride % widths[column];
            (slot < dictionary.values().len())
                .then(|| u32::try_from(slot).expect("cartesian dictionary slot fits UInt32"))
        })
        .collect::<Vec<_>>();
    Ok(take(
        dictionary.values().as_ref(),
        &UInt32Array::from(indices),
        None,
    )?)
}

fn raw_slot(keys: &UInt32Array, row: usize, width: usize) -> usize {
    if keys.is_null(row) {
        width - 1
    } else {
        usize::try_from(keys.value(row)).expect("UInt32 dictionary key fits usize")
    }
}

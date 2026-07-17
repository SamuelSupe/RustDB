use std::mem::size_of;

use ahash::RandomState;
use arrow::{
    array::{Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray},
    datatypes::DataType,
};

use crate::{Error, Result, runtime::MemoryReservation};

mod arena;

use arena::Entries;

pub(super) struct Table {
    hasher: RandomState,
    entries: Entries,
}

pub(in crate::execution::join) struct Probe {
    arrays: Arrays,
    _memory: MemoryReservation,
}

impl Table {
    pub(super) fn new() -> Self {
        Self {
            hasher: RandomState::new(),
            entries: Entries::new(),
        }
    }

    pub(super) fn try_insert(
        &mut self,
        arrays: &[ArrayRef],
        reservation: &mut MemoryReservation,
    ) -> Result<bool> {
        let arrays = Arrays::try_new(arrays)?;
        for row in 0..arrays.len() {
            let Some((left, right)) = arrays.value(row)? else {
                continue;
            };
            let hash = self.hasher.hash_one((left, right));
            if !self
                .entries
                .try_increment_hashed(hash, left, right, reservation)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn count(&self, probe: &Probe, row: usize) -> Result<u64> {
        let Some((left, right)) = probe.arrays.value(row)? else {
            return Ok(0);
        };
        let hash = self.hasher.hash_one((left, right));
        Ok(self.entries.count_hashed(hash, left, right))
    }
}

impl Probe {
    pub(super) fn try_new(arrays: &[ArrayRef], memory: MemoryReservation) -> Result<Self> {
        Ok(Self {
            arrays: Arrays::try_new(arrays)?,
            _memory: memory,
        })
    }
}

pub(super) fn supports(types: &[DataType]) -> bool {
    types.len() == 2 && types.iter().all(is_bytes)
}

pub(super) fn probe_memory_bytes() -> usize {
    size_of::<Probe>().max(1)
}

struct Arrays {
    left: Values,
    right: Values,
    len: usize,
}

impl Arrays {
    fn try_new(arrays: &[ArrayRef]) -> Result<Self> {
        let [left, right] = arrays else {
            return Err(Error::Internal(
                "byte-pair Join multiplicity requires exactly two arrays".into(),
            ));
        };
        if left.len() != right.len() {
            return Err(Error::Internal(
                "byte-pair Join multiplicity arrays have different row counts".into(),
            ));
        }
        Ok(Self {
            left: Values::try_new(left)?,
            right: Values::try_new(right)?,
            len: left.len(),
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    fn value(&self, row: usize) -> Result<Option<(&[u8], &[u8])>> {
        if row >= self.len {
            return Err(Error::Internal(
                "byte-pair Join multiplicity row is out of bounds".into(),
            ));
        }
        Ok(match (self.left.value(row), self.right.value(row)) {
            (Some(left), Some(right)) => Some((left, right)),
            _ => None,
        })
    }
}

enum Values {
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
    Binary(BinaryArray),
    LargeBinary(LargeBinaryArray),
}

impl Values {
    fn try_new(array: &ArrayRef) -> Result<Self> {
        match array.data_type() {
            DataType::Utf8 => downcast::<StringArray>(array, "Utf8").map(Self::Utf8),
            DataType::LargeUtf8 => {
                downcast::<LargeStringArray>(array, "LargeUtf8").map(Self::LargeUtf8)
            }
            DataType::Binary => downcast::<BinaryArray>(array, "Binary").map(Self::Binary),
            DataType::LargeBinary => {
                downcast::<LargeBinaryArray>(array, "LargeBinary").map(Self::LargeBinary)
            }
            other => Err(Error::Internal(format!(
                "unsupported byte-pair Join multiplicity type {other}"
            ))),
        }
    }

    fn value(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Utf8(array) => array.is_valid(row).then(|| array.value(row).as_bytes()),
            Self::LargeUtf8(array) => array.is_valid(row).then(|| array.value(row).as_bytes()),
            Self::Binary(array) => array.is_valid(row).then(|| array.value(row)),
            Self::LargeBinary(array) => array.is_valid(row).then(|| array.value(row)),
        }
    }
}

fn downcast<A>(array: &ArrayRef, name: &str) -> Result<A>
where
    A: Array + Clone + 'static,
{
    array
        .as_any()
        .downcast_ref::<A>()
        .cloned()
        .ok_or_else(|| Error::Internal(format!("byte-pair Join {name} array type mismatch")))
}

fn is_bytes(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

#[cfg(test)]
#[path = "byte_pair/tests.rs"]
mod tests;

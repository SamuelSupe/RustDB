use arrow::array::{Array, ArrayRef, Int64Array, UInt64Array};

use crate::{Error, Result};

use super::{FixedHashTable, JoinHashTable};

/// A batch-bound view of a fixed-width Join table. Downcasting happens once
/// when the batch enters the kernel instead of once for every row.
pub(in crate::execution::join) enum FixedProbe<'a> {
    Int64 {
        keys: &'a Int64Array,
        table: &'a FixedHashTable<i64>,
    },
    UInt64 {
        keys: &'a UInt64Array,
        table: &'a FixedHashTable<u64>,
    },
}

impl FixedProbe<'_> {
    pub(in crate::execution::join) fn lookup(&self, row: usize) -> Option<&[u32]> {
        match self {
            Self::Int64 { keys, table } => {
                table.lookup((!keys.is_null(row)).then(|| keys.value(row)), false)
            }
            Self::UInt64 { keys, table } => {
                table.lookup((!keys.is_null(row)).then(|| keys.value(row)), false)
            }
        }
    }
}

pub(super) fn bind<'a>(
    table: &'a JoinHashTable,
    key_arrays: &'a [ArrayRef],
) -> Result<Option<FixedProbe<'a>>> {
    let key = match key_arrays {
        [key] => key,
        _ => return Ok(None),
    };
    Ok(match table {
        JoinHashTable::Int64(table) => Some(FixedProbe::Int64 {
            keys: downcast(key, "Int64")?,
            table,
        }),
        JoinHashTable::UInt64(table) => Some(FixedProbe::UInt64 {
            keys: downcast(key, "UInt64")?,
            table,
        }),
        JoinHashTable::Utf8(_)
        | JoinHashTable::Binary(_)
        | JoinHashTable::Composite(_)
        | JoinHashTable::Generic(_) => None,
    })
}

fn downcast<'a, A: 'static>(array: &'a ArrayRef, expected: &str) -> Result<&'a A> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        Error::Internal(format!(
            "fixed Join probe expected {expected}, found {}",
            array.data_type()
        ))
    })
}

#[cfg(test)]
#[path = "probe/tests.rs"]
mod tests;

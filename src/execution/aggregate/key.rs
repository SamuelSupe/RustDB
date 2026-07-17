use arrow::{
    array::ArrayRef,
    datatypes::DataType,
    row::{RowConverter, Rows, SortField},
};
use std::mem::size_of;

use crate::{Error, Result, sql::BoundExpr};

use super::{CellValue, cell};

mod cache;
mod dictionary;
mod index;

pub(super) use cache::EncodedGroupIdCache;
pub(super) use index::{GroupIndex, ReleaseGroupIndex};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum GroupKey {
    Encoded(Vec<u8>),
    Cells(Vec<CellValue>),
}

impl GroupKey {
    pub(super) fn memory_size(&self) -> usize {
        match self {
            Self::Encoded(bytes) => Self::encoded_memory_size(bytes.capacity()),
            Self::Cells(values) => Self::cells_memory_size(values, values.capacity()),
        }
    }

    pub(super) fn encoded_memory_size(capacity: usize) -> usize {
        capacity.saturating_add(64)
    }

    pub(super) fn cells_memory_size(values: &[CellValue], capacity: usize) -> usize {
        capacity
            .saturating_mul(size_of::<CellValue>())
            .saturating_add(values.iter().fold(0usize, |bytes, value| {
                bytes.saturating_add(match value {
                    CellValue::Utf8(value) => value.capacity(),
                    CellValue::Binary(value) => value.capacity(),
                    _ => 0,
                })
            }))
            .saturating_add(64)
    }
}

pub(super) enum GroupKeyEncoder {
    Empty,
    Rows {
        logical_types: Vec<DataType>,
        converter: RowConverter,
    },
    Cells,
}

pub(super) enum EncodedGroupRows {
    Empty,
    Rows(Rows),
    Dictionary(Box<dictionary::DictionaryGroupRows>),
    Cells,
}

impl EncodedGroupRows {
    pub(super) fn memory_size(&self) -> usize {
        match self {
            Self::Rows(rows) => rows.size(),
            Self::Dictionary(rows) => rows.memory_size(),
            Self::Empty | Self::Cells => 0,
        }
    }

    pub(super) fn borrowed_key(&self, row: usize) -> Option<&[u8]> {
        match self {
            Self::Rows(rows) => Some(rows.row(row).data()),
            Self::Dictionary(rows) => Some(rows.key(row)),
            Self::Empty | Self::Cells => None,
        }
    }

    pub(super) fn dense_dictionary_shape(&self) -> Option<(usize, usize)> {
        match self {
            Self::Dictionary(rows) => Some((rows.len(), rows.slot_count())),
            Self::Empty | Self::Rows(_) | Self::Cells => None,
        }
    }

    pub(super) fn dense_dictionary_slot(&self, row: usize) -> Option<usize> {
        match self {
            Self::Dictionary(rows) => Some(rows.slot(row)),
            Self::Empty | Self::Rows(_) | Self::Cells => None,
        }
    }
}

impl GroupKeyEncoder {
    pub(super) fn new(groups: &[BoundExpr]) -> Self {
        if groups.is_empty() {
            return Self::Empty;
        }
        // Arrow rows intentionally total-order signed zero and NaN payloads,
        // while SQL grouping uses CellValue's normalized float equality.
        if groups.iter().any(|group| {
            matches!(
                group.data_type,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            )
        }) {
            return Self::Cells;
        }
        let logical_types = groups
            .iter()
            .map(|group| group.data_type.clone())
            .collect::<Vec<_>>();
        let fields = logical_types.iter().cloned().map(SortField::new).collect();
        RowConverter::new(fields).map_or(Self::Cells, |converter| Self::Rows {
            logical_types,
            converter,
        })
    }

    pub(super) fn encode(&self, arrays: &[ArrayRef]) -> Result<EncodedGroupRows> {
        match self {
            Self::Empty => Ok(EncodedGroupRows::Empty),
            Self::Rows {
                logical_types,
                converter,
            } => {
                if let Some(rows) = dictionary::try_encode(logical_types, converter, arrays)? {
                    return Ok(EncodedGroupRows::Dictionary(Box::new(rows)));
                }
                let runtime = runtime_converter(logical_types, arrays)?;
                runtime
                    .as_ref()
                    .unwrap_or(converter)
                    .convert_columns(arrays)
                    .map(EncodedGroupRows::Rows)
                    .map_err(Into::into)
            }
            Self::Cells => Ok(EncodedGroupRows::Cells),
        }
    }

    pub(super) fn key(
        &self,
        encoded: &EncodedGroupRows,
        arrays: &[ArrayRef],
        row: usize,
    ) -> Result<GroupKey> {
        match (self, encoded) {
            (Self::Empty, EncodedGroupRows::Empty) => Ok(GroupKey::Encoded(Vec::new())),
            (Self::Rows { .. }, EncodedGroupRows::Rows(rows)) => {
                Ok(GroupKey::Encoded(rows.row(row).data().to_vec()))
            }
            (Self::Rows { .. }, EncodedGroupRows::Dictionary(rows)) => {
                Ok(GroupKey::Encoded(rows.key(row).to_vec()))
            }
            (Self::Cells, EncodedGroupRows::Cells) => arrays
                .iter()
                .map(|array| cell(array, row))
                .collect::<Result<Vec<_>>>()
                .map(GroupKey::Cells),
            _ => unreachable!("encoded group rows must match their encoder"),
        }
    }
}

/// Arrow row encoding hydrates dictionary keys to their values. Constructing
/// the converter from each batch's physical dictionary types therefore keeps
/// persistent row bytes stable even when files use different key widths or
/// dictionary orderings.
fn runtime_converter(
    logical_types: &[DataType],
    arrays: &[ArrayRef],
) -> Result<Option<RowConverter>> {
    if logical_types.len() != arrays.len() {
        return Err(Error::Internal(format!(
            "aggregate group encoder expected {} arrays, received {}",
            logical_types.len(),
            arrays.len()
        )));
    }
    if logical_types
        .iter()
        .zip(arrays)
        .all(|(logical, array)| logical == array.data_type())
    {
        return Ok(None);
    }

    let fields = logical_types
        .iter()
        .zip(arrays)
        .map(|(logical, array)| {
            let physical = array.data_type();
            if dictionary_matches(logical, physical) {
                Ok(SortField::new(physical.clone()))
            } else if logical == physical {
                Ok(SortField::new(logical.clone()))
            } else {
                Err(Error::Internal(format!(
                    "aggregate group array type {physical} is incompatible with logical type {logical}"
                )))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RowConverter::new(fields).map(Some).map_err(Into::into)
}

fn dictionary_matches(logical: &DataType, physical: &DataType) -> bool {
    let DataType::Dictionary(key, value) = physical else {
        return false;
    };
    matches!(
        key.as_ref(),
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    ) && matches!(
        logical,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    ) && logical == value.as_ref()
}

#[cfg(test)]
#[path = "key/dictionary_tests.rs"]
mod dictionary_tests;

#[cfg(test)]
#[path = "key/dictionary_uint32_tests.rs"]
mod dictionary_uint32_tests;

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Float64Array, Int64Array},
        datatypes::DataType,
    };

    use super::{GroupKey, GroupKeyEncoder};
    use crate::sql::BoundExpr;

    #[test]
    fn integer_groups_use_arrow_row_encoding() {
        let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Int64, "key")]);
        let arrays: Vec<ArrayRef> = vec![std::sync::Arc::new(Int64Array::from(vec![1, 2]))];
        let rows = encoder.encode(&arrays).unwrap();
        assert!(rows.borrowed_key(0).is_some());
        assert!(matches!(
            encoder.key(&rows, &arrays, 0).unwrap(),
            GroupKey::Encoded(_)
        ));
        assert_ne!(
            encoder.key(&rows, &arrays, 0).unwrap(),
            encoder.key(&rows, &arrays, 1).unwrap()
        );
    }

    #[test]
    fn float_groups_keep_normalized_sql_equality() {
        let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Float64, "key")]);
        let arrays: Vec<ArrayRef> = vec![std::sync::Arc::new(Float64Array::from(vec![-0.0, 0.0]))];
        let rows = encoder.encode(&arrays).unwrap();
        assert!(rows.borrowed_key(0).is_none());
        assert_eq!(
            encoder.key(&rows, &arrays, 0).unwrap(),
            encoder.key(&rows, &arrays, 1).unwrap()
        );
    }

    #[test]
    fn empty_groups_keep_the_owned_fallback() {
        let encoder = GroupKeyEncoder::new(&[]);
        let rows = encoder.encode(&[]).unwrap();

        assert!(rows.borrowed_key(0).is_none());
        assert_eq!(
            encoder.key(&rows, &[], 0).unwrap(),
            GroupKey::Encoded(Vec::new())
        );
    }
}

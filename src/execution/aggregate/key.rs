use arrow::{
    array::ArrayRef,
    datatypes::DataType,
    row::{RowConverter, Rows, SortField},
};
use std::mem::size_of;

use crate::{Result, sql::BoundExpr};

use super::{CellValue, cell};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum GroupKey {
    Encoded(Vec<u8>),
    Cells(Vec<CellValue>),
}

impl GroupKey {
    pub(super) fn memory_size(&self) -> usize {
        match self {
            Self::Encoded(bytes) => bytes.capacity().saturating_add(64),
            Self::Cells(values) => values
                .capacity()
                .saturating_mul(size_of::<CellValue>())
                .saturating_add(values.iter().fold(0usize, |bytes, value| {
                    bytes.saturating_add(match value {
                        CellValue::Utf8(value) => value.capacity(),
                        CellValue::Binary(value) => value.capacity(),
                        _ => 0,
                    })
                }))
                .saturating_add(64),
        }
    }
}

pub(super) enum GroupKeyEncoder {
    Empty,
    Rows(RowConverter),
    Cells,
}

pub(super) enum EncodedGroupRows {
    Empty,
    Rows(Rows),
    Cells,
}

impl EncodedGroupRows {
    pub(super) fn memory_size(&self) -> usize {
        match self {
            Self::Rows(rows) => rows.size(),
            Self::Empty | Self::Cells => 0,
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
        let fields = groups
            .iter()
            .map(|group| SortField::new(group.data_type.clone()))
            .collect();
        RowConverter::new(fields).map_or(Self::Cells, Self::Rows)
    }

    pub(super) fn encode(&self, arrays: &[ArrayRef]) -> Result<EncodedGroupRows> {
        match self {
            Self::Empty => Ok(EncodedGroupRows::Empty),
            Self::Rows(converter) => converter
                .convert_columns(arrays)
                .map(EncodedGroupRows::Rows)
                .map_err(Into::into),
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
            (Self::Rows(_), EncodedGroupRows::Rows(rows)) => {
                Ok(GroupKey::Encoded(rows.row(row).data().to_vec()))
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
        assert_eq!(
            encoder.key(&rows, &arrays, 0).unwrap(),
            encoder.key(&rows, &arrays, 1).unwrap()
        );
    }
}

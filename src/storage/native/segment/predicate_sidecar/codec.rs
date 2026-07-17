use arrow::datatypes::DataType;

use super::{
    PredicateType, Result, array_decode::decode_selected_array, evaluate::evaluate_all_block_typed,
};
#[cfg(test)]
use super::{decode::decode_block, encode::encode_block, evaluate::evaluate_block, format::parse};

#[cfg(test)]
pub(crate) use super::format::Encoding;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ComparisonOp {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Predicate {
    Compare { op: ComparisonOp, value: i64 },
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedPredicateBlock {
    #[cfg(test)]
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) struct DecodedPredicateBlock {
    pub(super) data_type: PredicateType,
    pub(super) values: Vec<Option<i64>>,
}

impl EncodedPredicateBlock {
    /// Encodes a row-group/column block when it is at most half the raw fixed
    /// width after reserving space for its future directory entry.
    #[cfg(test)]
    pub(crate) fn encode(data_type: PredicateType, values: &[Option<i64>]) -> Result<Option<Self>> {
        encode_block(data_type, values).map(|bytes| bytes.map(|bytes| Self { bytes }))
    }

    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        evaluate_block(&bytes, Predicate::IsNotNull)?;
        Ok(Self { bytes })
    }

    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[cfg(test)]
    pub(crate) fn encoding(&self) -> Result<Encoding> {
        Ok(parse(&self.bytes)?.encoding)
    }

    #[cfg(test)]
    pub(crate) fn row_count(&self) -> Result<usize> {
        Ok(parse(&self.bytes)?.row_count)
    }

    #[cfg(test)]
    pub(crate) fn decode(&self) -> Result<DecodedPredicateBlock> {
        decode_block(&self.bytes)
    }

    #[cfg(test)]
    pub(crate) fn evaluate(&self, predicate: Predicate) -> Result<Vec<bool>> {
        // The scan hot path streams ids/deltas from the bit-packed payload and
        // builds only the compact Vec<bool> selection; it never materializes a
        // row-sized Vec<Option<i64>>.
        evaluate_block(&self.bytes, predicate)
    }

    /// Evaluates a borrowed block only when its encoded logical type exactly
    /// matches the physical column that owns the sidecar directory entry.
    pub(crate) fn evaluate_all_bytes_for_type(
        bytes: &[u8],
        data_type: &DataType,
        predicates: &[Predicate],
    ) -> Result<Vec<bool>> {
        evaluate_all_block_typed(bytes, PredicateType::from_arrow(data_type)?, predicates)
    }

    /// Decodes selected rows from a borrowed block directly into a typed Arrow
    /// array. Every encoded row is still validated, including unselected rows.
    #[allow(dead_code)] // Wired into the native full-projection scan in the next integration slice.
    pub(crate) fn decode_selected_bytes_for_type(
        bytes: &[u8],
        data_type: &DataType,
        selection: &[bool],
    ) -> Result<arrow::array::ArrayRef> {
        decode_selected_array(bytes, data_type, selection)
    }
}

#[cfg(test)]
impl DecodedPredicateBlock {
    pub(crate) fn data_type(&self) -> PredicateType {
        self.data_type
    }

    pub(crate) fn values(&self) -> &[Option<i64>] {
        &self.values
    }

    pub(crate) fn evaluate(&self, predicate: Predicate) -> Vec<bool> {
        self.values
            .iter()
            .map(|value| match predicate {
                Predicate::IsNull => value.is_none(),
                Predicate::IsNotNull => value.is_some(),
                Predicate::Compare { op, value: rhs } => {
                    value.is_some_and(|lhs| compare(lhs, rhs, op))
                }
            })
            .collect()
    }
}

pub(super) fn compare(lhs: i64, rhs: i64, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Equal => lhs == rhs,
        ComparisonOp::NotEqual => lhs != rhs,
        ComparisonOp::Less => lhs < rhs,
        ComparisonOp::LessOrEqual => lhs <= rhs,
        ComparisonOp::Greater => lhs > rhs,
        ComparisonOp::GreaterOrEqual => lhs >= rhs,
    }
}

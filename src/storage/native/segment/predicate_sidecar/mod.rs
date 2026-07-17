//! Compact fixed-width predicate blocks for native segments.
//!
//! This module is intentionally independent from the segment manifest and
//! writer. The on-disk integration can therefore adopt the codec without
//! coupling predicate evaluation to commit or recovery code.

#[allow(dead_code)] // Wired into the native full-projection scan in the next integration slice.
mod array_decode;
mod bitpack;
mod codec;
#[cfg(test)]
mod collector;
#[cfg(test)]
mod decode;
#[cfg(test)]
mod encode;
mod evaluate;
mod format;
mod logical_type;
mod whole_file;

#[cfg(test)]
pub(crate) use codec::DecodedPredicateBlock;
#[cfg(test)]
pub(crate) use codec::Encoding;
pub(crate) use codec::{ComparisonOp, EncodedPredicateBlock, Predicate};
#[cfg(test)]
pub(in crate::storage::native) use collector::PredicateSidecarArtifact;
#[cfg(test)]
pub(super) use collector::PredicateSidecarCollector;
pub(crate) use logical_type::PredicateType;
pub(crate) use whole_file::PredicateSidecarIndex;
#[cfg(test)]
pub(crate) use whole_file::{FILE_FORMAT_VERSION, PredicateSidecarBlock, PredicateSidecarFile};

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum PredicateSidecarError {
    #[error("unsupported predicate sidecar type: {0}")]
    UnsupportedType(String),

    #[error("predicate sidecar value {value} is outside {data_type:?}")]
    ValueOutOfRange {
        data_type: PredicateType,
        value: i64,
    },

    #[error("predicate sidecar block is corrupt: {0}")]
    Corrupt(String),

    #[error("predicate sidecar block is too large")]
    TooLarge,
}

pub(crate) type Result<T> = std::result::Result<T, PredicateSidecarError>;

#[cfg(test)]
mod tests;

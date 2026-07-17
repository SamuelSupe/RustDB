mod local_identity;
mod location;
mod native;

pub(crate) use local_identity::LocalFileIdentity;
pub(crate) use location::validate_endpoint;
pub use location::{LocationResolver, ObjectSnapshot, ObjectSource};
pub(crate) use native::{
    NativeDatabase, NativePredicate, NativePredicateBlock, NativePredicateComparisonOp,
    NativePredicateSidecarBinding, NativePredicateSidecarIndex,
    TableSnapshot as NativeTableSnapshot,
};
pub(crate) use native::{NativeTableWriter, NativeWriteMode, PreparedSnapshot};

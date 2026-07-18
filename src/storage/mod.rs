mod copy_manifest;
mod destination;
mod local_identity;
mod location;
mod native;
mod remote_backup;
mod remote_manifest;
mod remote_temp;
mod s3_uri;

pub(crate) use copy_manifest::CopyManifestEntry;
pub(crate) use copy_manifest::{FILE_NAME as COPY_MANIFEST_FILE, encode as encode_copy_manifest};
pub(crate) use destination::WriteDestination;
pub(crate) use local_identity::LocalFileIdentity;
pub(crate) use location::validate_endpoint;
pub use location::{LocationResolver, ObjectSnapshot, ObjectSource};
pub(crate) use native::{
    DeleteVector as NativeDeleteVector, NativeDatabase, NativeDeleteSegment, NativeDeleteWriter,
    NativePredicate, NativePredicateBlock, NativePredicateComparisonOp,
    NativePredicateSidecarBinding, NativePredicateSidecarIndex, NativePublishedSnapshot,
    NativeTransactionChanges, NativeView, TableSnapshot as NativeTableSnapshot,
    decode_native_segment_batch, native_segment_encoding_required, native_segment_physical_schema,
};
pub(crate) use native::{
    NativeCommit, NativeTableWriter, NativeWriteMode, NativeWritePlan, PreparedSnapshot,
};
#[cfg(test)]
pub(crate) use native::{
    NativeCommitTestBoundary, arm_native_commit_test_failpoint,
    arm_native_wal_ambiguous_reconciliation,
};
#[cfg(test)]
pub(crate) use remote_backup::upload_to_store as upload_remote_backup_to_store;
pub(crate) use remote_backup::{
    download as download_remote_backup, upload as upload_remote_backup,
};
pub(crate) use remote_manifest::{
    PublicationState, inspect as inspect_remote_manifest,
    is_definitive_rejection as is_definitive_remote_rejection,
};
pub(crate) use remote_temp::{
    RemoteTempDir, RemoteTempKind, scavenge as scavenge_remote_temp_orphans,
};

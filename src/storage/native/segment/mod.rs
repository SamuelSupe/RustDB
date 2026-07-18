pub(crate) mod encoding;
mod fingerprint;
mod metadata;
#[cfg(test)]
pub(in crate::storage::native) mod predicate_file_writer;
pub(crate) mod predicate_sidecar;
#[allow(dead_code)]
pub(crate) mod row_id;
mod staging_file;
pub(crate) mod writer;

pub(crate) use metadata::SegmentMetadata;

pub(crate) const FORMAT_VERSION: u32 = 1;

#[cfg(test)]
mod tests;

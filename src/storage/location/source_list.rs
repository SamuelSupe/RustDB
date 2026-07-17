use std::mem::size_of;

use crate::{Error, Result};

use super::ObjectSource;

// Covers per-file provider state created after resolution (Hive values,
// lightweight scan handles, and allocator slack) in addition to ObjectSource.
const SOURCE_RUNTIME_SLACK_BYTES: usize = 512;

pub(super) struct SourceList {
    objects: Vec<ObjectSource>,
    dynamic_bytes: usize,
    limit: usize,
}

impl SourceList {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            objects: Vec::new(),
            dynamic_bytes: 0,
            limit,
        }
    }

    pub(super) fn push(&mut self, object: ObjectSource) -> Result<()> {
        let dynamic = dynamic_bytes(&object);
        let slots = self
            .objects
            .len()
            .saturating_add(1)
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
            .max(4);
        let required = slots
            .saturating_mul(size_of::<ObjectSource>())
            .saturating_add(self.dynamic_bytes)
            .saturating_add(dynamic);
        ensure_within_limit(required, self.limit)?;
        self.dynamic_bytes = self.dynamic_bytes.saturating_add(dynamic);
        self.objects.push(object);
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Vec<ObjectSource>> {
        self.objects.sort_by(|left, right| left.uri.cmp(&right.uri));
        self.objects.dedup_by(|left, right| left.uri == right.uri);
        let required = self
            .objects
            .capacity()
            .saturating_mul(size_of::<ObjectSource>())
            .saturating_add(self.objects.iter().fold(0usize, |bytes, object| {
                bytes.saturating_add(dynamic_bytes(object))
            }));
        ensure_within_limit(required, self.limit)?;
        Ok(self.objects)
    }
}

fn dynamic_bytes(object: &ObjectSource) -> usize {
    object
        .uri
        .capacity()
        .saturating_add(object.location.as_ref().len())
        .saturating_add(object.snapshot.e_tag.as_ref().map_or(0, String::capacity))
        .saturating_add(object.snapshot.version.as_ref().map_or(0, String::capacity))
        .saturating_add(
            object
                .local_path
                .as_ref()
                .map_or(0, |path| path.as_os_str().len()),
        )
        .saturating_add(SOURCE_RUNTIME_SLACK_BYTES)
}

fn ensure_within_limit(required: usize, limit: usize) -> Result<()> {
    if required <= limit {
        return Ok(());
    }
    Err(Error::ResourceExhausted(format!(
        "resolved file metadata requires at least {required} bytes, exceeding the configured \
         {limit}-byte file metadata limit; narrow the file pattern or increase the engine \
         memory limit"
    )))
}

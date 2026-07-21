use std::{
    fs::{self, File},
    io::{Cursor, Read},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    datatypes::SchemaRef,
    ipc::{reader::FileReader, writer::FileWriter},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

use super::layout::atomic_write_private;

pub(super) const FORMAT_EPOCH: u32 = 2;
pub(super) const MANIFEST_FILE: &str = "manifest.json";
pub(super) const PRODUCER_VERSION: &str = env!("CARGO_PKG_VERSION");
pub(super) const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestState {
    Running,
    Completed,
    Interrupted,
    Failed,
    Invalidated,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BatchEntry {
    pub(super) seq: u64,
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub(super) format_epoch: u32,
    pub(super) producer_version: String,
    pub(super) query_id: String,
    pub(super) schema_ipc_base64: String,
    pub(super) state: ManifestState,
    pub(super) next_batch_seq: u64,
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) batches: Vec<BatchEntry>,
    pub(super) error: Option<String>,
    pub(super) created_at_ms: u64,
    pub(super) updated_at_ms: u64,
}

impl Manifest {
    pub(super) fn new(query_id: &str, schema: &SchemaRef) -> Result<Self> {
        let now = now_ms();
        Ok(Self {
            format_epoch: FORMAT_EPOCH,
            producer_version: PRODUCER_VERSION.to_owned(),
            query_id: query_id.to_owned(),
            schema_ipc_base64: encode_schema(schema)?,
            state: ManifestState::Running,
            next_batch_seq: 0,
            rows: 0,
            bytes: 0,
            batches: Vec::new(),
            error: None,
            created_at_ms: now,
            updated_at_ms: now,
        })
    }

    pub(super) fn append(&mut self, entries: Vec<BatchEntry>) -> Result<()> {
        if self.state != ManifestState::Running {
            return Err(Error::Internal(
                "cannot append to a terminal HTTP result".into(),
            ));
        }
        for entry in entries {
            if entry.seq != self.next_batch_seq
                || entry.rows == 0
                || entry.bytes == 0
                || !valid_sha256(&entry.sha256)
            {
                return Err(Error::Internal(
                    "HTTP result batch sequence is not continuous".into(),
                ));
            }
            self.next_batch_seq = self.next_batch_seq.checked_add(1).ok_or_else(|| {
                Error::ResourceExhausted("result batch sequence overflowed".into())
            })?;
            self.rows = self
                .rows
                .checked_add(entry.rows)
                .ok_or_else(|| Error::ResourceExhausted("result row count overflowed".into()))?;
            self.bytes = self
                .bytes
                .checked_add(entry.bytes)
                .ok_or_else(|| Error::ResourceExhausted("result byte count overflowed".into()))?;
            self.batches.push(entry);
        }
        self.updated_at_ms = now_ms();
        Ok(())
    }

    pub(super) fn complete(&mut self) {
        self.state = ManifestState::Completed;
        self.error = None;
        self.updated_at_ms = now_ms();
    }

    pub(super) fn interrupt(&mut self, message: impl Into<String>) {
        self.state = ManifestState::Interrupted;
        self.error = Some(message.into());
        self.updated_at_ms = now_ms();
    }

    pub(super) fn fail(&mut self, message: impl Into<String>) {
        self.state = ManifestState::Failed;
        self.clear_batches();
        self.error = Some(message.into());
        self.updated_at_ms = now_ms();
    }

    pub(super) fn validate(&self, query_id: &str) -> Result<()> {
        if self.format_epoch != FORMAT_EPOCH {
            return Err(Error::InvalidArgument(format!(
                "HTTP result uses unsupported format epoch {}",
                self.format_epoch
            )));
        }
        if self.query_id != query_id {
            return Err(Error::InvalidArgument(
                "HTTP result manifest query ID does not match its directory".into(),
            ));
        }
        if self.next_batch_seq != u64::try_from(self.batches.len()).unwrap_or(u64::MAX) {
            return Err(Error::InvalidArgument(
                "HTTP result manifest has a discontinuous batch sequence".into(),
            ));
        }
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for (index, entry) in self.batches.iter().enumerate() {
            if entry.seq != u64::try_from(index).unwrap_or(u64::MAX)
                || entry.rows == 0
                || entry.bytes == 0
                || !valid_sha256(&entry.sha256)
            {
                return Err(Error::InvalidArgument(
                    "HTTP result manifest has an invalid batch entry".into(),
                ));
            }
            rows = rows.checked_add(entry.rows).ok_or_else(|| {
                Error::InvalidArgument("HTTP result manifest row count overflowed".into())
            })?;
            bytes = bytes.checked_add(entry.bytes).ok_or_else(|| {
                Error::InvalidArgument("HTTP result manifest byte count overflowed".into())
            })?;
        }
        if rows != self.rows || bytes != self.bytes {
            return Err(Error::InvalidArgument(
                "HTTP result manifest totals do not match its batches".into(),
            ));
        }
        if matches!(
            self.state,
            ManifestState::Failed | ManifestState::Invalidated
        ) && !self.batches.is_empty()
        {
            return Err(Error::InvalidArgument(
                "terminal HTTP result manifest still references batch data".into(),
            ));
        }
        self.schema().map(|_| ())
    }

    pub(super) fn schema(&self) -> Result<SchemaRef> {
        decode_schema(&self.schema_ipc_base64)
    }

    pub(super) fn expired(&self, ttl: std::time::Duration, now: SystemTime) -> bool {
        let updated = UNIX_EPOCH
            .checked_add(std::time::Duration::from_millis(self.updated_at_ms))
            .unwrap_or(UNIX_EPOCH);
        now.duration_since(updated).is_ok_and(|age| age >= ttl)
    }

    fn clear_batches(&mut self) {
        self.next_batch_seq = 0;
        self.rows = 0;
        self.bytes = 0;
        self.batches.clear();
    }
}

pub(super) fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub(super) fn load(path: &Path) -> Result<Manifest> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if metadata.len() > MAX_MANIFEST_BYTES as u64 {
        return Err(manifest_too_large(path));
    }
    let file = File::open(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(MAX_MANIFEST_BYTES)
            .min(MAX_MANIFEST_BYTES),
    );
    file.take((MAX_MANIFEST_BYTES as u64).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(manifest_too_large(path));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        Error::InvalidArgument(format!(
            "invalid HTTP result manifest {}: {error}",
            path.display()
        ))
    })
}

pub(super) fn persist(directory: &Path, manifest: &Manifest) -> Result<()> {
    let bytes = serde_json::to_vec(manifest).map_err(|error| {
        Error::Internal(format!("failed to encode HTTP result manifest: {error}"))
    })?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(manifest_too_large(&directory.join(MANIFEST_FILE)));
    }
    atomic_write_private(&directory.join(MANIFEST_FILE), &bytes)
}

fn manifest_too_large(path: &Path) -> Error {
    Error::ResourceExhausted(format!(
        "HTTP result manifest {} exceeds the {}-byte limit",
        path.display(),
        MAX_MANIFEST_BYTES
    ))
}

fn encode_schema(schema: &SchemaRef) -> Result<String> {
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, schema)?;
        writer.finish()?;
    }
    Ok(STANDARD.encode(bytes))
}

fn decode_schema(encoded: &str) -> Result<SchemaRef> {
    let bytes = STANDARD.decode(encoded).map_err(|error| {
        Error::InvalidArgument(format!("invalid HTTP result schema encoding: {error}"))
    })?;
    let reader = FileReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.schema())
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(super) fn updated_at(manifest: &Manifest) -> SystemTime {
    UNIX_EPOCH
        .checked_add(std::time::Duration::from_millis(manifest.updated_at_ms))
        .unwrap_or(UNIX_EPOCH)
}

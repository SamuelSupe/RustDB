use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

use super::super::io;

pub(super) const MAX_RECORD_BYTES: usize = 64 * 1024;
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum TransactionMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum RecordKind {
    Begin {
        snapshot_generation: u64,
        mode: TransactionMode,
    },
    CatalogCommit {
        expected_generation: u64,
        generation: u64,
    },
    Abort,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    format_version: u32,
    database_id: String,
    lsn: u64,
    transaction_id: String,
    kind: RecordKind,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    record: Record,
    sha256: String,
}

#[derive(Serialize)]
struct EnvelopeRef<'a> {
    record: &'a Record,
    sha256: &'a str,
}

impl Record {
    pub(super) fn new(database_id: &str, lsn: u64, transaction_id: &str, kind: RecordKind) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            database_id: database_id.to_owned(),
            lsn,
            transaction_id: transaction_id.to_owned(),
            kind,
        }
    }

    pub(super) fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    pub(super) fn kind(&self) -> &RecordKind {
        &self.kind
    }
}

pub(super) fn write(path: &Path, record: &Record) -> Result<()> {
    validate(path, record, &record.database_id, record.lsn)?;
    let sha256 = io::json_sha256(path, record, MAX_RECORD_BYTES, "WAL record")?;
    let bytes = io::encode_json_bounded(
        path,
        &EnvelopeRef {
            record,
            sha256: &sha256,
        },
        MAX_RECORD_BYTES,
        "WAL record envelope",
        true,
        true,
    )?;
    io::atomic_create(path, &bytes)
}

pub(super) fn read(path: &Path, database_id: &str, expected_lsn: u64) -> Result<Record> {
    let bytes = io::read_bounded(path, MAX_RECORD_BYTES, "WAL record")?;
    let envelope: Envelope = serde_json::from_slice(&bytes)
        .map_err(|error| Error::native_storage(path, format!("invalid WAL record: {error}")))?;
    validate(path, &envelope.record, database_id, expected_lsn)?;
    let actual = io::json_sha256(path, &envelope.record, MAX_RECORD_BYTES, "WAL record")?;
    if envelope.sha256 != actual {
        return Err(Error::native_storage(path, "WAL record checksum mismatch"));
    }
    Ok(envelope.record)
}

fn validate(path: &Path, record: &Record, database_id: &str, expected_lsn: u64) -> Result<()> {
    if record.format_version != FORMAT_VERSION {
        return Err(Error::native_storage(
            path,
            format!(
                "unsupported WAL format version {}; expected {FORMAT_VERSION}",
                record.format_version
            ),
        ));
    }
    if record.database_id != database_id {
        return Err(Error::native_storage(
            path,
            "WAL record belongs to another database",
        ));
    }
    if record.lsn == 0 || record.lsn != expected_lsn {
        return Err(Error::native_storage(
            path,
            format!(
                "WAL record LSN mismatch: expected {expected_lsn}, found {}",
                record.lsn
            ),
        ));
    }
    Uuid::parse_str(&record.transaction_id).map_err(|error| {
        Error::native_storage(path, format!("invalid WAL transaction id: {error}"))
    })?;
    if let RecordKind::CatalogCommit {
        expected_generation,
        generation,
    } = record.kind
        && expected_generation.checked_add(1) != Some(generation)
    {
        return Err(Error::native_storage(
            path,
            "WAL catalog commit does not advance exactly one generation",
        ));
    }
    Ok(())
}

pub(super) fn path(directory: &Path, lsn: u64) -> PathBuf {
    directory.join(format!("{lsn:020}.wal"))
}

pub(super) fn lsn_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_suffix(".wal")?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

use super::super::io;

pub(super) const FILE_NAME: &str = "CHECKPOINT";
const FORMAT_VERSION: u32 = 1;
const MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    format_version: u32,
    database_id: String,
    next_lsn: u64,
    catalog_generation: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    checkpoint: Checkpoint,
    sha256: String,
}

#[derive(Serialize)]
struct EnvelopeRef<'a> {
    checkpoint: &'a Checkpoint,
    sha256: &'a str,
}

pub(super) fn read(directory: &Path, database_id: &str) -> Result<(u64, u64)> {
    let path = directory.join(FILE_NAME);
    if !path.exists() {
        return Ok((1, 0));
    }
    let bytes = io::read_bounded(&path, MAX_BYTES, "WAL checkpoint")?;
    let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|error| {
        Error::native_storage(&path, format!("invalid WAL checkpoint: {error}"))
    })?;
    validate(&path, &envelope.checkpoint, database_id)?;
    let actual = io::json_sha256(&path, &envelope.checkpoint, MAX_BYTES, "WAL checkpoint")?;
    if envelope.sha256 != actual {
        return Err(Error::native_storage(
            &path,
            "WAL checkpoint checksum mismatch",
        ));
    }
    Ok((
        envelope.checkpoint.next_lsn,
        envelope.checkpoint.catalog_generation,
    ))
}

pub(super) fn write(
    directory: &Path,
    database_id: &str,
    next_lsn: u64,
    catalog_generation: u64,
) -> Result<()> {
    let path = directory.join(FILE_NAME);
    let checkpoint = Checkpoint {
        format_version: FORMAT_VERSION,
        database_id: database_id.to_owned(),
        next_lsn,
        catalog_generation,
    };
    validate(&path, &checkpoint, database_id)?;
    let sha256 = io::json_sha256(&path, &checkpoint, MAX_BYTES, "WAL checkpoint")?;
    let bytes = io::encode_json_bounded(
        &path,
        &EnvelopeRef {
            checkpoint: &checkpoint,
            sha256: &sha256,
        },
        MAX_BYTES,
        "WAL checkpoint envelope",
        true,
        true,
    )?;
    if path.exists() {
        io::atomic_replace(&path, &bytes, &Uuid::new_v4().to_string())
    } else {
        io::atomic_create(&path, &bytes)
    }
}

fn validate(path: &Path, checkpoint: &Checkpoint, database_id: &str) -> Result<()> {
    if checkpoint.format_version != FORMAT_VERSION {
        return Err(Error::native_storage(
            path,
            format!(
                "unsupported WAL checkpoint format {}; expected {FORMAT_VERSION}",
                checkpoint.format_version
            ),
        ));
    }
    if checkpoint.database_id != database_id {
        return Err(Error::native_storage(
            path,
            "WAL checkpoint belongs to another database",
        ));
    }
    if checkpoint.next_lsn == 0 {
        return Err(Error::native_storage(
            path,
            "WAL checkpoint next LSN must be positive",
        ));
    }
    Ok(())
}

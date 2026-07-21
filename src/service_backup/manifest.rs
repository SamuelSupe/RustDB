use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

pub(super) const MANIFEST_FILE: &str = "manifest.json";
pub(super) const FORMAT: &str = "rustdb-service-backup";
pub(super) const FORMAT_VERSION: u32 = 1;
pub(super) const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub format: String,
    pub format_version: u32,
    pub producer_version: String,
    pub database_id: String,
    pub service_state_included: bool,
    pub excluded: Vec<String>,
    pub directories: Vec<String>,
    pub files: Vec<FileEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileEntry {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    manifest: Manifest,
    sha256: String,
}

#[derive(Serialize)]
struct EnvelopeRef<'a> {
    manifest: &'a Manifest,
    sha256: &'a str,
}

pub(super) fn encode(manifest: &Manifest) -> Result<Vec<u8>> {
    let body = serde_json::to_vec(manifest)
        .map_err(|error| Error::Internal(format!("cannot encode service backup: {error}")))?;
    let checksum = format!("{:x}", Sha256::digest(body));
    let mut encoded = serde_json::to_vec_pretty(&EnvelopeRef {
        manifest,
        sha256: &checksum,
    })
    .map_err(|error| Error::Internal(format!("cannot encode service backup: {error}")))?;
    encoded.push(b'\n');
    if encoded.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "service backup manifest exceeds {MAX_MANIFEST_BYTES} bytes"
        )));
    }
    Ok(encoded)
}

pub(super) fn decode(bytes: &[u8]) -> Result<Manifest> {
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "service backup manifest exceeds {MAX_MANIFEST_BYTES} bytes"
        )));
    }
    let envelope: Envelope = serde_json::from_slice(bytes)
        .map_err(|error| Error::Execution(format!("invalid service backup manifest: {error}")))?;
    let body = serde_json::to_vec(&envelope.manifest)
        .map_err(|error| Error::Internal(format!("cannot validate service backup: {error}")))?;
    if format!("{:x}", Sha256::digest(body)) != envelope.sha256 {
        return Err(Error::Execution(
            "service backup manifest checksum mismatch".to_owned(),
        ));
    }
    let manifest = envelope.manifest;
    if manifest.format != FORMAT || manifest.format_version != FORMAT_VERSION {
        return Err(Error::Execution(format!(
            "unsupported service backup format {} version {}",
            manifest.format, manifest.format_version
        )));
    }
    uuid::Uuid::parse_str(&manifest.database_id)
        .map_err(|_| Error::Execution("service backup database id is invalid".to_owned()))?;
    let mut paths = HashSet::new();
    if manifest.files.iter().any(|entry| {
        !paths.insert(entry.path.as_str())
            || !is_safe_relative(&entry.path)
            || entry.sha256.len() != 64
            || !entry.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    }) {
        return Err(Error::Execution(
            "service backup manifest contains an invalid or duplicate file".to_owned(),
        ));
    }
    let mut directories = HashSet::new();
    if manifest.directories.iter().any(|path| {
        !directories.insert(path.as_str())
            || !is_safe_relative(path)
            || paths.contains(path.as_str())
    }) {
        return Err(Error::Execution(
            "service backup manifest contains an invalid or duplicate directory".to_owned(),
        ));
    }
    Ok(manifest)
}

pub(super) fn is_safe_relative(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

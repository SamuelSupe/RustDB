use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

pub(crate) const FILE_NAME: &str = "_rustdb_manifest.json";
pub(crate) const MAX_BYTES: usize = 64 * 1024;
const FORMAT_VERSION: u32 = 2;
const LEGACY_FORMAT_VERSION: u32 = 1;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format_version: u32,
    format: String,
    object: String,
    bytes: u64,
    sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    e_tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<String>,
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

#[derive(Clone, Debug)]
pub(crate) struct CopyManifestEntry {
    pub(crate) format: String,
    pub(crate) object: String,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
    pub(crate) e_tag: Option<String>,
    pub(crate) version: Option<String>,
}

pub(crate) fn encode(entry: &CopyManifestEntry) -> Result<Vec<u8>> {
    encode_version(entry, FORMAT_VERSION, true)
}

#[cfg(test)]
pub(crate) fn encode_legacy(entry: &CopyManifestEntry) -> Result<Vec<u8>> {
    encode_version(entry, LEGACY_FORMAT_VERSION, false)
}

fn encode_version(
    entry: &CopyManifestEntry,
    format_version: u32,
    require_identity: bool,
) -> Result<Vec<u8>> {
    validate_entry_inner(Path::new(FILE_NAME), entry, require_identity)?;
    let manifest = Manifest {
        format_version,
        format: entry.format.clone(),
        object: entry.object.clone(),
        bytes: entry.bytes,
        sha256: entry.sha256.clone(),
        e_tag: entry.e_tag.clone(),
        version: entry.version.clone(),
    };
    let encoded = serde_json::to_vec(&manifest)
        .map_err(|error| Error::Internal(format!("cannot encode COPY manifest: {error}")))?;
    let sha256 = format!("{:x}", Sha256::digest(&encoded));
    let envelope = serde_json::to_vec_pretty(&EnvelopeRef {
        manifest: &manifest,
        sha256: &sha256,
    })
    .map_err(|error| Error::Internal(format!("cannot encode COPY envelope: {error}")))?;
    if envelope.len() > MAX_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "COPY manifest exceeds {MAX_BYTES} bytes"
        )));
    }
    Ok(envelope)
}

pub(crate) fn decode(path: &Path, bytes: &[u8]) -> Result<CopyManifestEntry> {
    if bytes.len() > MAX_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "COPY manifest '{}' exceeds {MAX_BYTES} bytes",
            path.display()
        )));
    }
    let envelope: Envelope = serde_json::from_slice(bytes)
        .map_err(|error| Error::Execution(format!("invalid COPY manifest: {error}")))?;
    if !matches!(
        envelope.manifest.format_version,
        LEGACY_FORMAT_VERSION | FORMAT_VERSION
    ) {
        return Err(Error::Execution(format!(
            "unsupported COPY manifest format {}",
            envelope.manifest.format_version
        )));
    }
    let encoded = serde_json::to_vec(&envelope.manifest)
        .map_err(|error| Error::Internal(format!("cannot validate COPY manifest: {error}")))?;
    if format!("{:x}", Sha256::digest(&encoded)) != envelope.sha256 {
        return Err(Error::Execution(
            "COPY manifest checksum mismatch".to_owned(),
        ));
    }
    let legacy = envelope.manifest.format_version == LEGACY_FORMAT_VERSION;
    let entry = CopyManifestEntry {
        format: envelope.manifest.format,
        object: envelope.manifest.object,
        bytes: envelope.manifest.bytes,
        sha256: envelope.manifest.sha256,
        e_tag: envelope.manifest.e_tag,
        version: envelope.manifest.version,
    };
    validate_entry_inner(path, &entry, !legacy)?;
    Ok(entry)
}

fn validate_entry_inner(
    path: &Path,
    entry: &CopyManifestEntry,
    require_identity: bool,
) -> Result<()> {
    if !matches!(entry.format.as_str(), "csv" | "parquet") {
        return Err(Error::Execution(format!(
            "COPY manifest '{}' contains unsupported format '{}'",
            path.display(),
            entry.format
        )));
    }
    if entry.object.is_empty()
        || entry.object.starts_with('/')
        || entry.object.contains("..")
        || entry.object.contains('\\')
        || entry.sha256.len() != 64
        || !entry.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        || entry.e_tag.as_ref().is_some_and(String::is_empty)
        || entry.version.as_ref().is_some_and(String::is_empty)
        || (require_identity && entry.e_tag.is_none() && entry.version.is_none())
    {
        return Err(Error::Execution(format!(
            "COPY manifest '{}' contains an invalid data object",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CopyManifestEntry, decode, encode};

    #[test]
    fn manifest_round_trips_and_detects_corruption() {
        let entry = CopyManifestEntry {
            format: "parquet".to_owned(),
            object: "prefix/part.parquet".to_owned(),
            bytes: 42,
            sha256: "a".repeat(64),
            e_tag: Some("etag".to_owned()),
            version: None,
        };
        let encoded = encode(&entry).unwrap();
        let decoded = decode(std::path::Path::new("manifest"), &encoded).unwrap();
        assert_eq!(decoded.object, entry.object);
        let mut corrupted = encoded;
        let index = corrupted.len() / 2;
        corrupted[index] ^= 1;
        assert!(decode(std::path::Path::new("manifest"), &corrupted).is_err());
    }

    #[test]
    fn current_manifest_requires_a_stable_object_identity() {
        let entry = CopyManifestEntry {
            format: "csv".to_owned(),
            object: "prefix/part.csv".to_owned(),
            bytes: 4,
            sha256: "a".repeat(64),
            e_tag: None,
            version: None,
        };
        assert!(encode(&entry).is_err());
        assert!(super::encode_legacy(&entry).is_ok());
    }
}

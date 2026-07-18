use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

use super::{
    CATALOG_GENERATION_BUDGET_PER_TABLE_BYTES, CatalogState, FORMAT_VERSION, LEGACY_FORMAT_VERSION,
    MAX_CATALOG_MANIFEST_BYTES, MAX_CURRENT_BYTES,
};
use crate::storage::native::io;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestEnvelope {
    manifest: CatalogState,
    sha256: String,
}

#[derive(Serialize)]
struct ManifestEnvelopeRef<'a> {
    manifest: &'a CatalogState,
    sha256: &'a str,
}

pub(super) fn ensure_generation(root: &Path, state: &CatalogState) -> Result<()> {
    let path = generation_path(root, state.generation);
    if path.exists() {
        let existing = read_generation(root, state.generation)?;
        if existing != *state {
            return Err(Error::native_storage(
                path,
                "catalog generation already exists with different contents",
            ));
        }
        return Ok(());
    }
    write_generation(root, state)
}

fn write_generation(root: &Path, state: &CatalogState) -> Result<()> {
    let path = generation_path(root, state.generation);
    let bytes = encode_generation(&path, state)?;
    io::atomic_create(&path, &bytes)
}

pub(super) fn validate_generation_size(root: &Path, state: &CatalogState) -> Result<()> {
    let path = generation_path(root, state.generation);
    encode_generation(&path, state).map(|_| ())
}

pub(super) fn encoded_generation_size(root: &Path, state: &CatalogState) -> Result<u64> {
    let path = generation_path(root, state.generation);
    u64::try_from(encode_generation(&path, state)?.len())
        .map_err(|_| Error::ResourceExhausted("catalog generation size overflowed".to_owned()))
}

fn encode_generation(path: &Path, state: &CatalogState) -> Result<Vec<u8>> {
    let max_bytes = generation_budget(state)?;
    let sha256 = io::json_sha256(path, state, max_bytes, "catalog manifest")?;
    let envelope = ManifestEnvelopeRef {
        manifest: state,
        sha256: &sha256,
    };
    io::encode_json_bounded(
        path,
        &envelope,
        max_bytes,
        "catalog generation manifest",
        true,
        true,
    )
}

fn generation_budget(state: &CatalogState) -> Result<usize> {
    let object_budget = state
        .tables
        .len()
        .saturating_add(state.views.len())
        .saturating_add(state.schemas.len())
        .max(1)
        .checked_mul(CATALOG_GENERATION_BUDGET_PER_TABLE_BYTES)
        .ok_or_else(|| Error::ResourceExhausted("catalog byte budget overflow".to_owned()))?;
    let view_payload_budget = state.views.values().try_fold(0_usize, |bytes, view| {
        let escaped_sql =
            view.sql.len().checked_mul(6).ok_or_else(|| {
                Error::ResourceExhausted("view SQL byte budget overflow".to_owned())
            })?;
        bytes
            .checked_add(escaped_sql)
            .and_then(|bytes| bytes.checked_add(view.schema_ipc_hex.len()))
            .ok_or_else(|| Error::ResourceExhausted("view payload byte budget overflow".to_owned()))
    })?;
    object_budget
        .checked_add(view_payload_budget)
        .map(|bytes| bytes.min(MAX_CATALOG_MANIFEST_BYTES))
        .ok_or_else(|| Error::ResourceExhausted("catalog byte budget overflow".to_owned()))
}

pub(super) fn read_generation(root: &Path, generation: u64) -> Result<CatalogState> {
    let path = generation_path(root, generation);
    let bytes = io::read_bounded(
        &path,
        MAX_CATALOG_MANIFEST_BYTES,
        "catalog generation manifest",
    )?;
    let envelope: ManifestEnvelope = serde_json::from_slice(&bytes).map_err(|error| {
        Error::native_storage(&path, format!("invalid catalog manifest: {error}"))
    })?;
    let max_bytes = generation_budget(&envelope.manifest)?;
    io::validate_size(&path, bytes.len(), max_bytes, "catalog generation manifest")?;
    let actual = io::json_sha256(&path, &envelope.manifest, max_bytes, "catalog manifest")?;
    if envelope.sha256 != actual {
        return Err(Error::native_storage(
            &path,
            "catalog manifest checksum mismatch",
        ));
    }
    if !matches!(
        envelope.manifest.format_version,
        LEGACY_FORMAT_VERSION | FORMAT_VERSION
    ) {
        return Err(Error::native_storage(
            &path,
            format!(
                "unsupported catalog format version {}; supported versions are {LEGACY_FORMAT_VERSION} and {FORMAT_VERSION}",
                envelope.manifest.format_version,
            ),
        ));
    }
    Ok(super::normalize_legacy(envelope.manifest))
}

pub(super) fn write_current(root: &Path, generation: u64) -> Result<()> {
    let path = current_path(root);
    let bytes = format!("{generation}\n");
    io::validate_size(&path, bytes.len(), MAX_CURRENT_BYTES, "catalog CURRENT")?;
    io::atomic_create(&path, bytes.as_bytes())
}

pub(super) fn read_current(root: &Path) -> Result<u64> {
    let path = current_path(root);
    let bytes = io::read_bounded(&path, MAX_CURRENT_BYTES, "catalog CURRENT")?;
    let value = std::str::from_utf8(&bytes).map_err(|error| {
        Error::native_storage(&path, format!("CURRENT is not valid UTF-8: {error}"))
    })?;
    value.trim().parse::<u64>().map_err(|error| {
        Error::native_storage(&path, format!("invalid CURRENT generation: {error}"))
    })
}

pub(super) fn current_path(root: &Path) -> PathBuf {
    root.join("catalog").join("CURRENT")
}

pub(super) fn generation_path(root: &Path, generation: u64) -> PathBuf {
    root.join("catalog")
        .join("generations")
        .join(format!("{generation:020}.json"))
}

pub(super) fn generation_from_name(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_suffix(".json")?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{Error, Result};

use super::{
    files::{
        atomic_write_secure, check_secure_directory, ensure_new_secure_directory,
        ensure_secure_directory, read_secure_file, secure_rename_directory,
    },
    token::{BearerToken, read_token_text},
};

const PROFILE_FORMAT: &str = "rustdb-http-shell-profile";
const PROFILE_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "profile.json";
const CA_FILE: &str = "ca.pem";
const TOKEN_FILE: &str = "bearer.token";
const MAX_MANIFEST_BYTES: u64 = 16 * 1024;
const MAX_CA_BYTES: u64 = 64 * 1024;

/// A client profile contains only file paths, never the bearer token value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientProfile {
    name: String,
    server_url: Url,
    ca_path: PathBuf,
    token_path: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProfileManifest {
    format: String,
    version: u32,
    server_url: String,
    ca_file: String,
    token_file: String,
}

impl ClientProfile {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn server_url(&self) -> &Url {
        &self.server_url
    }

    pub fn ca_path(&self) -> &Path {
        &self.ca_path
    }

    pub fn token_path(&self) -> &Path {
        &self.token_path
    }

    pub(crate) fn read_ca(&self) -> Result<Vec<u8>> {
        let ca = read_secure_file(&self.ca_path, MAX_CA_BYTES)?;
        validate_ca(&ca)?;
        Ok(ca)
    }

    pub(crate) fn read_token(&self) -> Result<String> {
        read_token_text(&self.token_path)
    }
}

/// Copies a generated offline bundle after validating every source file. The
/// destination is published atomically and is never overwritten.
pub fn copy_profile_bundle(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
    let source = source.as_ref();
    check_secure_directory(source)?;
    let manifest = read_manifest(source)?;
    let server_url = decode_manifest(&manifest)?;
    export_profile_bundle(
        destination,
        &server_url,
        source.join(CA_FILE),
        source.join(TOKEN_FILE),
    )
}

/// Copies the endpoint and CA from a managed bundle while substituting one
/// explicitly selected local credential. This is the onboarding path for
/// non-bootstrap principals; the token value never becomes a CLI argument.
pub fn export_profile_bundle_with_token(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    token_path: impl AsRef<Path>,
) -> Result<()> {
    let source = source.as_ref();
    check_secure_directory(source)?;
    let manifest = read_manifest(source)?;
    let server_url = decode_manifest(&manifest)?;
    export_profile_bundle(destination, &server_url, source.join(CA_FILE), token_path)
}

/// Exports a permission-restricted directory suitable for offline transfer.
pub fn export_profile_bundle(
    destination: impl AsRef<Path>,
    server_url: &Url,
    ca_path: impl AsRef<Path>,
    token_path: impl AsRef<Path>,
) -> Result<()> {
    validate_server_url(server_url)?;
    let ca = read_secure_file(ca_path.as_ref(), MAX_CA_BYTES)?;
    validate_ca(&ca)?;
    // Validate token format without exposing its value.
    let _ = BearerToken::load(token_path.as_ref())?;
    let token = read_secure_file(token_path.as_ref(), 128)?;

    let destination = destination.as_ref();
    let staging = staging_sibling(destination, "export")?;
    ensure_new_secure_directory(&staging)?;
    let result = (|| {
        let manifest = ProfileManifest {
            format: PROFILE_FORMAT.to_owned(),
            version: PROFILE_VERSION,
            server_url: server_url.as_str().to_owned(),
            ca_file: CA_FILE.to_owned(),
            token_file: TOKEN_FILE.to_owned(),
        };
        let encoded = serde_json::to_vec_pretty(&manifest).map_err(|error| {
            Error::Internal(format!("failed to encode HTTP shell profile: {error}"))
        })?;
        atomic_write_secure(&staging.join(MANIFEST_FILE), &encoded)?;
        atomic_write_secure(&staging.join(CA_FILE), &ca)?;
        atomic_write_secure(&staging.join(TOKEN_FILE), &token)?;
        secure_rename_directory(&staging, destination)
    })();
    cleanup_staging_after(&staging, result)
}

/// Refreshes the server-owned connection bundle in place. The manifest is
/// published last so a changed endpoint never leaves a durable stale profile.
pub(crate) fn write_managed_profile_bundle(
    destination: &Path,
    server_url: &Url,
    ca_path: &Path,
    token_path: &Path,
) -> Result<()> {
    validate_server_url(server_url)?;
    let ca = read_secure_file(ca_path, MAX_CA_BYTES)?;
    validate_ca(&ca)?;
    let _ = BearerToken::load(token_path)?;
    let token = read_secure_file(token_path, 128)?;
    if destination.exists() {
        check_secure_directory(destination)?;
    } else {
        ensure_secure_directory(destination)?;
    }
    let manifest = ProfileManifest {
        format: PROFILE_FORMAT.to_owned(),
        version: PROFILE_VERSION,
        server_url: server_url.as_str().to_owned(),
        ca_file: CA_FILE.to_owned(),
        token_file: TOKEN_FILE.to_owned(),
    };
    let encoded = serde_json::to_vec_pretty(&manifest).map_err(|error| {
        Error::Internal(format!("failed to encode HTTP shell profile: {error}"))
    })?;
    atomic_write_secure(&destination.join(TOKEN_FILE), &token)?;
    atomic_write_secure(&destination.join(CA_FILE), &ca)?;
    atomic_write_secure(&destination.join(MANIFEST_FILE), &encoded)
}

/// Imports an offline bundle under a named profile without overwriting an
/// existing profile.
pub fn import_profile_bundle(
    profile_root: impl AsRef<Path>,
    name: &str,
    bundle: impl AsRef<Path>,
) -> Result<ClientProfile> {
    validate_profile_name(name)?;
    let bundle = bundle.as_ref();
    check_secure_directory(bundle)?;
    let manifest = read_manifest(bundle)?;
    let server_url = decode_manifest(&manifest)?;
    let ca = read_secure_file(&bundle.join(CA_FILE), MAX_CA_BYTES)?;
    validate_ca(&ca)?;
    let source_token = bundle.join(TOKEN_FILE);
    let _ = BearerToken::load(&source_token)?;
    let token = read_secure_file(&source_token, 128)?;

    let profile_root = profile_root.as_ref();
    ensure_secure_directory(profile_root)?;
    let destination = profile_root.join(name);
    let staging = profile_root.join(format!(".{name}.import.{}", Uuid::new_v4()));
    ensure_new_secure_directory(&staging)?;
    let result = (|| {
        let encoded = serde_json::to_vec_pretty(&manifest).map_err(|error| {
            Error::Internal(format!("failed to encode HTTP shell profile: {error}"))
        })?;
        atomic_write_secure(&staging.join(MANIFEST_FILE), &encoded)?;
        atomic_write_secure(&staging.join(CA_FILE), &ca)?;
        atomic_write_secure(&staging.join(TOKEN_FILE), &token)?;
        secure_rename_directory(&staging, &destination)?;
        Ok(ClientProfile {
            name: name.to_owned(),
            server_url: server_url.clone(),
            ca_path: destination.join(CA_FILE),
            token_path: destination.join(TOKEN_FILE),
        })
    })();
    cleanup_staging_after(&staging, result)
}

/// Loads and validates an imported named profile.
pub fn load_profile(profile_root: impl AsRef<Path>, name: &str) -> Result<ClientProfile> {
    validate_profile_name(name)?;
    let directory = profile_root.as_ref().join(name);
    check_secure_directory(&directory)?;
    let manifest = read_manifest(&directory)?;
    let server_url = decode_manifest(&manifest)?;
    let ca_path = directory.join(CA_FILE);
    let token_path = directory.join(TOKEN_FILE);
    validate_ca(&read_secure_file(&ca_path, MAX_CA_BYTES)?)?;
    let _ = BearerToken::load(&token_path)?;
    Ok(ClientProfile {
        name: name.to_owned(),
        server_url,
        ca_path,
        token_path,
    })
}

fn read_manifest(directory: &Path) -> Result<ProfileManifest> {
    let path = directory.join(MANIFEST_FILE);
    let encoded = read_secure_file(&path, MAX_MANIFEST_BYTES)?;
    serde_json::from_slice(&encoded).map_err(|error| {
        Error::InvalidArgument(format!(
            "invalid HTTP shell profile at {}: {error}",
            path.display()
        ))
    })
}

fn decode_manifest(manifest: &ProfileManifest) -> Result<Url> {
    if manifest.format != PROFILE_FORMAT
        || manifest.version != PROFILE_VERSION
        || manifest.ca_file != CA_FILE
        || manifest.token_file != TOKEN_FILE
    {
        return Err(Error::InvalidArgument(
            "unsupported or unsafe HTTP shell profile manifest".to_owned(),
        ));
    }
    let server_url = Url::parse(&manifest.server_url)
        .map_err(|error| Error::InvalidArgument(format!("invalid profile URL: {error}")))?;
    validate_server_url(&server_url)?;
    Ok(server_url)
}

fn validate_server_url(url: &Url) -> Result<()> {
    if url.scheme() != "https"
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(Error::InvalidArgument(
            "profile URL must be an HTTPS origin without credentials, path, query, or fragment"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_ca(encoded: &[u8]) -> Result<()> {
    if encoded.starts_with(b"-----BEGIN CERTIFICATE-----")
        && encoded
            .windows(b"-----END CERTIFICATE-----".len())
            .any(|window| window == b"-----END CERTIFICATE-----")
    {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "profile CA file is not a PEM certificate".to_owned(),
        ))
    }
}

fn validate_profile_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || matches!(name, "." | "..")
    {
        return Err(Error::InvalidArgument(
            "profile name must use 1-64 ASCII letters, digits, '.', '_' or '-'".to_owned(),
        ));
    }
    Ok(())
}

fn staging_sibling(destination: &Path, purpose: &str) -> Result<PathBuf> {
    let parent = destination.parent().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "bundle path has no parent: {}",
            destination.display()
        ))
    })?;
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Error::InvalidArgument("bundle path must have a UTF-8 file name".to_owned())
        })?;
    Ok(parent.join(format!(".{name}.{purpose}.{}", Uuid::new_v4())))
}

fn cleanup_staging_after<T>(staging: &Path, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => match fs::remove_dir_all(staging) {
            Ok(()) => Err(error),
            Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => Err(error),
            Err(cleanup) => Err(Error::Io {
                path: Some(staging.to_path_buf()),
                source: cleanup,
            }),
        },
    }
}

use std::path::{Path, PathBuf};

use chrono::{DateTime, Datelike, Duration, Utc};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{Error, Result};

use super::{
    endpoint::ServerEndpoint,
    files::{atomic_write_secure, read_secure_file},
    state::SecurityState,
};

const CA_VALID_DAYS: i64 = 7_300;
const LEAF_VALID_DAYS: i64 = 30;
const LEAF_RENEW_DAYS: i64 = 7;
const MAX_IDENTITY_BYTES: u64 = 64 * 1024;
const MAX_METADATA_BYTES: u64 = 4 * 1024;
const TLS_METADATA_VERSION: u32 = 1;

/// Paths and metadata for a generated local CA and short-lived server leaf.
#[derive(Clone, Debug)]
pub struct TlsMaterial {
    ca_certificate_path: PathBuf,
    server_identity_path: PathBuf,
    public_url: Url,
    leaf_not_after_unix: i64,
    renewed: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TlsMetadata {
    version: u32,
    certificate_host: String,
    leaf_not_after_unix: i64,
}

impl TlsMaterial {
    /// Generates the CA/token-independent TLS material on first start and
    /// renews only the server leaf when it is near expiry or its SAN changes.
    pub fn load_or_create(state: &SecurityState, endpoint: &ServerEndpoint) -> Result<Self> {
        Self::load_or_create_at(state, endpoint, Utc::now())
    }

    pub fn ca_certificate_path(&self) -> &Path {
        &self.ca_certificate_path
    }

    /// Combined PEM containing the server certificate followed by its PKCS#8
    /// private key. Keeping the pair in one atomically replaced file avoids a
    /// crash-visible mismatched certificate and key.
    pub fn server_identity_path(&self) -> &Path {
        &self.server_identity_path
    }

    /// Path passed to a PEM certificate loader. It is intentionally the same
    /// atomic combined-identity file as [`Self::server_private_key_path`].
    pub fn server_certificate_path(&self) -> &Path {
        &self.server_identity_path
    }

    /// Path passed to a PEM private-key loader. It is intentionally the same
    /// atomic combined-identity file as [`Self::server_certificate_path`].
    pub fn server_private_key_path(&self) -> &Path {
        &self.server_identity_path
    }

    pub fn public_url(&self) -> &Url {
        &self.public_url
    }

    pub fn leaf_not_after_unix(&self) -> i64 {
        self.leaf_not_after_unix
    }

    pub fn renewed(&self) -> bool {
        self.renewed
    }

    pub fn server_certificate_pem(&self) -> Result<String> {
        let identity = read_secure_file(&self.server_identity_path, MAX_IDENTITY_BYTES)?;
        extract_pem(&identity, "CERTIFICATE")
    }

    pub fn server_private_key_pem(&self) -> Result<String> {
        let identity = read_secure_file(&self.server_identity_path, MAX_IDENTITY_BYTES)?;
        extract_pem(&identity, "PRIVATE KEY")
    }

    pub(super) fn load_or_create_at(
        state: &SecurityState,
        endpoint: &ServerEndpoint,
        now: DateTime<Utc>,
    ) -> Result<Self> {
        ensure_ca(state, now)?;
        let metadata = load_optional_metadata(state)?;
        let identity_exists = state
            .server_identity_path()
            .try_exists()
            .map_err(|error| Error::io(state.server_identity_path(), error))?;
        let renewal_cutoff = now
            .checked_add_signed(Duration::days(LEAF_RENEW_DAYS))
            .ok_or_else(|| Error::Internal("TLS renewal time overflow".to_owned()))?
            .timestamp();
        let must_renew = !identity_exists
            || metadata.as_ref().is_none_or(|metadata| {
                metadata.version != TLS_METADATA_VERSION
                    || metadata.certificate_host != endpoint.certificate_host
                    || metadata.leaf_not_after_unix <= renewal_cutoff
            });
        let metadata = if must_renew {
            renew_leaf(state, endpoint, now)?
        } else {
            let metadata = metadata.expect("checked above");
            // Validate the credential file and its permissions before serving.
            let _ = read_secure_file(&state.server_identity_path(), MAX_IDENTITY_BYTES)?;
            metadata
        };
        Ok(Self {
            ca_certificate_path: state.ca_certificate_path(),
            server_identity_path: state.server_identity_path(),
            public_url: endpoint.public_url.clone(),
            leaf_not_after_unix: metadata.leaf_not_after_unix,
            renewed: must_renew,
        })
    }
}

fn ensure_ca(state: &SecurityState, now: DateTime<Utc>) -> Result<()> {
    let identity_exists = state
        .ca_identity_path()
        .try_exists()
        .map_err(|error| Error::io(state.ca_identity_path(), error))?;
    let certificate_exists = state
        .ca_certificate_path()
        .try_exists()
        .map_err(|error| Error::io(state.ca_certificate_path(), error))?;
    match (identity_exists, certificate_exists) {
        (true, true) => validate_ca_state(state),
        (false, false) => create_ca(state, now),
        (true, false) => {
            let identity = read_secure_file(&state.ca_identity_path(), MAX_IDENTITY_BYTES)?;
            let certificate = extract_pem(&identity, "CERTIFICATE")?;
            atomic_write_secure(&state.ca_certificate_path(), certificate.as_bytes())?;
            validate_ca_state(state)
        }
        _ => Err(Error::InvalidArgument(format!(
            "incomplete CA state in {}; refusing to replace the existing CA",
            state.directory().display()
        ))),
    }
}

fn validate_ca_state(state: &SecurityState) -> Result<()> {
    let identity = read_secure_file(&state.ca_identity_path(), MAX_IDENTITY_BYTES)?;
    let identity_certificate = extract_pem(&identity, "CERTIFICATE")?;
    let persisted_certificate = read_secure_file(&state.ca_certificate_path(), MAX_IDENTITY_BYTES)?;
    if identity_certificate.as_bytes() != persisted_certificate {
        return Err(Error::InvalidArgument(
            "persisted CA certificate does not match the CA identity".to_owned(),
        ));
    }
    let key_pem = extract_pem(&identity, "PRIVATE KEY")?;
    let key = KeyPair::from_pem(&key_pem)
        .map_err(|error| Error::InvalidArgument(format!("invalid persisted CA key: {error}")))?;
    Issuer::from_ca_cert_pem(&identity_certificate, &key).map_err(|error| {
        Error::InvalidArgument(format!("invalid persisted CA certificate: {error}"))
    })?;
    Ok(())
}

fn create_ca(state: &SecurityState, now: DateTime<Utc>) -> Result<()> {
    let signing_key = KeyPair::generate()
        .map_err(|error| Error::Internal(format!("failed to generate CA key: {error}")))?;
    let mut params = CertificateParams::default();
    let not_before = now - Duration::days(1);
    let not_after = now + Duration::days(CA_VALID_DAYS);
    params.not_before = rcgen::date_time_ymd(
        not_before.year(),
        not_before.month() as u8,
        not_before.day() as u8,
    );
    params.not_after = rcgen::date_time_ymd(
        not_after.year(),
        not_after.month() as u8,
        not_after.day() as u8,
    );
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "RustDB HTTP Shell Local CA");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let certificate = params
        .self_signed(&signing_key)
        .map_err(|error| Error::Internal(format!("failed to generate CA certificate: {error}")))?;
    let certificate_pem = certificate.pem();
    let identity = format!("{certificate_pem}{}", signing_key.serialize_pem());
    // The combined identity is the authoritative record. If publication of the
    // public copy is interrupted, startup refuses to rotate the established CA.
    atomic_write_secure(&state.ca_identity_path(), identity.as_bytes())?;
    atomic_write_secure(&state.ca_certificate_path(), certificate_pem.as_bytes())
}

fn renew_leaf(
    state: &SecurityState,
    endpoint: &ServerEndpoint,
    now: DateTime<Utc>,
) -> Result<TlsMetadata> {
    let ca_identity = read_secure_file(&state.ca_identity_path(), MAX_IDENTITY_BYTES)?;
    let ca_certificate_pem = extract_pem(&ca_identity, "CERTIFICATE")?;
    let ca_key_pem = extract_pem(&ca_identity, "PRIVATE KEY")?;
    let ca_key = KeyPair::from_pem(&ca_key_pem)
        .map_err(|error| Error::InvalidArgument(format!("invalid persisted CA key: {error}")))?;
    let issuer = Issuer::from_ca_cert_pem(&ca_certificate_pem, &ca_key).map_err(|error| {
        Error::InvalidArgument(format!("invalid persisted CA certificate: {error}"))
    })?;
    let leaf_key = KeyPair::generate()
        .map_err(|error| Error::Internal(format!("failed to generate server key: {error}")))?;
    let mut params = CertificateParams::new(vec![endpoint.certificate_host.clone()])
        .map_err(|error| Error::InvalidArgument(format!("invalid certificate SAN: {error}")))?;
    let not_before = now - Duration::days(1);
    params.not_before = rcgen::date_time_ymd(
        not_before.year(),
        not_before.month() as u8,
        not_before.day() as u8,
    );
    let not_after = now + Duration::days(LEAF_VALID_DAYS);
    params.not_after = rcgen::date_time_ymd(
        not_after.year(),
        not_after.month() as u8,
        not_after.day() as u8,
    );
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, endpoint.certificate_host.clone());
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let certificate = params
        .signed_by(&leaf_key, &issuer)
        .map_err(|error| Error::Internal(format!("failed to sign server certificate: {error}")))?;
    let identity = format!("{}{}", certificate.pem(), leaf_key.serialize_pem());
    let metadata = TlsMetadata {
        version: TLS_METADATA_VERSION,
        certificate_host: endpoint.certificate_host.clone(),
        leaf_not_after_unix: date_midnight(not_after)?.timestamp(),
    };
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)
        .map_err(|error| Error::Internal(format!("failed to encode TLS metadata: {error}")))?;
    atomic_write_secure(&state.server_identity_path(), identity.as_bytes())?;
    atomic_write_secure(&state.tls_metadata_path(), &metadata_bytes)?;
    Ok(metadata)
}

fn load_optional_metadata(state: &SecurityState) -> Result<Option<TlsMetadata>> {
    let path = state.tls_metadata_path();
    if !path
        .try_exists()
        .map_err(|error| Error::io(path.clone(), error))?
    {
        return Ok(None);
    }
    let bytes = read_secure_file(&path, MAX_METADATA_BYTES)?;
    let metadata = serde_json::from_slice(&bytes).map_err(|error| {
        Error::InvalidArgument(format!(
            "invalid TLS metadata at {}: {error}",
            path.display()
        ))
    })?;
    Ok(Some(metadata))
}

fn date_midnight(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    value
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .map(|value| value.and_utc())
        .ok_or_else(|| Error::Internal("invalid TLS certificate date".to_owned()))
}

fn extract_pem(encoded: &[u8], label: &str) -> Result<String> {
    let text = std::str::from_utf8(encoded)
        .map_err(|_| Error::InvalidArgument("TLS identity is not UTF-8 PEM".to_owned()))?;
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = text.find(&begin).ok_or_else(|| {
        Error::InvalidArgument(format!("TLS identity does not contain a {label} block"))
    })?;
    let end_offset = text[start..].find(&end).ok_or_else(|| {
        Error::InvalidArgument(format!("TLS identity contains an incomplete {label} block"))
    })?;
    let end = start + end_offset + end.len();
    Ok(format!("{}\n", &text[start..end]))
}

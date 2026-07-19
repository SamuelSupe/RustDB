use std::{fmt, path::Path};

use sha2::{Digest, Sha256};

use crate::{Error, Result};

use super::{
    files::{atomic_write_secure, read_secure_file},
    state::SecurityState,
};

const TOKEN_BYTES: usize = 32;
const TOKEN_HEX_BYTES: usize = TOKEN_BYTES * 2;
const MAX_TOKEN_FILE_BYTES: u64 = 128;

/// A bearer token verifier that never retains or prints the clear-text token.
#[derive(Clone)]
pub struct BearerToken {
    digest: [u8; 32],
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BearerToken([REDACTED])")
    }
}

impl BearerToken {
    /// Loads the token, creating a cryptographically random 256-bit token once.
    pub fn load_or_create(state: &SecurityState) -> Result<Self> {
        let path = state.token_path();
        if path
            .try_exists()
            .map_err(|error| Error::io(path.clone(), error))?
        {
            return Self::load(&path);
        }
        let encoded = generate_token()?;
        atomic_write_secure(&path, &encoded)?;
        Self::from_encoded(&encoded)
    }

    /// Loads a token from a permission-restricted token file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let token = read_token_text(path.as_ref())?;
        Ok(Self {
            digest: Sha256::digest(token.as_bytes()).into(),
        })
    }

    /// Rotates the token locally. Existing token verifiers become invalid once
    /// the caller swaps this returned verifier into the server state.
    pub fn rotate(state: &SecurityState) -> Result<Self> {
        let encoded = generate_token()?;
        atomic_write_secure(&state.token_path(), &encoded)?;
        Self::from_encoded(&encoded)
    }

    /// Verifies a presented bearer token without an early-exit comparison.
    pub fn verify(&self, presented: &str) -> bool {
        let actual: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        constant_time_equal(&self.digest, &actual)
    }

    fn from_encoded(encoded: &[u8]) -> Result<Self> {
        let token = parse_token(encoded)?;
        Ok(Self {
            digest: Sha256::digest(token.as_bytes()).into(),
        })
    }
}

pub(super) fn read_token_text(path: &Path) -> Result<String> {
    let encoded = read_secure_file(path, MAX_TOKEN_FILE_BYTES)?;
    parse_token(&encoded).map(str::to_owned)
}

fn generate_token() -> Result<Vec<u8>> {
    let mut random = [0_u8; TOKEN_BYTES];
    getrandom::fill(&mut random).map_err(|error| {
        Error::Internal(format!("operating system random source failed: {error}"))
    })?;
    let mut encoded = Vec::with_capacity(TOKEN_HEX_BYTES + 1);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in random {
        encoded.push(HEX[usize::from(byte >> 4)]);
        encoded.push(HEX[usize::from(byte & 0x0f)]);
    }
    encoded.push(b'\n');
    Ok(encoded)
}

fn parse_token(encoded: &[u8]) -> Result<&str> {
    let encoded = encoded.strip_suffix(b"\n").unwrap_or(encoded);
    if encoded.len() != TOKEN_HEX_BYTES
        || !encoded
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(Error::InvalidArgument(
            "bearer token file must contain exactly 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    std::str::from_utf8(encoded)
        .map_err(|_| Error::InvalidArgument("bearer token file is not UTF-8".to_owned()))
}

fn constant_time_equal(expected: &[u8; 32], actual: &[u8; 32]) -> bool {
    let mut difference = 0_u8;
    for (expected, actual) in expected.iter().zip(actual) {
        difference |= *expected ^ *actual;
    }
    difference == 0
}

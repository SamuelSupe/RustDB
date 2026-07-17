use std::{io::Write, path::Path};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{Error, Result};

pub(in crate::storage::native) fn encode_bounded<T: Serialize>(
    path: &Path,
    value: &T,
    max_bytes: usize,
    kind: &str,
    pretty: bool,
    trailing_newline: bool,
) -> Result<Vec<u8>> {
    let content_limit = max_bytes
        .checked_sub(usize::from(trailing_newline))
        .ok_or_else(|| limit_error(path, max_bytes, kind))?;
    let mut writer = BoundedBytes::new(content_limit);
    let encoded = if pretty {
        serde_json::to_writer_pretty(&mut writer, value)
    } else {
        serde_json::to_writer(&mut writer, value)
    };
    map_encoding_result(path, max_bytes, kind, writer.exceeded, encoded)?;
    if trailing_newline {
        writer.bytes.push(b'\n');
    }
    Ok(writer.bytes)
}

pub(in crate::storage::native) fn sha256_bounded<T: Serialize>(
    path: &Path,
    value: &T,
    max_bytes: usize,
    kind: &str,
) -> Result<String> {
    let mut writer = BoundedDigest::new(max_bytes);
    let encoded = serde_json::to_writer(&mut writer, value);
    map_encoding_result(path, max_bytes, kind, writer.exceeded, encoded)?;
    Ok(format!("{:x}", writer.digest.finalize()))
}

fn map_encoding_result(
    path: &Path,
    max_bytes: usize,
    kind: &str,
    exceeded: bool,
    result: serde_json::Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(_) if exceeded => Err(limit_error(path, max_bytes, kind)),
        Err(error) => Err(Error::native_storage(
            path,
            format!("could not encode {kind}: {error}"),
        )),
    }
}

fn limit_error(path: &Path, max_bytes: usize, kind: &str) -> Error {
    Error::native_storage(path, format!("{kind} exceeds the {max_bytes}-byte limit"))
}

struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedBytes {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "bounded JSON output exceeded its limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct BoundedDigest {
    digest: Sha256,
    written: usize,
    limit: usize,
    exceeded: bool,
}

impl BoundedDigest {
    fn new(limit: usize) -> Self {
        Self {
            digest: Sha256::new(),
            written: 0,
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedDigest {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.written) {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "bounded JSON digest exceeded its limit",
            ));
        }
        self.digest.update(bytes);
        self.written += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

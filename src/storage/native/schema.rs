use std::{path::Path, sync::Arc};

use arrow::{
    datatypes::{Schema, SchemaRef},
    ipc::{
        convert::try_schema_from_flatbuffer_bytes,
        writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

// Arrow IPC schemas are normally measured in KiB. Eight MiB leaves ample room
// for very wide tables while preventing corrupt hex from driving large decode
// allocations.
const MAX_SCHEMA_BYTES: usize = 8 * 1024 * 1024;

pub(super) fn encode(schema: &Schema) -> Vec<u8> {
    let mut dictionaries = DictionaryTracker::new(false);
    IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut dictionaries,
            &IpcWriteOptions::default(),
        )
        .ipc_message
}

pub(super) fn decode(path: &Path, encoded: &str, expected_sha256: &str) -> Result<SchemaRef> {
    let bytes = decode_hex(path, encoded)?;
    let actual = sha256(&bytes);
    if actual != expected_sha256 {
        return Err(Error::native_storage(
            path,
            "stored schema checksum mismatch",
        ));
    }
    try_schema_from_flatbuffer_bytes(&bytes)
        .map(Arc::new)
        .map_err(|error| Error::native_storage(path, format!("invalid stored schema: {error}")))
}

pub(super) fn validate_encoded_size(path: &Path, len: usize) -> Result<()> {
    if len > MAX_SCHEMA_BYTES {
        return Err(Error::native_storage(
            path,
            format!("stored schema exceeds the {MAX_SCHEMA_BYTES}-byte limit ({len} bytes)"),
        ));
    }
    Ok(())
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

pub(super) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn decode_hex(path: &Path, encoded: &str) -> Result<Vec<u8>> {
    validate_hex_size(path, encoded.len())?;
    if !encoded.len().is_multiple_of(2) {
        return Err(Error::native_storage(
            path,
            "stored schema hex has odd length",
        ));
    }

    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0]);
            let low = hex_digit(pair[1]);
            match (high, low) {
                (Some(high), Some(low)) => Ok((high << 4) | low),
                _ => Err(Error::native_storage(
                    path,
                    "stored schema contains non-hex data",
                )),
            }
        })
        .collect()
}

fn validate_hex_size(path: &Path, len: usize) -> Result<()> {
    let max_hex_bytes = MAX_SCHEMA_BYTES * 2;
    if len > max_hex_bytes {
        return Err(Error::native_storage(
            path,
            format!("stored schema hex exceeds the {max_hex_bytes}-byte limit ({len} bytes)"),
        ));
    }
    Ok(())
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{
        MAX_SCHEMA_BYTES, decode, encode, encode_hex, sha256, validate_encoded_size,
        validate_hex_size,
    };
    use crate::Error;

    #[test]
    fn round_trips_an_arrow_schema() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let bytes = encode(&schema);
        let decoded = decode(
            std::path::Path::new("schema"),
            &encode_hex(&bytes),
            &sha256(&bytes),
        )
        .unwrap();
        assert_eq!(decoded.as_ref(), &schema);
    }

    #[test]
    fn rejects_oversized_stored_schemas_before_allocating_the_decode_buffer() {
        let path = std::path::Path::new("schema");
        assert!(matches!(
            validate_encoded_size(path, MAX_SCHEMA_BYTES + 1),
            Err(Error::NativeStorage { .. })
        ));
        assert!(matches!(
            validate_hex_size(path, MAX_SCHEMA_BYTES * 2 + 1),
            Err(Error::NativeStorage { .. })
        ));
    }
}

use std::{
    collections::HashSet,
    fs::{self, File},
    io::Read,
    path::{Component, Path},
};

use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceFile {
    pub(crate) relative_path: String,
    pub(crate) location: String,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

pub(crate) fn validate_manifest(
    files: &[SourceFile],
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<(), String> {
    if files.is_empty() {
        return Err("native setup source file list is empty".to_owned());
    }
    let mut relative_paths = HashSet::with_capacity(files.len());
    let mut locations = HashSet::with_capacity(files.len());
    let mut roots = HashSet::with_capacity(files.len());
    let mut ordered = Vec::with_capacity(files.len());
    let mut total_bytes = 0_u64;
    for file in files {
        validate_entry(file)?;
        if !relative_paths.insert(file.relative_path.as_str()) {
            return Err("native setup source paths must be unique".to_owned());
        }
        if !locations.insert(file.location.as_str()) {
            return Err("native setup source locations must be unique".to_owned());
        }
        let suffix = format!("/{}", file.relative_path);
        let root = file
            .location
            .strip_suffix(&suffix)
            .ok_or_else(|| "native setup source location does not match its path".to_owned())?;
        roots.insert(root);
        total_bytes = total_bytes
            .checked_add(file.bytes)
            .ok_or_else(|| "native setup source size overflowed".to_owned())?;
        ordered.push(file);
    }
    if roots.len() != 1 {
        return Err("native setup source files must share one root".to_owned());
    }
    if total_bytes != expected_bytes {
        return Err("native setup source byte count does not match".to_owned());
    }
    ordered.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let mut aggregate = Sha256::new();
    for file in ordered {
        let path = file.relative_path.as_bytes();
        aggregate.update((path.len() as u64).to_le_bytes());
        aggregate.update(path);
        aggregate.update(file.bytes.to_le_bytes());
        aggregate.update(decode_sha256(&file.sha256)?);
    }
    if format!("{:x}", aggregate.finalize()) != expected_sha256 {
        return Err("native setup source hash does not match".to_owned());
    }
    Ok(())
}

pub(crate) fn verify_files(files: &[SourceFile]) -> Result<(), String> {
    let mut canonical = HashSet::with_capacity(files.len());
    for (index, source) in files.iter().enumerate() {
        let path = fs::canonicalize(&source.location)
            .map_err(|_| format!("native setup source file {} failed verification", index + 1))?;
        if !canonical.insert(path) {
            return Err("native setup source locations must resolve to unique files".to_owned());
        }
        verify_file(source)
            .map_err(|_| format!("native setup source file {} failed verification", index + 1))?;
    }
    Ok(())
}

fn validate_entry(file: &SourceFile) -> Result<(), String> {
    if file.relative_path.is_empty()
        || file.relative_path.contains('\0')
        || file.relative_path.contains('\\')
        || has_glob(&file.relative_path)
    {
        return Err("native setup source relative path is invalid".to_owned());
    }
    let relative = Path::new(&file.relative_path);
    if relative.is_absolute()
        || !relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err("native setup source relative path escapes its root".to_owned());
    }
    if file.location.is_empty()
        || file.location.contains('\0')
        || file.location.contains('\\')
        || file.location.contains("://")
        || has_glob(&file.location)
        || !Path::new(&file.location).is_absolute()
    {
        return Err("native setup source location must be one absolute local file".to_owned());
    }
    if file.bytes == 0 {
        return Err("native setup source file size must be positive".to_owned());
    }
    decode_sha256(&file.sha256)?;
    Ok(())
}

fn verify_file(source: &SourceFile) -> Result<(), String> {
    let mut file = File::open(&source.location).map_err(|_| "cannot open source".to_owned())?;
    let before = file
        .metadata()
        .map_err(|_| "cannot inspect source".to_owned())?;
    if !before.is_file() || before.len() != source.bytes {
        return Err("source metadata changed".to_owned());
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| "cannot read source".to_owned())?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file
        .metadata()
        .map_err(|_| "cannot inspect source".to_owned())?;
    let actual: [u8; 32] = hasher.finalize().into();
    if after.len() != source.bytes || actual != decode_sha256(&source.sha256)? {
        return Err("source contents changed".to_owned());
    }
    Ok(())
}

fn has_glob(value: &str) -> bool {
    value
        .bytes()
        .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b']'))
}

fn decode_sha256(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64 {
        return Err("native setup source hash must be lowercase SHA-256".to_owned());
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = hex_digit(pair[0])? * 16 + hex_digit(pair[1])?;
    }
    Ok(decoded)
}

fn hex_digit(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("native setup source hash must be lowercase SHA-256".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{SourceFile, validate_manifest, verify_files};

    #[test]
    fn validates_aggregate_and_file_content() {
        let temp = tempfile::tempdir().unwrap();
        let location = temp.path().join("data/t.parquet");
        std::fs::create_dir_all(location.parent().unwrap()).unwrap();
        std::fs::write(&location, b"abc").unwrap();
        let files = vec![SourceFile {
            relative_path: "data/t.parquet".to_owned(),
            location: location.to_string_lossy().into_owned(),
            bytes: 3,
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned(),
        }];
        validate_manifest(
            &files,
            3,
            "8a4f62878ec3b580c780d8709b855b320720a8c3bea1175aae44f6977b405567",
        )
        .unwrap();
        verify_files(&files).unwrap();
    }

    #[test]
    fn rejects_escape_duplicate_and_glob_paths() {
        for relative_path in ["../x", "a/../x", "*.parquet", "a\\b"] {
            let files = vec![SourceFile {
                relative_path: relative_path.to_owned(),
                location: "/tmp/source.parquet".to_owned(),
                bytes: 1,
                sha256: "a".repeat(64),
            }];
            assert!(validate_manifest(&files, 1, &"b".repeat(64)).is_err());
        }
        let files = vec![
            SourceFile {
                relative_path: "a.parquet".to_owned(),
                location: "/tmp/source.parquet".to_owned(),
                bytes: 1,
                sha256: "a".repeat(64),
            },
            SourceFile {
                relative_path: "b.parquet".to_owned(),
                location: "/tmp/source.parquet".to_owned(),
                bytes: 1,
                sha256: "b".repeat(64),
            },
        ];
        assert!(validate_manifest(&files, 2, &"c".repeat(64)).is_err());
    }
}

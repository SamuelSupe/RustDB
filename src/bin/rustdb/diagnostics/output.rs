use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
};

use rustdb::{Error, Result};
use uuid::Uuid;

pub(super) fn emit(encoded: &[u8], output: Option<&Path>) -> Result<()> {
    match output {
        Some(path) => atomic_write_private(path, encoded),
        None => {
            let mut stdout = io::stdout().lock();
            stdout
                .write_all(encoded)
                .and_then(|_| stdout.flush())
                .map_err(|error| Error::io(None, error))
        }
    }
}

fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_metadata =
        fs::symlink_metadata(parent).map_err(|error| Error::io(parent.to_path_buf(), error))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(Error::InvalidArgument(format!(
            "diagnostics output parent is not a non-symlink directory: {}",
            parent.display()
        )));
    }
    if let Ok(metadata) = fs::symlink_metadata(path)
        && (metadata.file_type().is_symlink() || !metadata.is_file())
    {
        return Err(Error::InvalidArgument(format!(
            "refusing to replace unsafe diagnostics output {}",
            path.display()
        )));
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Error::InvalidArgument("diagnostics output must have a valid file name".into())
        })?;
    let temporary = parent.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        set_private_mode(&mut options);
        let mut file = options
            .open(&temporary)
            .map_err(|error| Error::io(temporary.clone(), error))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| Error::io(temporary.clone(), error))?;
        fs::rename(&temporary, path).map_err(|error| Error::io(path.to_path_buf(), error))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| Error::io(parent.to_path_buf(), error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn set_private_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_mode(_options: &mut OpenOptions) {}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::atomic_write_private;

    #[test]
    fn output_is_replaced_atomically_with_private_permissions() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("diagnostics.json");
        atomic_write_private(&path, b"{\"schema_version\":1}\n").unwrap();
        atomic_write_private(&path, b"{\"schema_version\":1,\"new\":true}\n").unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "{\"schema_version\":1,\"new\":true}\n"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o077, 0);
        }
    }
}

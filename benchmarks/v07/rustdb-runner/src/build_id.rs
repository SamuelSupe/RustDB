use std::{
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

pub(crate) fn verify(expected: &str) -> Result<(), String> {
    let path = executable_path()?;
    let actual = hash_path(&path)?;
    if actual != expected {
        return Err(format!(
            "build-id does not match running executable: expected {expected}, actual {actual}"
        ));
    }
    Ok(())
}

fn executable_path() -> Result<PathBuf, String> {
    #[cfg(target_os = "linux")]
    {
        // Opening this procfs link hashes the executable inode that created
        // this process, even if its pathname is replaced after startup.
        Ok(PathBuf::from("/proc/self/exe"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::env::current_exe()
            .map_err(|error| format!("cannot locate running executable: {error}"))
    }
}

fn hash_path(path: &Path) -> Result<String, String> {
    let file = File::open(path)
        .map_err(|error| format!("cannot open running executable {}: {error}", path.display()))?;
    hash_reader(BufReader::new(file))
        .map_err(|error| format!("cannot hash running executable {}: {error}", path.display()))
}

fn hash_reader(mut reader: impl Read) -> std::io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 << 10];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => hasher.update(&buffer[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Error, ErrorKind, Read};

    use super::{hash_reader, verify};

    #[test]
    fn hashes_streamed_bytes() {
        assert_eq!(
            hash_reader(Cursor::new(b"abc")).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn retries_interrupted_reads() {
        struct InterruptedOnce {
            interrupted: bool,
            bytes: Cursor<&'static [u8]>,
        }

        impl Read for InterruptedOnce {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(Error::new(ErrorKind::Interrupted, "retry"));
                }
                self.bytes.read(buffer)
            }
        }

        let reader = InterruptedOnce {
            interrupted: false,
            bytes: Cursor::new(b"abc"),
        };
        assert_eq!(
            hash_reader(reader).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn accepts_the_running_executable_hash() {
        let actual = super::hash_path(&super::executable_path().unwrap()).unwrap();
        verify(&actual).unwrap();
    }
}

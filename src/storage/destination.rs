use std::{path::PathBuf, sync::Arc};

use object_store::{ObjectStore, path::Path};
use url::Url;

use crate::{Error, Result, S3Config};

#[derive(Clone)]
pub(crate) enum WriteDestination {
    Local(PathBuf),
    S3 {
        uri: String,
        store: Arc<dyn ObjectStore>,
        location: Path,
    },
}

impl WriteDestination {
    pub(crate) fn resolve(location: &str, s3: &S3Config) -> Result<Self> {
        if location.starts_with("s3://") {
            return resolve_s3(location, s3);
        }
        resolve_local(location).map(Self::Local)
    }
}

fn resolve_s3(location: &str, s3: &S3Config) -> Result<WriteDestination> {
    let url = super::s3_uri::S3Uri::parse(location)?;
    let key = url.path().trim_start_matches('/');
    if key.is_empty() || has_glob(key) || key.ends_with('/') {
        return Err(Error::InvalidArgument(
            "COPY destination must be one concrete S3 object".to_owned(),
        ));
    }
    let location_path = Path::from_url_path(key)
        .map_err(|error| Error::InvalidArgument(format!("invalid S3 key: {error}")))?;
    let resolver = super::LocationResolver::with_memory_limit(s3.clone(), 64 * 1024 * 1024);
    let store = resolver.build_s3_store(url.bucket())?;
    Ok(WriteDestination::S3 {
        uri: location.to_owned(),
        store,
        location: location_path,
    })
}

fn resolve_local(location: &str) -> Result<PathBuf> {
    let path = if location.starts_with("file://") {
        let url = Url::parse(location)
            .map_err(|_| Error::InvalidArgument("invalid file URI".to_owned()))?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(Error::InvalidArgument(
                "file URI must not contain user information".to_owned(),
            ));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(Error::InvalidArgument(
                "file URI must not contain a query or fragment".to_owned(),
            ));
        }
        url.to_file_path().map_err(|()| {
            Error::InvalidArgument(format!("file URI is not a local path: {location}"))
        })?
    } else {
        let path = PathBuf::from(location);
        if path.is_absolute() {
            path
        } else {
            std::env::current_dir()
                .map_err(|error| Error::io(None, error))?
                .join(path)
        }
    };
    if has_glob(&path.to_string_lossy()) || path.file_name().is_none() {
        return Err(Error::InvalidArgument(format!(
            "COPY destination must be one concrete local file: {}",
            path.display()
        )));
    }
    let parent = path.parent().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "COPY destination has no parent: {}",
            path.display()
        ))
    })?;
    if !parent.is_dir() {
        return Err(Error::InvalidArgument(format!(
            "COPY destination directory does not exist: {}",
            parent.display()
        )));
    }
    Ok(path)
}

fn has_glob(value: &str) -> bool {
    value.bytes().any(|byte| matches!(byte, b'*' | b'?' | b'['))
}

#[cfg(test)]
mod tests {
    use super::{WriteDestination, resolve_local};

    #[test]
    fn local_destination_requires_an_existing_parent_and_no_glob() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("out.parquet");
        assert!(matches!(
            WriteDestination::resolve(
                destination.to_str().unwrap(),
                &crate::S3Config::default()
            )
            .unwrap(),
            WriteDestination::Local(path) if path == destination
        ));
        assert!(resolve_local("/missing-rustdb-parent/out.csv").is_err());
        assert!(resolve_local(&format!("{}/*.csv", directory.path().display())).is_err());
    }

    #[test]
    fn file_uri_rejects_secrets_without_echoing_them() {
        for (uri, secret) in [
            (
                "file://alice:password-secret@localhost/tmp/out.csv",
                "password-secret",
            ),
            ("file:///tmp/out.csv?token=query-secret", "query-secret"),
            ("file:///tmp/out.csv#fragment-secret", "fragment-secret"),
        ] {
            let error = resolve_local(uri).unwrap_err().to_string();
            assert!(!error.contains(secret), "{error}");
            assert!(!error.contains(uri), "{error}");
        }
    }
}

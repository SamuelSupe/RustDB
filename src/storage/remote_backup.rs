use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use futures::StreamExt;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::{Error, Result, S3Config};

const FORMAT_VERSION: u32 = 1;
const MANIFEST_NAME: &str = "manifest.json";
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
const UPLOAD_PART_BYTES: usize = 8 << 20;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format_version: u32,
    backup_id: String,
    directories: Vec<String>,
    files: Vec<FileEntry>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FileEntry {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    manifest: Manifest,
    sha256: String,
}

#[derive(Serialize)]
struct EnvelopeRef<'a> {
    manifest: &'a Manifest,
    sha256: &'a str,
}

pub(crate) struct DownloadedBackup {
    temporary: super::RemoteTempDir,
    snapshot: PathBuf,
}

impl DownloadedBackup {
    pub(crate) fn snapshot(&self) -> &Path {
        &self.snapshot
    }

    pub(crate) fn finish<T>(self, result: Result<T>) -> Result<T> {
        self.temporary.finish(result)
    }
}

pub(crate) async fn upload(root: &Path, uri: &str, s3: &S3Config) -> Result<()> {
    let target = RemoteTarget::parse(uri, s3)?;
    upload_to_target(root, &target, uri).await
}

async fn upload_to_target(root: &Path, target: &RemoteTarget, uri: &str) -> Result<()> {
    let manifest_path = target.child(MANIFEST_NAME)?;
    match target.store.head(&manifest_path).await {
        Ok(_) => {
            return Err(Error::InvalidArgument(
                "remote backup destination already exists".to_owned(),
            ));
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => return Err(error.into()),
    }
    let destination_prefix = ObjectPath::parse(&target.prefix).map_err(|error| {
        Error::InvalidArgument(format!("invalid remote backup object prefix: {error}"))
    })?;
    let mut existing = target.store.list(Some(&destination_prefix));
    while let Some(object) = existing.next().await {
        let object = object?;
        let location = object.location.as_ref();
        if location == target.prefix.as_str()
            || location
                .strip_prefix(&target.prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return Err(Error::InvalidArgument(format!(
                "remote backup destination already contains object '{}'; refusing to add another backup without deleting existing data",
                object.location
            )));
        }
    }
    let (directories, files) = walk(root)?;
    let backup_id = Uuid::new_v4().to_string();
    let mut uploaded = Vec::new();
    let mut entries = Vec::with_capacity(files.len());
    for (relative, source) in files {
        let object = match target.child(&format!("data/{backup_id}/{relative}")) {
            Ok(object) => object,
            Err(error) => return cleanup_error(&target.store, &uploaded, error).await,
        };
        // The multipart completion response can be lost after the object was
        // committed. Track the key before starting so failure cleanup also
        // covers that uncertain final object.
        uploaded.push(object.clone());
        let entry = match upload_file(&target.store, &object, &source, &relative).await {
            Ok(entry) => entry,
            Err(error) => return cleanup_error(&target.store, &uploaded, error).await,
        };
        entries.push(entry);
    }
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        backup_id: backup_id.clone(),
        directories,
        files: entries,
    };
    let payload = match encode_manifest(&manifest) {
        Ok(payload) => payload,
        Err(error) => return cleanup_error(&target.store, &uploaded, error).await,
    };
    let options = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    if let Err(error) = target
        .store
        .put_opts(&manifest_path, PutPayload::from(payload.clone()), options)
        .await
    {
        let state = super::inspect_remote_manifest(&target.store, &manifest_path, &payload).await;
        if state == super::PublicationState::Matches {
            return Ok(());
        }
        if super::is_definitive_remote_rejection(&error) {
            return cleanup_error(&target.store, &uploaded, error.into()).await;
        }
        return Err(Error::commit_outcome_unknown(
            PathBuf::from(uri),
            format!("remote-backup-{backup_id}"),
            format!(
                "backup manifest publication failed and could not be reconciled ({state:?}); uploaded data was retained: {error}"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn upload_to_store(
    root: &Path,
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<()> {
    let target = RemoteTarget {
        store,
        prefix: prefix.trim_matches('/').to_owned(),
    };
    upload_to_target(root, &target, "memory://remote-backup-test").await
}

pub(crate) async fn download(
    uri: &str,
    temporary_parent: &Path,
    s3: &S3Config,
) -> Result<DownloadedBackup> {
    let target = RemoteTarget::parse(uri, s3)?;
    let response = target.store.get(&target.child(MANIFEST_NAME)?).await?;
    if response.meta.size > MAX_MANIFEST_BYTES as u64 {
        return Err(Error::ResourceExhausted(format!(
            "remote backup manifest exceeds {MAX_MANIFEST_BYTES} bytes"
        )));
    }
    let manifest = decode_manifest(&response.bytes().await?)?;
    let temporary = super::RemoteTempDir::create(temporary_parent, super::RemoteTempKind::Restore)?;
    let snapshot = temporary.path().join("snapshot");
    let result = async {
        create_private_directory(&snapshot)?;
        for directory in &manifest.directories {
            create_private_directory(&safe_join(&snapshot, directory)?)?;
        }
        for file in &manifest.files {
            let source = target.child(&format!("data/{}/{}", manifest.backup_id, file.path))?;
            let destination = safe_join(&snapshot, &file.path)?;
            download_file(&target.store, &source, &destination, file).await?;
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(DownloadedBackup {
            temporary,
            snapshot,
        }),
        Err(error) => temporary.finish(Err(error)),
    }
}

struct RemoteTarget {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl RemoteTarget {
    fn parse(uri: &str, s3: &S3Config) -> Result<Self> {
        let url = super::s3_uri::S3Uri::parse(uri)?;
        let prefix = url.path().trim_matches('/').to_owned();
        if prefix.is_empty() || has_glob(&prefix) {
            return Err(Error::InvalidArgument(
                "remote backup location must contain one concrete object prefix".to_owned(),
            ));
        }
        let resolver = super::LocationResolver::with_memory_limit(s3.clone(), 64 * 1024 * 1024);
        Ok(Self {
            store: resolver.build_s3_store(url.bucket())?,
            prefix,
        })
    }

    fn child(&self, suffix: &str) -> Result<ObjectPath> {
        ObjectPath::parse(format!("{}/{}", self.prefix, suffix)).map_err(|error| {
            Error::InvalidArgument(format!("invalid remote backup object path: {error}"))
        })
    }
}

async fn upload_file(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectPath,
    source: &Path,
    relative: &str,
) -> Result<FileEntry> {
    let upload = store.put_multipart(object).await?;
    upload_file_with_upload(upload, source, relative).await
}

async fn upload_file_with_upload(
    mut upload: Box<dyn MultipartUpload>,
    source: &Path,
    relative: &str,
) -> Result<FileEntry> {
    let result = upload_file_parts(upload.as_mut(), source).await;
    let (bytes, sha256) = match result {
        Ok(result) => result,
        Err(error) => return Err(abort_upload(upload.as_mut(), error).await),
    };
    if let Err(error) = upload.complete().await {
        return Err(abort_upload(upload.as_mut(), error.into()).await);
    }
    Ok(FileEntry {
        path: relative.to_owned(),
        bytes,
        sha256,
    })
}

async fn upload_file_parts(
    upload: &mut dyn MultipartUpload,
    source: &Path,
) -> Result<(u64, String)> {
    let mut input = tokio::fs::File::open(source)
        .await
        .map_err(|error| Error::io(Some(source.to_path_buf()), error))?;
    let mut buffer = vec![0_u8; UPLOAD_PART_BYTES];
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    loop {
        let mut filled = 0;
        while filled < buffer.len() {
            let read = input
                .read(&mut buffer[filled..])
                .await
                .map_err(|error| Error::io(Some(source.to_path_buf()), error))?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled == 0 {
            break;
        }
        hasher.update(&buffer[..filled]);
        bytes = bytes
            .checked_add(u64::try_from(filled).unwrap_or(u64::MAX))
            .ok_or_else(|| Error::ResourceExhausted("backup byte count overflow".to_owned()))?;
        upload
            .put_part(PutPayload::from(Bytes::copy_from_slice(&buffer[..filled])))
            .await?;
        if filled < buffer.len() {
            break;
        }
    }
    Ok((bytes, format!("{:x}", hasher.finalize())))
}

async fn abort_upload(upload: &mut dyn MultipartUpload, error: Error) -> Error {
    match upload.abort().await {
        Ok(()) => error,
        Err(cleanup) => Error::Execution(format!(
            "{error}; remote backup multipart cleanup also failed: {cleanup}"
        )),
    }
}

async fn download_file(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectPath,
    destination: &Path,
    expected: &FileEntry,
) -> Result<()> {
    if let Some(parent) = destination.parent() {
        create_private_directory(parent)?;
    }
    let response = store.get(object).await?;
    if response.meta.size != expected.bytes {
        return Err(Error::Execution(format!(
            "remote backup object '{}' has size {}, expected {}",
            expected.path, response.meta.size, expected.bytes
        )));
    }
    let mut output = create_private_file(destination).await?;
    let mut stream = response.into_stream();
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        hasher.update(&chunk);
        bytes = bytes.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        output
            .write_all(&chunk)
            .await
            .map_err(|error| Error::io(Some(destination.to_path_buf()), error))?;
    }
    output
        .sync_all()
        .await
        .map_err(|error| Error::io(Some(destination.to_path_buf()), error))?;
    if bytes != expected.bytes || format!("{:x}", hasher.finalize()) != expected.sha256 {
        return Err(Error::Execution(format!(
            "remote backup object '{}' failed checksum validation",
            expected.path
        )));
    }
    Ok(())
}

type BackupFile = (String, PathBuf);
type BackupTree = (Vec<String>, Vec<BackupFile>);

fn walk(root: &Path) -> Result<BackupTree> {
    let mut directories = Vec::new();
    let mut files = Vec::new();
    walk_directory(root, root, &mut directories, &mut files)?;
    directories.sort();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok((directories, files))
}

fn walk_directory(
    root: &Path,
    directory: &Path,
    directories: &mut Vec<String>,
    files: &mut Vec<(String, PathBuf)>,
) -> Result<()> {
    for entry in std::fs::read_dir(directory)
        .map_err(|error| Error::io(Some(directory.to_path_buf()), error))?
    {
        let path = entry
            .map_err(|error| Error::io(Some(directory.to_path_buf()), error))?
            .path();
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|error| Error::io(Some(path.clone()), error))?;
        if metadata.file_type().is_symlink() {
            return Err(Error::Execution(format!(
                "backup contains a symbolic link: {}",
                path.display()
            )));
        }
        let relative = relative_path(root, &path)?;
        if metadata.is_dir() {
            directories.push(relative.clone());
            walk_directory(root, &path, directories, files)?;
        } else if metadata.is_file() {
            files.push((relative, path));
        } else {
            return Err(Error::Execution(format!(
                "backup contains a non-regular file: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| Error::Internal("backup path escaped its root".to_owned()))?;
    let value = relative.to_str().ok_or_else(|| {
        Error::InvalidArgument("remote backup paths must be valid UTF-8".to_owned())
    })?;
    if value.is_empty() || value.contains('\\') {
        return Err(Error::InvalidArgument(
            "remote backup contains an invalid relative path".to_owned(),
        ));
    }
    Ok(value.to_owned())
}

fn safe_join(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if relative.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(Error::Execution(format!(
            "remote backup manifest contains unsafe path '{relative}'"
        )));
    }
    Ok(root.join(path))
}

fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>> {
    let manifest_bytes = serde_json::to_vec(manifest)
        .map_err(|error| Error::Internal(format!("cannot encode backup manifest: {error}")))?;
    let sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));
    let bytes = serde_json::to_vec_pretty(&EnvelopeRef {
        manifest,
        sha256: &sha256,
    })
    .map_err(|error| Error::Internal(format!("cannot encode backup envelope: {error}")))?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "remote backup manifest exceeds {MAX_MANIFEST_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

fn decode_manifest(bytes: &[u8]) -> Result<Manifest> {
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "remote backup manifest exceeds {MAX_MANIFEST_BYTES} bytes"
        )));
    }
    let envelope: Envelope = serde_json::from_slice(bytes)
        .map_err(|error| Error::Execution(format!("invalid remote backup manifest: {error}")))?;
    if envelope.manifest.format_version != FORMAT_VERSION
        || Uuid::parse_str(&envelope.manifest.backup_id).is_err()
    {
        return Err(Error::Execution(
            "unsupported or invalid remote backup manifest".to_owned(),
        ));
    }
    let encoded = serde_json::to_vec(&envelope.manifest)
        .map_err(|error| Error::Internal(format!("cannot validate backup manifest: {error}")))?;
    if format!("{:x}", Sha256::digest(&encoded)) != envelope.sha256 {
        return Err(Error::Execution(
            "remote backup manifest checksum mismatch".to_owned(),
        ));
    }
    let mut paths = std::collections::HashSet::new();
    for directory in &envelope.manifest.directories {
        safe_join(Path::new("root"), directory)?;
    }
    for file in &envelope.manifest.files {
        safe_join(Path::new("root"), &file.path)?;
        if !paths.insert(file.path.clone()) || file.sha256.len() != 64 {
            return Err(Error::Execution(
                "remote backup manifest contains invalid or duplicate files".to_owned(),
            ));
        }
    }
    Ok(envelope.manifest)
}

async fn cleanup_error<T>(
    store: &Arc<dyn ObjectStore>,
    uploaded: &[ObjectPath],
    error: Error,
) -> Result<T> {
    let mut cleanup_failure = None;
    for object in uploaded.iter().rev() {
        if let Err(cleanup) = store.delete(object).await
            && !matches!(cleanup, object_store::Error::NotFound { .. })
        {
            cleanup_failure = Some(cleanup);
        }
    }
    match cleanup_failure {
        Some(cleanup) => Err(Error::Execution(format!(
            "{error}; remote backup cleanup also failed: {cleanup}"
        ))),
        None => Err(error),
    }
}

fn create_private_directory(path: &Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

async fn create_private_file(path: &Path) -> Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    options
        .open(path)
        .await
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

fn has_glob(value: &str) -> bool {
    value.bytes().any(|byte| matches!(byte, b'*' | b'?' | b'['))
}

#[cfg(test)]
mod tests;

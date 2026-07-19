use std::{
    collections::HashMap,
    fmt,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use futures::{StreamExt, TryStreamExt};
use glob::{MatchOptions, Pattern, glob};
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, local::LocalFileSystem, path::Path,
};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{Error, Result, S3Config, runtime::QueryContext, storage::LocalFileIdentity};

mod endpoint;
mod source_list;

use endpoint::builder_from_env;
pub(crate) use endpoint::validate_endpoint;
use source_list::SourceList;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectSnapshot {
    pub size: u64,
    pub e_tag: Option<String>,
    pub version: Option<String>,
    pub(crate) local_identity: Option<LocalFileIdentity>,
}

impl From<&ObjectMeta> for ObjectSnapshot {
    fn from(meta: &ObjectMeta) -> Self {
        Self {
            size: meta.size,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
            local_identity: None,
        }
    }
}

impl ObjectSnapshot {
    /// Returns whether `actual` only adds identity tokens that were absent
    /// from this unsealed snapshot. Existing opaque tokens are never
    /// normalized or replaced.
    pub(crate) fn can_refine_to(&self, actual: &Self) -> bool {
        self.size == actual.size
            && identity_refines(&self.e_tag, &actual.e_tag)
            && identity_refines(&self.version, &actual.version)
            && identity_refines(&self.local_identity, &actual.local_identity)
    }

    /// Verifies that an object GET still refers to the identity captured for
    /// this query. Some S3-compatible stores omit ETag and version values, so
    /// size remains a required fallback identity check.
    pub(crate) fn validate_get_response(&self, uri: &str, meta: &ObjectMeta) -> Result<()> {
        let actual = Self::from(meta);
        self.validate_wire_snapshot(uri, &actual)
    }

    pub(crate) fn validate_snapshot(&self, uri: &str, actual: &Self) -> Result<()> {
        self.validate_wire_snapshot(uri, actual)?;
        let local_identity_changed = self
            .local_identity
            .is_some_and(|expected| actual.local_identity != Some(expected));
        if local_identity_changed {
            return Err(self.changed_error(uri, actual));
        }
        Ok(())
    }

    fn validate_wire_snapshot(&self, uri: &str, actual: &Self) -> Result<()> {
        let e_tag_changed = self
            .e_tag
            .as_deref()
            .is_some_and(|expected| actual.e_tag.as_deref() != Some(expected));
        let version_changed = self
            .version
            .as_deref()
            .is_some_and(|expected| actual.version.as_deref() != Some(expected));
        let size_changed = self.size != actual.size;
        if size_changed || e_tag_changed || version_changed {
            return Err(self.changed_error(uri, actual));
        }
        Ok(())
    }

    fn changed_error(&self, uri: &str, actual: &Self) -> Error {
        Error::Execution(format!(
            "object changed during query: {uri}: expected size {}, ETag {:?}, version {:?}, \
             local identity {:?}; read returned size {}, ETag {:?}, version {:?}, local \
             identity {:?}",
            self.size,
            self.e_tag,
            self.version,
            self.local_identity,
            actual.size,
            actual.e_tag,
            actual.version,
            actual.local_identity,
        ))
    }
}

fn identity_refines<T: PartialEq>(existing: &Option<T>, actual: &Option<T>) -> bool {
    match (existing, actual) {
        (Some(existing), Some(actual)) => existing == actual,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

#[derive(Clone)]
pub struct ObjectSource {
    uri: String,
    store: Arc<dyn ObjectStore>,
    location: Path,
    snapshot: ObjectSnapshot,
    s3: bool,
    local_path: Option<PathBuf>,
}

impl ObjectSource {
    fn new(
        uri: String,
        store: Arc<dyn ObjectStore>,
        meta: ObjectMeta,
        s3: bool,
        local_path: Option<PathBuf>,
    ) -> Self {
        Self {
            uri,
            store,
            location: meta.location.clone(),
            snapshot: ObjectSnapshot::from(&meta),
            s3,
            local_path,
        }
    }

    fn new_local(
        uri: String,
        store: Arc<dyn ObjectStore>,
        meta: ObjectMeta,
        local_path: PathBuf,
    ) -> Result<Self> {
        let metadata = std::fs::metadata(&local_path)
            .map_err(|error| Error::io(Some(local_path.clone()), error))?;
        if meta.size != metadata.len() {
            return Err(Error::Execution(format!(
                "object changed while its identity was captured: {uri}: object store reported \
                 size {}, local metadata reported size {}",
                meta.size,
                metadata.len()
            )));
        }
        let mut source = Self::new(uri, store, meta, false, Some(local_path));
        source.snapshot.local_identity = LocalFileIdentity::from_metadata(&metadata);
        Ok(source)
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn location(&self) -> &Path {
        &self.location
    }

    pub fn snapshot(&self) -> &ObjectSnapshot {
        &self.snapshot
    }

    pub fn is_s3(&self) -> bool {
        self.s3
    }

    pub(crate) fn local_path(&self) -> Option<&FsPath> {
        self.local_path.as_deref()
    }

    pub fn get_options_for(&self, snapshot: &ObjectSnapshot) -> GetOptions {
        GetOptions {
            if_match: snapshot.e_tag.clone(),
            version: snapshot.version.clone(),
            ..GetOptions::default()
        }
    }

    /// Reads a fresh object identity for a new query.
    ///
    /// The snapshot retained by this source describes registration time and is
    /// useful for schema discovery. Query execution must call this method so a
    /// file may legitimately change between two queries while still remaining
    /// immutable for the duration of either query.
    pub async fn head_snapshot(&self) -> Result<ObjectSnapshot> {
        let current = self.store.head(&self.location).await?;
        let mut snapshot = ObjectSnapshot::from(&current);
        if let Some(path) = &self.local_path {
            let metadata =
                std::fs::metadata(path).map_err(|error| Error::io(Some(path.clone()), error))?;
            if snapshot.size != metadata.len() {
                return Err(Error::Execution(format!(
                    "object changed while its identity was captured: {}: object store reported \
                     size {}, local metadata reported size {}",
                    self.uri,
                    snapshot.size,
                    metadata.len()
                )));
            }
            snapshot.local_identity = LocalFileIdentity::from_metadata(&metadata);
        }
        Ok(snapshot)
    }
}

impl fmt::Debug for ObjectSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectSource")
            .field("uri", &self.uri)
            .field("location", &self.location)
            .field("snapshot", &self.snapshot)
            .field("s3", &self.s3)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct LocationResolver {
    s3: S3Config,
    metadata_limit: usize,
}

impl LocationResolver {
    #[cfg(test)]
    pub fn new(s3: S3Config) -> Self {
        Self {
            s3,
            metadata_limit: 64 * 1024 * 1024,
        }
    }

    pub(crate) fn with_memory_limit(s3: S3Config, memory_limit: usize) -> Self {
        Self {
            s3,
            metadata_limit: (memory_limit / 64).clamp(1, 64 * 1024 * 1024),
        }
    }

    #[cfg(test)]
    fn with_metadata_limit(s3: S3Config, metadata_limit: usize) -> Self {
        Self { s3, metadata_limit }
    }

    pub async fn resolve(&self, locations: &[String]) -> Result<Vec<ObjectSource>> {
        self.resolve_inner(locations, None).await
    }

    pub(crate) async fn resolve_for_query(
        &self,
        locations: &[String],
        context: &QueryContext,
    ) -> Result<Vec<ObjectSource>> {
        self.resolve_inner(locations, Some(context)).await
    }

    async fn resolve_inner(
        &self,
        locations: &[String],
        context: Option<&QueryContext>,
    ) -> Result<Vec<ObjectSource>> {
        if locations.is_empty() {
            return Err(Error::InvalidArgument(
                "at least one file location is required".to_owned(),
            ));
        }

        let local_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let mut s3_stores: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();
        let mut objects = SourceList::new(self.metadata_limit);

        for location in locations {
            if location.starts_with("s3://") {
                self.resolve_s3(location, &mut s3_stores, &mut objects, context)
                    .await?;
            } else {
                resolve_local(location, Arc::clone(&local_store), &mut objects).await?;
            }
        }

        let objects = objects.finish()?;
        if objects.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "no files matched: {}",
                locations.join(", ")
            )));
        }
        if let Some(context) = context {
            context
                .metrics
                .add_discovered_files(u64::try_from(objects.len()).unwrap_or(u64::MAX));
            for object in &objects {
                context.register_object_snapshot(object.uri(), object.snapshot().clone())?;
            }
        }
        Ok(objects)
    }

    async fn resolve_s3(
        &self,
        location: &str,
        stores: &mut HashMap<String, Arc<dyn ObjectStore>>,
        objects: &mut SourceList,
        context: Option<&QueryContext>,
    ) -> Result<()> {
        let url = super::s3_uri::S3Uri::parse(location)?;
        let bucket = url.bucket();

        let path = Path::from_url_path(url.path().trim_start_matches('/'))
            .map_err(|error| Error::InvalidArgument(format!("invalid S3 key: {error}")))?;
        if path.as_ref().is_empty() {
            return Err(Error::InvalidArgument(
                "S3 URI must include an object key or pattern".to_owned(),
            ));
        }

        let store = match stores.get(bucket) {
            Some(store) => Arc::clone(store),
            None => {
                let store = self.build_s3_store(bucket)?;
                stores.insert(bucket.to_owned(), Arc::clone(&store));
                store
            }
        };

        if has_glob(path.as_ref()) {
            let pattern = Pattern::new(path.as_ref())
                .map_err(|error| Error::InvalidArgument(format!("invalid S3 glob: {error}")))?;
            let prefix = literal_prefix(path.as_ref());
            let prefix = if prefix.is_empty() {
                None
            } else {
                Some(Path::parse(prefix).map_err(|error| {
                    Error::InvalidArgument(format!("invalid S3 prefix: {error}"))
                })?)
            };
            let mut listing = store.list(prefix.as_ref()).try_filter(|meta| {
                futures::future::ready(pattern_matches(&pattern, meta.location.as_ref()))
            });
            if let Some(context) = context {
                context.check_cancelled()?;
                context.metrics.add_s3_requests(1);
            }
            loop {
                let next = match context {
                    Some(context) => tokio::select! {
                        _ = context.control.cancelled() => Err(Error::Cancelled),
                        result = listing.try_next() => result.map_err(Error::from),
                    }?,
                    None => listing.try_next().await?,
                };
                let Some(meta) = next else { break };
                let uri = format!("s3://{bucket}/{}", meta.location);
                objects.push(ObjectSource::new(uri, Arc::clone(&store), meta, true, None))?;
            }
        } else {
            let source =
                resolve_concrete_s3_source(bucket, location, &path, Arc::clone(&store), context)
                    .await?;
            objects.push(source)?;
        }
        Ok(())
    }

    pub(crate) fn build_s3_store(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = builder_from_env(self.s3.endpoint.as_deref(), self.s3.allow_http)?
            .with_bucket_name(bucket)
            .with_allow_http(self.s3.allow_http);

        if let Some(region) = &self.s3.region {
            builder = builder.with_region(region);
        }
        if self.s3.force_path_style {
            builder = builder.with_virtual_hosted_style_request(false);
        }
        if let Some(credentials) = &self.s3.credential_provider {
            builder = builder.with_credentials(Arc::clone(credentials));
        }
        if self.s3.anonymous {
            builder = builder.with_skip_signature(true);
        }

        Ok(Arc::new(builder.build()?))
    }
}

async fn resolve_concrete_s3_source(
    bucket: &str,
    uri: &str,
    path: &Path,
    store: Arc<dyn ObjectStore>,
    context: Option<&QueryContext>,
) -> Result<ObjectSource> {
    let manifest = copy_manifest_path(path)?;
    if let Some(context) = context {
        context.check_cancelled()?;
        context.metrics.add_s3_requests(2);
    }
    let heads = async { tokio::join!(store.head(path), store.head(&manifest)) };
    let (exact, manifest_head) = match context {
        Some(context) => tokio::select! {
            _ = context.control.cancelled() => return Err(Error::Cancelled),
            result = heads => result,
        },
        None => heads.await,
    };
    match (exact, manifest_head) {
        (Ok(_), Ok(_)) => Err(Error::InvalidArgument(
            "ambiguous S3 source: both the exact object and its COPY manifest exist".to_owned(),
        )),
        (Ok(meta), Err(object_store::Error::NotFound { .. })) => {
            Ok(ObjectSource::new(uri.to_owned(), store, meta, true, None))
        }
        (Err(object_store::Error::NotFound { .. }), Ok(_)) => {
            resolve_copy_manifest(bucket, path, store, context).await
        }
        (
            Err(error @ object_store::Error::NotFound { .. }),
            Err(object_store::Error::NotFound { .. }),
        ) => Err(error.into()),
        (Err(error), _) | (_, Err(error)) => Err(error.into()),
    }
}

fn copy_manifest_path(prefix: &Path) -> Result<Path> {
    Path::parse(format!(
        "{}/{}",
        prefix.as_ref(),
        super::copy_manifest::FILE_NAME
    ))
    .map_err(|error| Error::InvalidArgument(format!("invalid COPY manifest key: {error}")))
}

async fn resolve_copy_manifest(
    bucket: &str,
    prefix: &Path,
    store: Arc<dyn ObjectStore>,
    context: Option<&QueryContext>,
) -> Result<ObjectSource> {
    let manifest = copy_manifest_path(prefix)?;
    if let Some(context) = context {
        context.check_cancelled()?;
        context.metrics.add_s3_requests(1);
    }
    let response = match context {
        Some(context) => tokio::select! {
            _ = context.control.cancelled() => return Err(Error::Cancelled),
            result = store.get(&manifest) => result?,
        },
        None => store.get(&manifest).await?,
    };
    if response.meta.size > super::copy_manifest::MAX_BYTES as u64 {
        return Err(Error::ResourceExhausted(format!(
            "COPY manifest s3://{bucket}/{manifest} exceeds {} bytes",
            super::copy_manifest::MAX_BYTES
        )));
    }
    let bytes = match context {
        Some(context) => tokio::select! {
            _ = context.control.cancelled() => return Err(Error::Cancelled),
            result = response.bytes() => result?,
        },
        None => response.bytes().await?,
    };
    let entry = super::copy_manifest::decode(FsPath::new(manifest.as_ref()), &bytes)?;
    let expected_prefix = format!("{}/", prefix.as_ref());
    if !entry.object.starts_with(&expected_prefix) {
        return Err(Error::Execution(format!(
            "COPY manifest s3://{bucket}/{manifest} references an object outside its prefix"
        )));
    }
    let data = Path::parse(&entry.object)
        .map_err(|error| Error::Execution(format!("invalid COPY data object: {error}")))?;
    if let Some(context) = context {
        context.check_cancelled()?;
        context.metrics.add_s3_requests(1);
    }
    let meta = match context {
        Some(context) => tokio::select! {
            _ = context.control.cancelled() => return Err(Error::Cancelled),
            result = store.head(&data) => result?,
        },
        None => store.head(&data).await?,
    };
    if meta.size != entry.bytes {
        return Err(Error::Execution(format!(
            "COPY data object s3://{bucket}/{data} has size {}, manifest expected {}",
            meta.size, entry.bytes
        )));
    }
    let data_uri = format!("s3://{bucket}/{data}");
    if entry.e_tag.is_some() || entry.version.is_some() {
        let e_tag_changed = entry
            .e_tag
            .as_deref()
            .is_some_and(|expected| meta.e_tag.as_deref() != Some(expected));
        let version_changed = entry
            .version
            .as_deref()
            .is_some_and(|expected| meta.version.as_deref() != Some(expected));
        if e_tag_changed || version_changed {
            return Err(Error::Execution(format!(
                "COPY data object {data_uri} no longer has the identity recorded by its manifest"
            )));
        }
    } else {
        verify_legacy_copy_checksum(&data_uri, &data, &store, &meta, &entry.sha256, context)
            .await?;
    }
    Ok(ObjectSource::new(data_uri, store, meta, true, None))
}

async fn verify_legacy_copy_checksum(
    uri: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    meta: &object_store::ObjectMeta,
    expected_sha256: &str,
    context: Option<&QueryContext>,
) -> Result<()> {
    if let Some(context) = context {
        context.check_cancelled()?;
        context.metrics.add_s3_requests(1);
    }
    let options = GetOptions {
        if_match: meta.e_tag.clone(),
        version: meta.version.clone(),
        ..GetOptions::default()
    };
    let response = match context {
        Some(context) => tokio::select! {
            _ = context.control.cancelled() => return Err(Error::Cancelled),
            result = store.get_opts(data, options) => result?,
        },
        None => store.get_opts(data, options).await?,
    };
    ObjectSnapshot::from(meta).validate_get_response(uri, &response.meta)?;
    let mut stream = response.into_stream();
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    loop {
        let next = match context {
            Some(context) => tokio::select! {
                _ = context.control.cancelled() => return Err(Error::Cancelled),
                result = stream.next() => result,
            },
            None => stream.next().await,
        };
        let Some(chunk) = next else { break };
        let chunk = chunk?;
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        bytes = bytes.checked_add(len).ok_or_else(|| {
            Error::ResourceExhausted("COPY checksum byte count overflowed u64".to_owned())
        })?;
        if let Some(context) = context {
            context.metrics.add_s3_bytes_transferred(len);
        }
        digest.update(&chunk);
    }
    let actual = format!("{:x}", digest.finalize());
    if bytes != meta.size || !actual.eq_ignore_ascii_case(expected_sha256) {
        return Err(Error::Execution(format!(
            "COPY data object {uri} failed checksum validation"
        )));
    }
    Ok(())
}

async fn resolve_local(
    location: &str,
    store: Arc<dyn ObjectStore>,
    objects: &mut SourceList,
) -> Result<()> {
    let pattern = local_pattern(location)?;
    let entries = glob(pattern.to_string_lossy().as_ref()).map_err(|error| {
        Error::InvalidArgument(format!("invalid local glob {location}: {error}"))
    })?;

    for entry in entries {
        let path = entry.map_err(|error| {
            Error::io(
                Some(error.path().to_path_buf()),
                std::io::Error::other(error.error().to_string()),
            )
        })?;
        if !path.is_file() {
            continue;
        }
        let canonical =
            std::fs::canonicalize(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
        let object_path = Path::from_filesystem_path(&canonical).map_err(|error| {
            Error::InvalidArgument(format!(
                "invalid local path {}: {error}",
                canonical.display()
            ))
        })?;
        let meta = store.head(&object_path).await?;
        let uri = Url::from_file_path(&canonical)
            .map_err(|()| {
                Error::InvalidArgument(format!(
                    "cannot convert local path to URI: {}",
                    canonical.display()
                ))
            })?
            .to_string();
        let source = ObjectSource::new_local(uri, Arc::clone(&store), meta, canonical)?;
        objects.push(source)?;
    }
    Ok(())
}

fn local_pattern(location: &str) -> Result<PathBuf> {
    if location.starts_with("file://") {
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
        url.to_file_path()
            .map_err(|()| Error::InvalidArgument("file URI is not a local path".to_owned()))
    } else {
        let path = FsPath::new(location);
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            std::env::current_dir()
                .map(|directory| directory.join(path))
                .map_err(|error| Error::io(None, error))
        }
    }
}

fn has_glob(path: &str) -> bool {
    path.bytes().any(|byte| matches!(byte, b'*' | b'?' | b'['))
}

fn literal_prefix(pattern: &str) -> &str {
    let wildcard = pattern
        .find(|character| ['*', '?', '['].contains(&character))
        .unwrap_or(pattern.len());
    let literal = &pattern[..wildcard];
    literal.rsplit_once('/').map_or("", |(prefix, _)| prefix)
}

fn pattern_matches(pattern: &Pattern, value: &str) -> bool {
    pattern.matches_with(
        value,
        MatchOptions {
            case_sensitive: true,
            require_literal_separator: false,
            require_literal_leading_dot: false,
        },
    )
}

#[cfg(test)]
mod tests;

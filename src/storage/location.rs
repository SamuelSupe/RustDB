use std::{
    collections::HashMap,
    fmt,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use futures::TryStreamExt;
use glob::{MatchOptions, Pattern, glob};
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, local::LocalFileSystem, path::Path,
};
use url::Url;

use crate::{Error, Result, S3Config, runtime::QueryContext};

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
}

impl From<&ObjectMeta> for ObjectSnapshot {
    fn from(meta: &ObjectMeta) -> Self {
        Self {
            size: meta.size,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        }
    }
}

impl ObjectSnapshot {
    /// Verifies that an object GET still refers to the identity captured for
    /// this query. Some S3-compatible stores omit ETag and version values, so
    /// size remains a required fallback identity check.
    pub(crate) fn validate_get_response(&self, uri: &str, meta: &ObjectMeta) -> Result<()> {
        let actual = Self::from(meta);
        let e_tag_changed = self
            .e_tag
            .as_deref()
            .is_some_and(|expected| actual.e_tag.as_deref() != Some(expected));
        let version_changed = self
            .version
            .as_deref()
            .is_some_and(|expected| actual.version.as_deref() != Some(expected));
        if self.size != actual.size || e_tag_changed || version_changed {
            return Err(Error::Execution(format!(
                "object changed during query: {uri}: expected size {}, ETag {:?}, version {:?}; \
                 GET returned size {}, ETag {:?}, version {:?}",
                self.size, self.e_tag, self.version, actual.size, actual.e_tag, actual.version,
            )));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ObjectSource {
    uri: String,
    store: Arc<dyn ObjectStore>,
    location: Path,
    snapshot: ObjectSnapshot,
    s3: bool,
}

impl ObjectSource {
    fn new(uri: String, store: Arc<dyn ObjectStore>, meta: ObjectMeta, s3: bool) -> Self {
        Self {
            uri,
            store,
            location: meta.location.clone(),
            snapshot: ObjectSnapshot::from(&meta),
            s3,
        }
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
        Ok(ObjectSnapshot::from(&current))
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
        let url = Url::parse(location).map_err(|error| {
            Error::InvalidArgument(format!("invalid S3 URI {location}: {error}"))
        })?;
        let bucket = url
            .host_str()
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| Error::InvalidArgument(format!("S3 URI has no bucket: {location}")))?;
        if url.query().is_some() || url.fragment().is_some() {
            return Err(Error::InvalidArgument(format!(
                "S3 URI must not contain query or fragment: {location}"
            )));
        }

        let path = Path::from_url_path(url.path().trim_start_matches('/')).map_err(|error| {
            Error::InvalidArgument(format!("invalid S3 key in {location}: {error}"))
        })?;
        if path.as_ref().is_empty() {
            return Err(Error::InvalidArgument(format!(
                "S3 URI must include an object key or pattern: {location}"
            )));
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
            let pattern = Pattern::new(path.as_ref()).map_err(|error| {
                Error::InvalidArgument(format!("invalid S3 glob {location}: {error}"))
            })?;
            let prefix = literal_prefix(path.as_ref());
            let prefix = if prefix.is_empty() {
                None
            } else {
                Some(Path::parse(prefix).map_err(|error| {
                    Error::InvalidArgument(format!("invalid S3 prefix in {location}: {error}"))
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
                objects.push(ObjectSource::new(uri, Arc::clone(&store), meta, true))?;
            }
        } else {
            let head = store.head(&path);
            let meta = match context {
                Some(context) => {
                    context.check_cancelled()?;
                    context.metrics.add_s3_requests(1);
                    tokio::select! {
                        _ = context.control.cancelled() => Err(Error::Cancelled),
                        result = head => result.map_err(Error::from),
                    }?
                }
                None => head.await?,
            };
            objects.push(ObjectSource::new(
                location.to_owned(),
                Arc::clone(&store),
                meta,
                true,
            ))?;
        }
        Ok(())
    }

    fn build_s3_store(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
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
        objects.push(ObjectSource::new(uri, Arc::clone(&store), meta, false))?;
    }
    Ok(())
}

fn local_pattern(location: &str) -> Result<PathBuf> {
    if location.starts_with("file://") {
        let url = Url::parse(location).map_err(|error| {
            Error::InvalidArgument(format!("invalid file URI {location}: {error}"))
        })?;
        url.to_file_path().map_err(|()| {
            Error::InvalidArgument(format!("file URI is not a local path: {location}"))
        })
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

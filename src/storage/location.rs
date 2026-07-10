use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use futures::TryStreamExt;
use glob::{MatchOptions, Pattern, glob};
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, aws::AmazonS3Builder,
    local::LocalFileSystem, path::Path,
};
use url::Url;

use crate::{Error, Result, S3Config, runtime::QueryContext};

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
}

impl LocationResolver {
    pub fn new(s3: S3Config) -> Self {
        Self { s3 }
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
        let mut objects = Vec::new();

        for location in locations {
            if location.starts_with("s3://") {
                self.resolve_s3(location, &mut s3_stores, &mut objects, context)
                    .await?;
            } else {
                resolve_local(location, Arc::clone(&local_store), &mut objects).await?;
            }
        }

        objects.sort_by(|left, right| left.uri.cmp(&right.uri));
        let mut seen = HashSet::new();
        objects.retain(|object| seen.insert(object.uri.clone()));
        if objects.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "no files matched: {}",
                locations.join(", ")
            )));
        }
        if let Some(context) = context {
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
        objects: &mut Vec<ObjectSource>,
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
            let listing = store
                .list(prefix.as_ref())
                .try_filter(|meta| {
                    futures::future::ready(pattern_matches(&pattern, meta.location.as_ref()))
                })
                .try_collect::<Vec<_>>();
            let matches = match context {
                Some(context) => {
                    context.check_cancelled()?;
                    context.metrics.add_s3_requests(1);
                    tokio::select! {
                        _ = context.control.cancelled() => Err(Error::Cancelled),
                        result = listing => result.map_err(Error::from),
                    }?
                }
                None => listing.await?,
            };
            objects.extend(matches.into_iter().map(|meta| {
                let uri = format!("s3://{bucket}/{}", meta.location);
                ObjectSource::new(uri, Arc::clone(&store), meta, true)
            }));
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
            ));
        }
        Ok(())
    }

    fn build_s3_store(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_allow_http(self.s3.allow_http);

        if let Some(region) = &self.s3.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &self.s3.endpoint {
            validate_endpoint(endpoint, self.s3.allow_http)?;
            builder = builder.with_endpoint(endpoint);
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
    objects: &mut Vec<ObjectSource>,
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
        objects.push(ObjectSource::new(uri, Arc::clone(&store), meta, false));
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

pub(crate) fn validate_endpoint(endpoint: &str, allow_http: bool) -> Result<()> {
    let url = Url::parse(endpoint)
        .map_err(|error| Error::InvalidArgument(format!("invalid S3 endpoint: {error}")))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::InvalidArgument(
            "S3 endpoint must not contain credentials; use a credential provider".to_owned(),
        ));
    }
    match url.scheme() {
        "https" => Ok(()),
        "http" if allow_http => Ok(()),
        "http" => Err(Error::InvalidArgument(
            "S3 endpoint uses HTTP but allow_http is false".to_owned(),
        )),
        scheme => Err(Error::InvalidArgument(format!(
            "unsupported S3 endpoint scheme {scheme}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use tempfile::tempdir;

    use super::{LocationResolver, literal_prefix, validate_endpoint};
    use crate::{
        S3Config,
        runtime::{MemoryPool, QueryContext},
    };

    #[tokio::test]
    async fn expands_local_globs_in_stable_order() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join("b.csv"), b"b\n2\n").unwrap();
        fs::write(directory.path().join("a.csv"), b"a\n1\n").unwrap();
        let locations = vec![format!("{}/*.csv", directory.path().display())];

        let objects = LocationResolver::new(S3Config::default())
            .resolve(&locations)
            .await
            .unwrap();

        assert_eq!(objects.len(), 2);
        assert!(objects[0].uri().ends_with("a.csv"));
        assert!(objects[1].uri().ends_with("b.csv"));
        assert_eq!(
            objects[0].head_snapshot().await.unwrap(),
            objects[0].snapshot().clone()
        );
    }

    #[tokio::test]
    async fn query_resolution_registers_the_initial_object_identity() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("changing.csv");
        fs::write(&path, b"value\nold\n").unwrap();
        let context =
            Arc::new(QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap());
        let objects = LocationResolver::new(S3Config::default())
            .resolve_for_query(&[path.display().to_string()], &context)
            .await
            .unwrap();

        fs::write(&path, b"value\nnew-and-different\n").unwrap();
        let current = objects[0].head_snapshot().await.unwrap();
        let error = context
            .register_object_snapshot(objects[0].uri(), current)
            .unwrap_err();
        assert!(error.to_string().contains("identity changed"));
    }

    #[test]
    fn rejects_insecure_endpoint_by_default() {
        let error = validate_endpoint("http://minio:9000", false).unwrap_err();
        assert!(error.to_string().contains("allow_http is false"));
    }

    #[test]
    fn rejects_and_redacts_endpoint_userinfo() {
        let error = validate_endpoint("https://alice:super-secret@example.test", false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("must not contain credentials"));
        assert!(!error.contains("super-secret"));
    }

    #[test]
    fn extracts_listing_prefix_before_glob_segment() {
        assert_eq!(
            literal_prefix("events/year=2026/*.parquet"),
            "events/year=2026"
        );
        assert_eq!(literal_prefix("*.parquet"), "");
    }
}

use std::{collections::HashMap, path::Path, sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use crate::{
    EngineConfig, Error, Result,
    runtime::{QueryContext, RecordBatchStream},
    storage::{LocationResolver, NativeTableSnapshot, ObjectSource},
};

use super::{
    MetadataCache, ParquetTable, PredicateGuarantee, ScanPredicate, ScanRequest, ScanTask,
    TableProvider, TableSourceIdentity, TableStatistics, provider::next_provider_id,
};

mod predicate_sidecar;
mod verification_cache;

pub(super) use predicate_sidecar::{
    NativePredicateSidecar, SidecarProjectionCandidate, SidecarProjectionExecution,
};
use verification_cache::VerificationCache;

#[derive(Clone, Debug)]
pub(crate) struct NativeSegmentTable {
    id: u64,
    root: Arc<Path>,
    snapshot: Arc<NativeTableSnapshot>,
    locations: Arc<[String]>,
    statistics: TableStatistics,
    config: EngineConfig,
    metadata_cache: MetadataCache,
    verification_cache: Arc<VerificationCache>,
}

impl NativeSegmentTable {
    pub(crate) fn new(
        root: &Path,
        snapshot: Arc<NativeTableSnapshot>,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Self {
        let locations = snapshot
            .segment_paths(root)
            .into_iter()
            .map(|path| {
                path.into_os_string()
                    .into_string()
                    .expect("native database path was validated as UTF-8")
            })
            .collect::<Vec<_>>();
        let statistics = TableStatistics {
            row_count: Some(snapshot.row_count()),
            total_byte_size: Some(snapshot.segment_bytes()),
            file_count: locations.len(),
        };
        let verification_cache = VerificationCache::new(&snapshot);
        Self {
            id: next_provider_id(),
            root: Arc::from(root),
            snapshot,
            locations: locations.into(),
            statistics,
            config: config.clone(),
            metadata_cache,
            verification_cache,
        }
    }

    async fn resolve_files(&self, context: &QueryContext) -> Result<Vec<ObjectSource>> {
        let files = if self.locations.is_empty() {
            Vec::new()
        } else {
            LocationResolver::with_memory_limit(self.config.s3.clone(), self.config.memory_limit)
                .resolve_for_query(&self.locations, context)
                .await?
        };
        Ok(files)
    }

    async fn resolve_predicate_sidecars(
        &self,
        files: &[ObjectSource],
        context: &QueryContext,
    ) -> Result<Vec<Option<NativePredicateSidecar>>> {
        let bindings = self.snapshot.predicate_sidecar_bindings(&self.root);
        if bindings.is_empty() {
            return Ok(vec![None; files.len()]);
        }

        let mut file_indices = HashMap::with_capacity(files.len());
        for (index, file) in files.iter().enumerate() {
            let path = file.local_path().ok_or_else(|| {
                Error::Internal(format!(
                    "Native segment did not resolve to a local path: {}",
                    file.uri()
                ))
            })?;
            if file_indices.insert(path.to_path_buf(), index).is_some() {
                return Err(Error::Internal(format!(
                    "Native query snapshot resolved duplicate segment path {}",
                    path.display()
                )));
            }
        }

        let locations = bindings
            .iter()
            .map(|binding| path_string(binding.sidecar_path()))
            .collect::<Result<Vec<_>>>()?;
        // Predicate companions are auxiliary objects. Resolve them without the
        // data-file discovery metric, then register their identities explicitly
        // before the query snapshot is sealed.
        let resolved =
            LocationResolver::with_memory_limit(self.config.s3.clone(), self.config.memory_limit)
                .resolve(&locations)
                .await
                .map_err(|error| {
                    Error::Execution(format!(
                        "declared Native predicate sidecar could not be resolved: {error}"
                    ))
                })?;
        let mut sidecars_by_path = HashMap::with_capacity(resolved.len());
        for sidecar in resolved {
            let path = sidecar.local_path().map(Path::to_path_buf).ok_or_else(|| {
                Error::Internal(format!(
                    "Native predicate sidecar did not resolve to a local path: {}",
                    sidecar.uri()
                ))
            })?;
            if sidecars_by_path.insert(path.clone(), sidecar).is_some() {
                return Err(Error::Execution(format!(
                    "duplicate Native predicate sidecar resolved for {}",
                    path.display()
                )));
            }
        }

        let mut aligned = vec![None; files.len()];
        for binding in bindings {
            let file_index = file_indices
                .get(binding.data_path())
                .copied()
                .ok_or_else(|| missing_binding("segment", binding.data_path()))?;
            let sidecar = sidecars_by_path
                .remove(binding.sidecar_path())
                .ok_or_else(|| missing_binding("predicate sidecar", binding.sidecar_path()))?;
            if sidecar.snapshot().size != binding.sidecar_bytes() {
                return Err(Error::Execution(format!(
                    "Native predicate sidecar size does not match its manifest: {} expected {} bytes, found {}",
                    binding.sidecar_path().display(),
                    binding.sidecar_bytes(),
                    sidecar.snapshot().size,
                )));
            }
            context.register_object_snapshot(sidecar.uri(), sidecar.snapshot().clone())?;
            if aligned[file_index].is_some() {
                return Err(Error::Execution(format!(
                    "multiple Native predicate sidecars are bound to {}",
                    binding.data_path().display()
                )));
            }
            aligned[file_index] = Some(NativePredicateSidecar::new(
                files[file_index].uri(),
                sidecar,
                binding,
            ));
        }
        if let Some((path, _)) = sidecars_by_path.into_iter().next() {
            return Err(Error::Execution(format!(
                "unexpected Native predicate sidecar resolved for {}",
                path.display()
            )));
        }
        Ok(aligned)
    }

    fn fixed_provider(
        &self,
        files: Vec<ObjectSource>,
        sidecars: Vec<Option<NativePredicateSidecar>>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table = ParquetTable::from_fixed_files_with_predicate_sidecars(
            files,
            sidecars,
            self.snapshot.schema(),
            self.statistics.clone(),
            &self.config,
            self.metadata_cache.clone(),
        )?;
        Ok(Arc::new(table))
    }

    fn validate_delegation(
        &self,
        request: &ScanRequest,
        provider: &Arc<dyn TableProvider>,
    ) -> Result<()> {
        let snapshot_schema = self.snapshot.schema();
        if provider.schema().as_ref() != snapshot_schema.as_ref() {
            return Err(Error::Internal(
                "native fixed Parquet provider does not match its snapshot schema".to_owned(),
            ));
        }
        if request.predicate_guarantee != PredicateGuarantee::Exact {
            return Ok(());
        }
        let predicate = request.predicate.as_ref().ok_or_else(|| {
            Error::Internal("native exact scan is missing its predicate".to_owned())
        })?;
        if !self.supports_exact_filter(predicate) || !provider.supports_exact_filter(predicate) {
            return Err(Error::Internal(
                "native exact predicate is not fully supported by its fixed Parquet provider"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl TableProvider for NativeSegmentTable {
    fn schema(&self) -> SchemaRef {
        self.snapshot.schema()
    }

    fn statistics(&self) -> TableStatistics {
        self.statistics.clone()
    }

    fn source_identity(&self) -> Option<TableSourceIdentity> {
        Some(TableSourceIdentity::from_spec(
            "native-parquet",
            &self.locations,
            format!(
                "table={};version={};snapshot={};manifest={}",
                self.snapshot.table_id(),
                self.snapshot.version(),
                self.snapshot.snapshot_id(),
                self.snapshot.manifest_sha256()
            ),
        ))
    }

    fn explain_scan(&self) -> Option<String> {
        Some(format!(
            "format=native-parquet version={} segments={} morsel=row_group_chunk(max=4) root={}",
            self.snapshot.version(),
            self.locations.len(),
            self.root.display()
        ))
    }

    fn supports_exact_filter(&self, predicate: &ScanPredicate) -> bool {
        let schema = self.snapshot.schema();
        super::exact_filter::supported(predicate, schema.as_ref(), schema.as_ref())
    }

    fn query_statistics(&self, _context: &QueryContext) -> TableStatistics {
        self.statistics()
    }

    async fn prepare(&self, context: Arc<QueryContext>) -> Result<()> {
        if context.prepared_provider(self.id).is_some() {
            return Ok(());
        }
        context.check_cancelled()?;
        // Fix the exact object identities before checksum verification. The
        // provider below consumes these same sources, so a replacement after
        // verification is rejected by its conditional/local identity reads
        // instead of becoming a new, unverified query snapshot.
        let files = self.resolve_files(&context).await?;
        let sidecars = self.resolve_predicate_sidecars(&files, &context).await?;
        context.check_cancelled()?;
        let verification_started = Instant::now();
        let verified = self
            .verification_cache
            .verify(
                Arc::clone(&self.root),
                Arc::clone(&self.snapshot),
                Arc::clone(&context),
            )
            .await;
        context
            .metrics
            .record_native_verification_time(verification_started.elapsed());
        verified?;
        context.check_cancelled()?;
        let provider = self.fixed_provider(files, sidecars)?;
        context.cache_prepared_provider(self.id, provider)
    }

    async fn scan(
        &self,
        mut request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        // Compatibility/public scans retain their requested result batch size.
        // Larger physical decode granularity is private to scan_tasks sinks.
        request.decode_batch_size = None;
        let provider = context
            .prepared_provider(self.id)
            .ok_or_else(|| Error::Internal("native table scanned before preparation".to_owned()))?;
        self.validate_delegation(&request, &provider)?;
        provider.scan(request, context).await
    }

    async fn scan_tasks(
        &self,
        mut request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        // Native scan sizing stays on the measured public batch size until a
        // larger private hint proves both faster and memory efficient.
        request.decode_batch_size = None;
        let provider = context
            .prepared_provider(self.id)
            .ok_or_else(|| Error::Internal("native table scanned before preparation".to_owned()))?;
        self.validate_delegation(&request, &provider)?;
        provider.scan_tasks(request, context, target_tasks).await
    }
}

fn path_string(path: &Path) -> Result<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        Error::InvalidArgument(format!(
            "Native predicate sidecar path is not valid UTF-8: {}",
            path.display()
        ))
    })
}

fn missing_binding(kind: &str, path: &Path) -> Error {
    Error::Execution(format!(
        "declared Native {kind} is missing from the query snapshot: {}",
        path.display()
    ))
}

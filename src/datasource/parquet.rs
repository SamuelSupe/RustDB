use std::{mem::size_of, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::Semaphore;

mod filtered_limit;
mod fixed;
mod preplan;
mod row_group_chunk;

use super::{
    MetadataCache, PredicateGuarantee, ScanRequest, ScanTask, TableProvider, TableSourceIdentity,
    TableStatistics,
    hive::HivePartitions,
    parquet_bloom::{bloom_prunes_row_group, supports_bloom},
    parquet_decode_batch,
    parquet_dictionary::DictionaryDecode,
    parquet_index_metadata::load_page_index_metadata,
    parquet_metadata::{
        ParquetMetadata, load_parquet_metadata, registration_metadata_limit, resize_schema_budget,
        schema_memory_size,
    },
    parquet_page_pruning::{prune_pages, supports_page_index},
    parquet_predicate_cache,
    parquet_pruning::can_prune_row_group,
    parquet_pruning_budget::PruningBudget,
    parquet_row_filter::{ParquetRowFilter, workspace_bytes as row_filter_workspace_bytes},
    parquet_scan::{
        NativeSidecarMorsel, ParquetFilePlan, ParquetMorsel, ParquetMorselStream, morsel_stream,
        scan_morsels,
    },
    provider::prepare_object_sources,
    schema_evolution::{
        ParquetSchemaMode, SchemaSource, canonical_type, canonicalize_schema, merge_file_schemas,
        merge_types,
    },
};
use crate::{
    EngineConfig, Error, ParquetOptions, ParquetPruningMode, ParquetScanConfig, Result,
    runtime::{MemoryReservation, QueryContext, RecordBatchStream, estimate_schema_batch_bytes},
    storage::{LocationResolver, ObjectSource},
};
use filtered_limit::{FilteredLimit, reader_limit};

const QUERY_METADATA_REUSE_DIVISOR: usize = 64;

#[derive(Clone, Debug)]
pub struct ParquetTable {
    files: Arc<[ObjectSource]>,
    schema: SchemaRef,
    physical_schema: SchemaRef,
    statistics: TableStatistics,
    hive: Option<Arc<HivePartitions>>,
    schema_mode: ParquetSchemaMode,
    file_schemas: Arc<[FileSchema]>,
    metadata_cache: MetadataCache,
    io_concurrency: usize,
    parquet_scan: ParquetScanConfig,
    _schema_reservation: Option<Arc<MemoryReservation>>,
    query_metadata: Option<Arc<[ParquetMetadata]>>,
    fixed_files: bool,
    native_predicate_sidecars: Option<Arc<[Option<super::native::NativePredicateSidecar>]>>,
}

#[derive(Clone, Debug)]
struct FileSchema {
    uri: String,
    schema: SchemaRef,
}

impl ParquetTable {
    pub(crate) async fn try_new_with_cache(
        locations: Vec<String>,
        options: ParquetOptions,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Result<Self> {
        Self::try_new_with_cache_for_query(locations, options, config, metadata_cache, None).await
    }

    pub(crate) async fn try_new_with_cache_for_query(
        locations: Vec<String>,
        options: ParquetOptions,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
        context: Option<Arc<QueryContext>>,
    ) -> Result<Self> {
        let resolver = LocationResolver::with_memory_limit(config.s3.clone(), config.memory_limit);
        let files = match context.as_deref() {
            Some(context) => resolver.resolve_for_query(&locations, context).await?,
            None => resolver.resolve(&locations).await?,
        };
        Self::from_files(
            files,
            options,
            config.io_concurrency,
            config.parquet_scan.clone(),
            metadata_cache,
            context,
            registration_metadata_limit(config),
        )
        .await
    }

    async fn from_files(
        files: Vec<ObjectSource>,
        options: ParquetOptions,
        io_concurrency: usize,
        parquet_scan: ParquetScanConfig,
        metadata_cache: MetadataCache,
        context: Option<Arc<QueryContext>>,
        registration_limit: usize,
    ) -> Result<Self> {
        if files.is_empty() {
            return Err(Error::InvalidArgument(
                "Parquet table requires at least one file".to_owned(),
            ));
        }
        if io_concurrency == 0 {
            return Err(Error::InvalidArgument(
                "I/O concurrency must be greater than zero".to_owned(),
            ));
        }

        let schema_mode = options.effective_schema_mode()?;
        let ParquetOptions {
            schema: explicit_schema,
            union_by_name: _,
            schema_mode: _,
            hive_partitioning,
        } = options;
        let explicit_schema = explicit_schema.map(canonicalize_schema);
        let has_explicit_schema = explicit_schema.is_some();
        let mut schema_reservation = context.as_ref().map(|context| context.memory.reservation());
        if let Some(schema) = &explicit_schema {
            resize_schema_budget(
                schema_memory_size(schema),
                context.as_deref(),
                registration_limit,
                schema_reservation.as_mut(),
            )?;
        }
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        let mut file_schemas = Vec::with_capacity(files.len());
        let mut query_metadata = context.as_ref().map(|_| Vec::with_capacity(files.len()));
        let query_metadata_limit = context
            .as_ref()
            .map(|context| context.memory.limit() / QUERY_METADATA_REUSE_DIVISOR);
        let mut query_metadata_bytes = 0_usize;
        for file in &files {
            let metadata = load_parquet_metadata(
                file,
                file.snapshot().clone(),
                context.as_deref(),
                &metadata_cache,
                registration_limit,
            )
            .await?;
            let reader_metadata = metadata.reader_metadata();
            let incoming = reader_metadata.schema();
            file_schemas.push(FileSchema {
                uri: file.uri().to_owned(),
                schema: Arc::clone(incoming),
            });
            rows = rows.saturating_add(
                u64::try_from(reader_metadata.metadata().file_metadata().num_rows())
                    .unwrap_or(u64::MAX),
            );
            bytes = bytes.saturating_add(file.snapshot().size);
            if query_metadata.is_some() {
                let next_bytes = query_metadata_bytes.saturating_add(metadata.reserved_bytes());
                if query_metadata_limit.is_some_and(|limit| next_bytes <= limit) {
                    query_metadata_bytes = next_bytes;
                    query_metadata
                        .as_mut()
                        .expect("query metadata retention is enabled")
                        .push(metadata);
                } else {
                    query_metadata = None;
                }
            }
        }

        let mut physical_schema = match explicit_schema {
            Some(schema) => {
                for file in &file_schemas {
                    validate_file_schema(&file.uri, &file.schema, &schema, schema_mode)?;
                }
                schema
            }
            None => {
                let sources = file_schemas
                    .iter()
                    .map(|file| SchemaSource {
                        uri: &file.uri,
                        schema: &file.schema,
                    })
                    .collect::<Vec<_>>();
                merge_file_schemas(&sources, schema_mode, None)?
            }
        };
        if !has_explicit_schema && schema_mode == ParquetSchemaMode::UnionByName {
            physical_schema = nullable_schema(&physical_schema);
        }
        let file_schema_bytes = file_schemas_memory_size(&file_schemas);
        resize_schema_budget(
            file_schema_bytes.saturating_add(schema_memory_size(&physical_schema)),
            context.as_deref(),
            registration_limit,
            schema_reservation.as_mut(),
        )?;
        let hive = if hive_partitioning {
            HivePartitions::discover(&files, &physical_schema)?.map(Arc::new)
        } else {
            None
        };
        let schema = hive.as_ref().map_or_else(
            || Arc::clone(&physical_schema),
            |hive| hive.append_schema(&physical_schema),
        );
        let retained_schema_bytes = file_schema_bytes
            .saturating_add(schema_memory_size(&schema))
            .saturating_add(if Arc::ptr_eq(&schema, &physical_schema) {
                0
            } else {
                schema_memory_size(&physical_schema)
            })
            .saturating_add(hive.as_deref().map_or(0, HivePartitions::memory_size));
        resize_schema_budget(
            retained_schema_bytes,
            context.as_deref(),
            registration_limit,
            schema_reservation.as_mut(),
        )?;
        let statistics = TableStatistics {
            row_count: Some(rows),
            total_byte_size: Some(bytes),
            file_count: files.len(),
        };
        Ok(Self {
            files: files.into(),
            schema,
            physical_schema,
            statistics,
            hive,
            schema_mode,
            file_schemas: file_schemas.into(),
            metadata_cache,
            io_concurrency,
            parquet_scan,
            _schema_reservation: schema_reservation.map(Arc::new),
            query_metadata: query_metadata.map(Arc::from),
            fixed_files: false,
            native_predicate_sidecars: None,
        })
    }

    pub(crate) fn physical_schema(&self) -> SchemaRef {
        Arc::clone(&self.physical_schema)
    }

    pub(crate) fn validate_compatible_with(
        &self,
        expected: &Schema,
        mode: ParquetSchemaMode,
    ) -> Result<()> {
        for file in self.file_schemas.iter() {
            validate_file_schema(&file.uri, &file.schema, expected, mode).map_err(|error| {
                Error::Execution(format!(
                    "Parquet file is incompatible with the registered schema: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

fn file_schemas_memory_size(file_schemas: &[FileSchema]) -> usize {
    file_schemas.iter().fold(
        size_of::<FileSchema>().saturating_mul(file_schemas.len()),
        |bytes, file| {
            bytes
                .saturating_add(file.uri.capacity())
                .saturating_add(schema_memory_size(&file.schema))
        },
    )
}

#[async_trait]
impl TableProvider for ParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        self.statistics.clone()
    }

    fn source_identity(&self) -> Option<TableSourceIdentity> {
        Some(TableSourceIdentity::from_objects(
            "parquet",
            &self.files,
            format!(
                "schema={:?};physical_schema={:?};schema_mode={:?};hive={}",
                self.schema,
                self.physical_schema,
                self.schema_mode,
                self.hive.is_some()
            ),
        ))
    }

    fn explain_scan(&self) -> Option<String> {
        let morsel = if self.fixed_files {
            "row_group_chunk(max=4)"
        } else {
            "row_group"
        };
        Some(format!(
            "format=parquet morsel={morsel} metadata=singleflight"
        ))
    }

    fn supports_exact_filter(&self, predicate: &super::ScanPredicate) -> bool {
        self.schema_mode == ParquetSchemaMode::Strict
            && super::exact_filter::supported(predicate, &self.schema, &self.physical_schema)
    }

    async fn prepare(&self, context: Arc<QueryContext>) -> Result<()> {
        prepare_object_sources(&self.files, self.io_concurrency, context).await
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        if request.batch_size == 0 {
            return Err(Error::InvalidArgument(
                "scan batch_size must be greater than zero".to_owned(),
            ));
        }
        let output_schema = request.projected_schema(&self.schema)?;
        let files = Arc::clone(&self.files);
        let table_schema = Arc::clone(&self.schema);
        let hive = self.hive.clone();
        let decode_batch_size = parquet_decode_batch::admitted_size(
            &request,
            output_schema.as_ref(),
            self.schema.as_ref(),
            context.memory.limit(),
            self.io_concurrency,
        );
        let limit = request.limit;
        let pruning_budget = PruningBudget::for_query(&self.parquet_scan, context.memory.limit());
        let preplanned_metadata = preplan::try_preload(
            self.fixed_files,
            &self.files,
            &request,
            &context,
            &self.metadata_cache,
            &self.parquet_scan,
            self.io_concurrency,
            self.io_concurrency,
            &pruning_budget,
        )
        .await?;
        let morsels = plan_morsels(ScanPlanning {
            files,
            output_schema: Arc::clone(&output_schema),
            table_schema,
            physical_schema: Arc::clone(&self.physical_schema),
            hive: hive.clone(),
            schema_mode: self.schema_mode,
            request,
            context: Arc::clone(&context),
            metadata_cache: self.metadata_cache.clone(),
            parquet_scan: self.parquet_scan.clone(),
            query_metadata: self.query_metadata.clone(),
            decode_batch_size,
            predicate_cache_lanes: self.io_concurrency,
            pruning_budget,
            preplanned_metadata,
            fixed_files: self.fixed_files,
            native_predicate_sidecars: self.native_predicate_sidecars.clone(),
        });
        Ok(scan_morsels(
            morsels,
            output_schema,
            hive,
            context,
            self.io_concurrency,
            decode_batch_size,
            limit,
        ))
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        if request.batch_size == 0 {
            return Err(Error::InvalidArgument(
                "scan batch_size must be greater than zero".to_owned(),
            ));
        }
        let output_schema = request.projected_schema(&self.schema)?;
        let decode_batch_size = parquet_decode_batch::admitted_size(
            &request,
            output_schema.as_ref(),
            self.schema.as_ref(),
            context.memory.limit(),
            target_tasks,
        );
        let preclaim = estimate_schema_batch_bytes(output_schema.as_ref(), decode_batch_size)
            .saturating_add(row_filter_workspace_bytes(
                request.predicate.as_ref(),
                &self.schema,
                decode_batch_size,
            ));
        let filtered_limit = FilteredLimit::for_request(&request);
        let hive = self.hive.clone();
        let target_tasks = target_tasks.max(1);
        let pruning_budget = PruningBudget::for_query(&self.parquet_scan, context.memory.limit());
        let preplanned_metadata = preplan::try_preload(
            self.fixed_files,
            &self.files,
            &request,
            &context,
            &self.metadata_cache,
            &self.parquet_scan,
            self.io_concurrency,
            target_tasks,
            &pruning_budget,
        )
        .await?;
        let mut morsels = plan_morsels(ScanPlanning {
            files: Arc::clone(&self.files),
            output_schema: Arc::clone(&output_schema),
            table_schema: Arc::clone(&self.schema),
            physical_schema: Arc::clone(&self.physical_schema),
            hive: hive.clone(),
            schema_mode: self.schema_mode,
            request,
            context: Arc::clone(&context),
            metadata_cache: self.metadata_cache.clone(),
            parquet_scan: self.parquet_scan.clone(),
            query_metadata: self.query_metadata.clone(),
            decode_batch_size,
            predicate_cache_lanes: target_tasks.max(1),
            pruning_budget,
            preplanned_metadata,
            fixed_files: self.fixed_files,
            native_predicate_sidecars: self.native_predicate_sidecars.clone(),
        });

        // Prefetch at most one first morsel per configured lane. This reveals
        // the exact task count when the source has fewer row groups than
        // workers, while keeping planning metadata bounded for files with very
        // many row groups. Remaining morsels stay lazy behind the shared
        // planning stream.
        let mut first_morsels = Vec::with_capacity(target_tasks);
        while first_morsels.len() < target_tasks {
            context.check_cancelled()?;
            let Some(morsel) = morsels.next().await else {
                break;
            };
            first_morsels.push(morsel?);
        }
        if first_morsels.is_empty() {
            return Ok(Vec::new());
        }

        // ScanTask is the schedulable lane input. Do not manufacture empty
        // workers up to the configured thread count: a one-row-group file has
        // one unit of work and therefore one active lane. Each task owns one
        // guaranteed first morsel, then pulls remaining work one morsel at a
        // time without holding the planning lock during decode or output.
        let morsels = Arc::new(tokio::sync::Mutex::new(morsels));
        let io_permits = Arc::new(Semaphore::new(self.io_concurrency));
        Ok(first_morsels
            .into_iter()
            .enumerate()
            .map(|(id, first_morsel)| {
                let morsels = Arc::clone(&morsels);
                let io_permits = Arc::clone(&io_permits);
                let output_schema = Arc::clone(&output_schema);
                let hive = hive.clone();
                let task_context = Arc::clone(&context);
                let stream_context = Arc::clone(&task_context);
                let filtered_limit = filtered_limit.clone();
                ScanTask::from_public(
                    id,
                    crate::runtime::boxed_record_batch_stream(try_stream! {
                        let mut next = Some(first_morsel);
                        'scan: loop {
                            if filtered_limit
                                .as_ref()
                                .is_some_and(|limit| limit.exhausted())
                            {
                                break;
                            }
                            let morsel = match next.take() {
                                Some(morsel) => morsel,
                                None => {
                                    let mut remaining = morsels.lock().await;
                                    let Some(morsel) = remaining.next().await else {
                                        break;
                                    };
                                    morsel?
                                }
                            };
                            stream_context.check_cancelled()?;
                            let mut input = morsel_stream(
                                morsel,
                                Arc::clone(&output_schema),
                                hive.clone(),
                                Arc::clone(&stream_context),
                                decode_batch_size,
                            );
                            loop {
                                // Compute lanes may outnumber configured object
                                // I/O concurrency. Admit only the poll that
                                // obtains the next batch; release the permit
                                // before yielding to a potentially slow
                                // consumer so backpressure cannot pin an I/O
                                // slot.
                                let batch = {
                                    let _io_permit = Arc::clone(&io_permits)
                                        .acquire_owned()
                                        .await
                                        .map_err(|_| Error::Internal(
                                            "Parquet I/O concurrency limiter closed unexpectedly".into(),
                                        ))?;
                                    // The shared budget may have been consumed
                                    // while this lane waited for I/O admission.
                                    if filtered_limit
                                        .as_ref()
                                        .is_some_and(|limit| limit.exhausted())
                                    {
                                        break 'scan;
                                    }
                                    input.next().await
                                };
                                let Some(batch) = batch else {
                                    break;
                                };
                                let batch = batch?;
                                let Some(filtered_limit) = filtered_limit.as_ref() else {
                                    yield batch;
                                    continue;
                                };
                                if batch.num_rows() == 0 {
                                    continue;
                                }
                                let claimed = filtered_limit.claim(batch.num_rows());
                                if claimed == 0 {
                                    break 'scan;
                                }
                                if claimed < batch.num_rows() {
                                    yield batch.slice(0, claimed);
                                } else {
                                    yield batch;
                                }
                                if filtered_limit.exhausted() {
                                    break 'scan;
                                }
                            }
                        }
                    }),
                    task_context,
                    preclaim,
                    "Parquet scan task",
                )
            })
            .collect())
    }
}

struct ScanPlanning {
    files: Arc<[ObjectSource]>,
    output_schema: SchemaRef,
    table_schema: SchemaRef,
    physical_schema: SchemaRef,
    hive: Option<Arc<HivePartitions>>,
    schema_mode: ParquetSchemaMode,
    request: ScanRequest,
    context: Arc<QueryContext>,
    metadata_cache: MetadataCache,
    parquet_scan: ParquetScanConfig,
    query_metadata: Option<Arc<[ParquetMetadata]>>,
    decode_batch_size: usize,
    predicate_cache_lanes: usize,
    pruning_budget: PruningBudget,
    preplanned_metadata: Option<preplan::MetadataPreplan>,
    fixed_files: bool,
    native_predicate_sidecars: Option<Arc<[Option<super::native::NativePredicateSidecar>]>>,
}

// `try_stream!` expands in the dependency's edition, where let-chains are not
// available. Keep the sidecar admission branches explicit.
#[allow(clippy::collapsible_if)]
fn plan_morsels(plan: ScanPlanning) -> ParquetMorselStream {
    Box::pin(try_stream! {
        if plan.request.limit == Some(0) {
            return;
        }
        let mut preplanned_metadata = plan.preplanned_metadata;
        let use_page_index = plan.parquet_scan.page_index == ParquetPruningMode::Auto
            && supports_page_index(plan.request.predicate.as_ref());
        let use_bloom = plan.parquet_scan.bloom_filter == ParquetPruningMode::Auto
            && supports_bloom(plan.request.predicate.as_ref());
        let mut pushdown_remaining = plan.request.limit.unwrap_or(usize::MAX);
        for file_index in 0..plan.files.len() {
            if plan
                .hive
                .as_ref()
                .is_some_and(|hive| hive.can_prune(file_index, plan.request.predicate.as_ref()))
            {
                plan.context.metrics.add_files_pruned(1);
                continue;
            }
            if pushdown_remaining == 0 {
                break;
            }
            plan.context.check_cancelled()?;
            let file = &plan.files[file_index];
            let snapshot = plan.context.object_snapshot(file.uri())?;
            let (mut metadata, page_index_attempted) =
                if let Some(preplanned) = preplanned_metadata
                    .as_mut()
                    .and_then(|metadata| metadata.take(file_index))
                {
                    preplanned.into_parts()
                } else if let Some(metadata) = plan
                    .query_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get(file_index))
                    .filter(|_| !use_page_index && &snapshot == file.snapshot())
                {
                    (metadata.clone(), false)
                } else {
                    (
                        load_parquet_metadata(
                            file,
                            snapshot.clone(),
                            Some(&plan.context),
                            &plan.metadata_cache,
                            usize::MAX,
                        )
                        .await?,
                        false,
                    )
                };
            let reader_metadata = metadata.reader_metadata();
            let file_schema = Arc::clone(reader_metadata.schema());
            validate_scan_schema(
                file,
                &file_schema,
                &plan.physical_schema,
                plan.schema_mode,
            )?;
            if let Some(hive) = &plan.hive {
                hive.validate_physical_schema(&file_schema)?;
            }
            let projection = file_projection(&file_schema, &plan.output_schema);
            let row_filter = if plan.request.predicate_guarantee == PredicateGuarantee::Exact {
                if plan.schema_mode != ParquetSchemaMode::Strict {
                    Err(Error::Internal(
                        "exact Parquet scan reached a non-strict schema mode".to_owned(),
                    ))?
                }
                Some(ParquetRowFilter::try_new_exact(
                    plan.request.predicate.as_ref(),
                    &file_schema,
                    &plan.table_schema,
                )?)
            } else if plan.schema_mode == ParquetSchemaMode::Strict {
                ParquetRowFilter::try_new_strict(
                    plan.request.predicate.as_ref(),
                    &file_schema,
                    &plan.table_schema,
                )
            } else {
                ParquetRowFilter::try_new(
                    plan.request.predicate.as_ref(),
                    &file_schema,
                    &plan.table_schema,
                )
            };
            let row_groups = reader_metadata.metadata().num_row_groups();
            let candidate_bytes = row_groups
                .checked_mul(size_of::<usize>())
                .and_then(|bytes| bytes.checked_mul(if use_bloom { 2 } else { 1 }))
                .ok_or_else(|| Error::ResourceExhausted(format!(
                    "Parquet pruning state for {} exceeds this platform's address space",
                    file.uri(),
                )))?;
            let _candidate_reservation = plan.context.memory.try_reserve(candidate_bytes).map_err(|_| {
                Error::ResourceExhausted(format!(
                    "Parquet pruning state for {} requires {candidate_bytes} bytes, but the query memory pool has {} bytes available",
                    file.uri(),
                    plan.context.memory.available(),
                ))
            })?;
            let mut candidate_groups = Vec::with_capacity(row_groups);
            for row_group in 0..row_groups {
                if can_prune_row_group(
                    reader_metadata.metadata(),
                    reader_metadata.parquet_schema(),
                    &file_schema,
                    &plan.table_schema,
                    row_group,
                    plan.request.predicate.as_ref(),
                ) {
                    plan.context.metrics.add_row_groups_pruned(1);
                    continue;
                }
                candidate_groups.push(row_group);
            }
            if candidate_groups.is_empty() {
                plan.context.metrics.add_files_pruned(1);
                continue;
            }
            if use_bloom {
                let mut bloom_survivors = Vec::with_capacity(candidate_groups.len());
                for row_group in candidate_groups {
                    plan.context.check_cancelled()?;
                    if bloom_prunes_row_group(
                        file,
                        &metadata,
                        &file_schema,
                        &plan.table_schema,
                        row_group,
                        plan.request.predicate.as_ref(),
                        &plan.context,
                        &plan.metadata_cache,
                        &plan.pruning_budget,
                    ).await? {
                        plan.context.metrics.add_parquet_bloom_row_groups_pruned(1);
                        plan.context.metrics.add_row_groups_pruned(1);
                    } else {
                        bloom_survivors.push(row_group);
                    }
                }
                candidate_groups = bloom_survivors;
            }
            if candidate_groups.is_empty() {
                plan.context.metrics.add_files_pruned(1);
                continue;
            }
            if use_page_index && !page_index_attempted {
                metadata = load_page_index_metadata(
                    file,
                    &snapshot,
                    metadata,
                    &plan.context,
                    &plan.metadata_cache,
                    &plan.pruning_budget,
                ).await?;
            }
            let dictionary = if plan.schema_mode == ParquetSchemaMode::Strict
                && !plan.request.dictionary_columns.is_empty()
            {
                DictionaryDecode::try_new(
                    metadata.reader_metadata(),
                    &file_schema,
                    &plan.table_schema,
                    &plan.output_schema,
                    &plan.request.dictionary_columns,
                    &plan.context,
                )?
            } else {
                None
            };
            let file_plan = Arc::new(ParquetFilePlan::try_new(
                Arc::clone(&plan.files),
                file_index,
                snapshot,
                metadata,
                projection,
                row_filter,
                dictionary,
                &plan.context,
            )?);
            let native_sidecar = if plan.request.predicate_guarantee
                == PredicateGuarantee::Exact
            {
                let predicate = Arc::new(
                    plan.request
                        .predicate
                        .clone()
                        .expect("exact Native sidecar scans have a predicate"),
                );
                plan.native_predicate_sidecars
                    .as_ref()
                    .and_then(|sidecars| sidecars.get(file_index))
                    .and_then(Option::as_ref)
                    .filter(|sidecar| {
                        sidecar.supports_predicate(
                            &predicate,
                            &plan.table_schema,
                            &file_schema,
                        )
                    })
                    .map(|sidecar| {
                        let projection = if file_plan.dictionary_columns().is_empty() {
                            sidecar
                                .projection_candidate(
                                &predicate,
                                &plan.table_schema,
                                &file_schema,
                                file_plan.projection(),
                                )
                                .map(Arc::new)
                        } else {
                            None
                        };
                        NativeSidecarMorsel {
                            sidecar: sidecar.clone(),
                            projection,
                            predicate: Arc::clone(&predicate),
                            table_schema: Arc::clone(&plan.table_schema),
                        }
                    })
            } else {
                None
            };
            // Preserve bounded reader chunks. The first full-projection slice
            // handles only naturally single-row-group morsels; larger chunks
            // stay on the proven Parquet path until streaming sidecar output is
            // implemented.
            let chunk_size = row_group_chunk::size(
                plan.fixed_files,
                plan.request.limit,
                candidate_groups.len(),
                plan.predicate_cache_lanes,
            );
            let mut chunk = (chunk_size > 1).then(|| row_group_chunk::RowGroupChunk::new(chunk_size));

            let mut has_unpruned_group = false;
            for row_group in candidate_groups {
                let row_count = usize::try_from(
                    file_plan
                        .metadata()
                        .reader_metadata()
                        .metadata()
                        .row_group(row_group)
                        .num_rows(),
                ).map_err(|_| Error::Execution(format!(
                    "Parquet row group {row_group} in {} has an invalid row count",
                    file.uri(),
                )))?;
                let page_pruning = if use_page_index {
                    prune_pages(
                        file.uri(),
                        file_plan.metadata().reader_metadata().metadata(),
                        &file_schema,
                        &plan.table_schema,
                        row_group,
                        plan.request.predicate.as_ref(),
                    )?
                } else {
                    None
                };
                let (row_selection, effective_rows) = match page_pruning {
                    Some(pruning) if pruning.rows_pruned > 0 => {
                        plan.context
                            .metrics
                            .add_parquet_pages_pruned(pruning.pages_pruned);
                        plan.context
                            .metrics
                            .add_parquet_page_rows_pruned(pruning.rows_pruned);
                        if pruning.selected_rows == 0 {
                            plan.context.metrics.add_row_groups_pruned(1);
                            continue;
                        }
                        (Some(pruning.selection), pruning.selected_rows)
                    }
                    _ => (None, row_count),
                };
                let apply_row_filter = true;
                has_unpruned_group = true;
                if row_count == 0 {
                    continue;
                }
                let row_limit = reader_limit(
                    &plan.request,
                    effective_rows,
                    &mut pushdown_remaining,
                );
                let predicate_cache = parquet_predicate_cache::plan(
                    if apply_row_filter {
                        file_plan.row_filter()
                    } else {
                        None
                    },
                    file_plan.projection(),
                    &file_schema,
                    row_count,
                    plan.decode_batch_size,
                    plan.context.memory.limit(),
                    plan.predicate_cache_lanes,
                );
                if let Some(chunk) = &mut chunk {
                    debug_assert!(row_limit.is_none());
                    if !chunk.can_accept(apply_row_filter) {
                        let completed = std::mem::replace(
                            chunk,
                            row_group_chunk::RowGroupChunk::new(chunk_size),
                        );
                        let (
                            row_groups,
                            chunk_selection,
                            chunk_cache,
                            chunk_apply_row_filter,
                            chunk_sidecar_leases,
                        ) = completed.finish();
                        yield ParquetMorsel {
                            file: Arc::clone(&file_plan),
                            row_groups,
                            row_limit: None,
                            row_selection: chunk_selection,
                            predicate_cache: chunk_cache,
                            apply_row_filter: chunk_apply_row_filter,
                            sidecar_selection_leases: chunk_sidecar_leases,
                            native_sidecar: native_sidecar.clone(),
                        };
                    }
                    chunk.push(
                        row_group,
                        row_count,
                        row_selection,
                        predicate_cache,
                        apply_row_filter,
                        None,
                    )?;
                    if chunk.is_full() {
                        let completed = std::mem::replace(
                            chunk,
                            row_group_chunk::RowGroupChunk::new(chunk_size),
                        );
                        let (
                            row_groups,
                            row_selection,
                            predicate_cache,
                            apply_row_filter,
                            sidecar_selection_leases,
                        ) = completed.finish();
                        yield ParquetMorsel {
                            file: Arc::clone(&file_plan),
                            row_groups,
                            row_limit: None,
                            row_selection,
                            predicate_cache,
                            apply_row_filter,
                            sidecar_selection_leases,
                            native_sidecar: native_sidecar.clone(),
                        };
                    }
                } else {
                    yield ParquetMorsel {
                        file: Arc::clone(&file_plan),
                        row_groups: vec![row_group],
                        row_limit,
                        row_selection,
                        predicate_cache,
                        apply_row_filter,
                        sidecar_selection_leases: Vec::new(),
                        native_sidecar: native_sidecar.clone(),
                    };
                }
                if pushdown_remaining == 0 {
                    break;
                }
            }
            if let Some(chunk) = chunk
                && !chunk.is_empty()
            {
                let (
                    row_groups,
                    row_selection,
                    predicate_cache,
                    apply_row_filter,
                    sidecar_selection_leases,
                ) = chunk.finish();
                yield ParquetMorsel {
                    file: Arc::clone(&file_plan),
                    row_groups,
                    row_limit: None,
                    row_selection,
                    predicate_cache,
                    apply_row_filter,
                    sidecar_selection_leases,
                    native_sidecar: native_sidecar.clone(),
                };
            }
            if !has_unpruned_group {
                plan.context.metrics.add_files_pruned(1);
            }
        }
    })
}

fn validate_file_schema(
    uri: &str,
    actual: &Schema,
    expected: &Schema,
    mode: ParquetSchemaMode,
) -> Result<()> {
    for field in expected.fields() {
        match actual.field_with_name(field.name()) {
            Ok(actual_field)
                if canonical_type(actual_field.data_type())
                    == canonical_type(field.data_type()) => {}
            Ok(actual_field) if mode == ParquetSchemaMode::SafeWidening => {
                let actual_type = canonical_type(actual_field.data_type());
                let expected_type = canonical_type(field.data_type());
                let merged = merge_types(&actual_type, &expected_type, mode).map_err(|reason| {
                    incompatible_file_column(
                        uri,
                        field.name(),
                        &expected_type,
                        &actual_type,
                        &reason,
                    )
                })?;
                if merged != expected_type {
                    return Err(Error::InvalidArgument(format!(
                        "Parquet schema for URI '{uri}', column '{}' requires widening from {:?} to {:?}; run REFRESH TABLE",
                        field.name(),
                        expected_type,
                        merged,
                    )));
                }
            }
            Ok(actual_field) => {
                let actual_type = canonical_type(actual_field.data_type());
                let expected_type = canonical_type(field.data_type());
                return Err(incompatible_file_column(
                    uri,
                    field.name(),
                    &expected_type,
                    &actual_type,
                    &format!("types differ under {mode:?} mode"),
                ));
            }
            Err(_) if mode == ParquetSchemaMode::UnionByName => {}
            Err(_) => {
                return Err(Error::InvalidArgument(format!(
                    "Parquet schema for URI '{uri}' is missing column '{}'",
                    field.name()
                )));
            }
        }
    }
    Ok(())
}

fn incompatible_file_column(
    uri: &str,
    column: &str,
    expected: &arrow::datatypes::DataType,
    actual: &arrow::datatypes::DataType,
    reason: &str,
) -> Error {
    Error::InvalidArgument(format!(
        "incompatible Parquet schema for URI '{uri}', column '{column}': expected {expected:?}, found {actual:?}: {reason}"
    ))
}

fn validate_scan_schema(
    file: &ObjectSource,
    actual: &Schema,
    expected: &Schema,
    schema_mode: ParquetSchemaMode,
) -> Result<()> {
    validate_file_schema(file.uri(), actual, expected, schema_mode)
        .map_err(|error| Error::Execution(format!("Parquet schema changed during query: {error}")))
}

fn nullable_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn file_projection(file_schema: &Schema, output_schema: &Schema) -> Vec<usize> {
    output_schema
        .fields()
        .iter()
        .filter_map(|field| file_schema.index_of(field.name()).ok())
        .collect()
}

#[cfg(test)]
#[path = "parquet_bloom_read_tests.rs"]
mod bloom_read_tests;
#[cfg(test)]
#[path = "parquet_decimal_pruning_tests.rs"]
mod decimal_pruning_tests;
#[cfg(test)]
#[path = "parquet_deep_pruning_tests.rs"]
mod deep_pruning_tests;
#[cfg(test)]
#[path = "parquet_predicate_budget_tests.rs"]
mod predicate_budget_tests;
#[cfg(test)]
#[path = "parquet/preplan_tests.rs"]
mod preplan_tests;
#[cfg(test)]
#[path = "parquet_row_filter_q6_read_tests.rs"]
mod row_filter_q6_read_tests;
#[cfg(test)]
#[path = "parquet_row_filter_read_tests.rs"]
mod row_filter_read_tests;
#[cfg(test)]
#[path = "parquet/row_group_chunk_read_tests.rs"]
mod row_group_chunk_read_tests;
#[cfg(test)]
#[path = "parquet_tests.rs"]
mod tests;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use uuid::Uuid;

use crate::{Error, Result};

use super::{
    NativeDatabase, StagedSnapshot,
    active_wal::ActiveWal,
    disk_budget::DiskBudget,
    segment::writer::SegmentWriter,
    table::{self, DeleteVector, NativeSegment, TableSnapshot},
    write_plan::{CATALOG_HEADROOM_BYTES, NativeWritePlan, storage_limit},
};

#[cfg(test)]
mod sidecar;

const TARGET_SEGMENT_INPUT_BYTES: u64 = 256 * 1024 * 1024;

struct OpenSegment {
    id: String,
    writer: SegmentWriter,
    input_bytes: u64,
}

pub(crate) struct NativeTableWriter {
    wal: Arc<super::wal::Wal>,
    wal_owner: Option<ActiveWal>,
    staging: Option<StagedSnapshot>,
    plan: Option<NativeWritePlan>,
    current: Option<OpenSegment>,
    new_segments: Vec<NativeSegment>,
    logical_input_bytes: u64,
    disk_budget: DiskBudget,
}

pub(crate) struct PreparedSnapshot {
    pub(super) wal: Arc<super::wal::Wal>,
    pub(super) wal_owner: ActiveWal,
    pub(super) staging: StagedSnapshot,
    pub(super) snapshot: TableSnapshot,
    pub(super) name: String,
}

impl PreparedSnapshot {
    pub(crate) fn abort(mut self) -> Result<()> {
        abort_owned_transaction(self.staging, &mut self.wal_owner)
    }

    pub(in crate::storage::native) fn quota_directories(
        &self,
        root: &std::path::Path,
    ) -> Vec<std::path::PathBuf> {
        let own = self.snapshot.final_directory(root);
        self.snapshot
            .reachable_directories(root)
            .into_iter()
            .map(|directory| {
                if directory == own {
                    self.staging.snapshot_directory().to_path_buf()
                } else {
                    directory
                }
            })
            .collect()
    }
}

impl NativeTableWriter {
    pub(super) fn begin(database: &NativeDatabase, plan: NativeWritePlan) -> Result<Self> {
        let staging = StagedSnapshot::begin(database.path(), database.database_id())?;
        let wal = database.wal()?;
        if let Err(error) = wal.begin(staging.transaction_id(), plan.expected_generation) {
            return match staging.abort() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(Error::native_storage(
                    database.path(),
                    format!("{error}; native staging cleanup failed: {cleanup}"),
                )),
            };
        }
        let disk_budget = DiskBudget::new(plan.new_snapshot_limit);
        let wal_owner = ActiveWal::new(Arc::clone(&wal), staging.transaction_id());
        Ok(Self {
            wal,
            wal_owner: Some(wal_owner),
            staging: Some(staging),
            plan: Some(plan),
            current: None,
            new_segments: Vec::new(),
            logical_input_bytes: 0,
            disk_budget,
        })
    }

    pub(crate) fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.plan.as_ref().expect("writer plan exists").schema()
    }

    pub(crate) fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let schema = self.schema();
        if batch.num_columns() != schema.fields().len()
            || batch
                .columns()
                .iter()
                .zip(schema.fields())
                .any(|(array, field)| array.data_type() != field.data_type())
        {
            return Err(Error::InvalidArgument(
                "native write batch does not match the target column types".to_owned(),
            ));
        }
        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            if !field.is_nullable() && array.null_count() != 0 {
                return Err(Error::InvalidArgument(format!(
                    "native table column '{}' is NOT NULL but the input contains NULL",
                    field.name()
                )));
            }
        }
        let normalized = RecordBatch::try_new(schema, batch.columns().to_vec())?;
        let input_bytes = u64::try_from(normalized.get_array_memory_size()).unwrap_or(u64::MAX);
        let logical_input_bytes = self
            .logical_input_bytes
            .checked_add(input_bytes)
            .ok_or_else(|| {
                Error::ResourceExhausted("native input byte count overflow".to_owned())
            })?;
        if self
            .plan
            .as_ref()
            .is_some_and(|plan| plan.new_source_bytes == 0)
        {
            let limit = self
                .plan
                .as_ref()
                .expect("writer plan exists")
                .limit_for_measured_source(logical_input_bytes)?;
            self.disk_budget.raise_limit(limit);
        }
        if self
            .current
            .as_ref()
            .is_some_and(|segment| segment.input_bytes >= TARGET_SEGMENT_INPUT_BYTES)
        {
            self.finish_current()?;
        }
        if self.current.is_none() {
            self.current = Some(self.open_segment()?);
        }
        self.logical_input_bytes = logical_input_bytes;
        let encoded = super::segment::encoding::encode(&normalized, &self.schema())?;
        let current = self.current.as_mut().expect("segment was opened");
        current.writer.write_batch(&encoded)?;
        current.input_bytes = current.input_bytes.saturating_add(input_bytes);
        Ok(())
    }

    pub(crate) fn write_delete_vector(
        &mut self,
        segment_id: &str,
        vector: &DeleteVector,
    ) -> Result<()> {
        let plan = self.plan.as_mut().expect("writer plan exists");
        let segment = plan
            .inherited_segments
            .iter_mut()
            .find(|segment| segment.segment_id() == segment_id)
            .ok_or_else(|| {
                Error::Internal(format!(
                    "native UPDATE segment '{segment_id}' is absent from its write plan"
                ))
            })?;
        if vector.row_count() != segment.rows() {
            return Err(Error::Internal(format!(
                "native UPDATE vector for segment '{segment_id}' covers {} rows, expected {}",
                vector.row_count(),
                segment.rows()
            )));
        }
        let path = self
            .staging
            .as_ref()
            .expect("writer staging exists")
            .delete_vector_path(segment_id);
        let descriptor = vector.write(&path, plan.version, &plan.snapshot_id, &self.disk_budget)?;
        *segment = segment.clone().with_delete_vector(descriptor);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<PreparedSnapshot> {
        if let Err(error) = self.finish_current() {
            return self.abort_with(error);
        }
        let database_id = self.staging_database_id();
        let plan = self.plan.take().expect("writer plan exists");
        let mut segments = plan.inherited_segments.clone();
        segments.append(&mut self.new_segments);
        let measured_source = if plan.new_source_bytes == 0 {
            self.logical_input_bytes
        } else {
            plan.new_source_bytes
        };
        let source_bytes = plan
            .inherited_source_bytes
            .checked_add(measured_source)
            .ok_or_else(|| {
                Error::ResourceExhausted("native source byte count overflow".to_owned())
            });
        let source_bytes = match source_bytes {
            Ok(source_bytes) => source_bytes,
            Err(error) => return self.abort_with(error),
        };
        let snapshot = TableSnapshot::new(
            database_id,
            plan.table_id.clone(),
            plan.version,
            plan.snapshot_id.clone(),
            plan.parent.clone(),
            plan.operation,
            Arc::clone(&plan.schema),
            source_bytes,
            segments,
        );
        let mut snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => return self.abort_with(error),
        };
        let mut wal_owner = self.wal_owner.take().expect("writer WAL owner exists");
        let staging = self.staging.take().expect("writer staging exists");
        if let Err(error) = table::write_staged_with_budget(
            staging.snapshot_directory(),
            &mut snapshot,
            &self.disk_budget,
        ) {
            return abort_staging(staging, &mut wal_owner, error);
        }
        if let Err(error) = check_limits(
            &plan,
            measured_source,
            self.disk_budget.used(),
            source_bytes,
        ) {
            return abort_staging(staging, &mut wal_owner, error);
        }
        Ok(PreparedSnapshot {
            wal: Arc::clone(&self.wal),
            wal_owner,
            staging,
            snapshot,
            name: plan.name,
        })
    }

    pub(crate) fn abort(mut self) -> Result<()> {
        self.current.take();
        self.plan.take();
        let Some(staging) = self.staging.take() else {
            return Ok(());
        };
        let mut wal_owner = self.wal_owner.take().expect("writer WAL owner exists");
        abort_owned_transaction(staging, &mut wal_owner)
    }

    fn abort_with<T>(mut self, error: Error) -> Result<T> {
        self.current.take();
        self.plan.take();
        let Some(staging) = self.staging.take() else {
            return Err(error);
        };
        let mut wal_owner = self.wal_owner.take().expect("writer WAL owner exists");
        abort_staging(staging, &mut wal_owner, error)
    }

    fn open_segment(&self) -> Result<OpenSegment> {
        let id = Uuid::new_v4().to_string();
        let staging = self.staging.as_ref().expect("writer staging exists");
        Ok(OpenSegment {
            writer: SegmentWriter::create_new_without_predicate_sidecar(
                staging.segment_path(&id),
                super::segment::encoding::physical_schema(&self.schema()),
                self.schema(),
                self.disk_budget.clone(),
            )?,
            id,
            input_bytes: 0,
        })
    }

    fn finish_current(&mut self) -> Result<()> {
        let Some(segment) = self.current.take() else {
            return Ok(());
        };
        let plan = self.plan.as_ref().expect("writer plan exists");
        let metadata = segment.writer.finish()?;
        let native_segment =
            NativeSegment::new(&segment.id, plan.version, &plan.snapshot_id, metadata);
        self.new_segments.push(native_segment);
        Ok(())
    }

    fn staging_database_id(&self) -> String {
        self.staging
            .as_ref()
            .expect("writer staging exists")
            .database_id()
            .to_owned()
    }
}

fn abort_staging<T>(staging: StagedSnapshot, wal_owner: &mut ActiveWal, error: Error) -> Result<T> {
    let path = staging.path().to_owned();
    match abort_owned_transaction(staging, wal_owner) {
        Ok(()) => Err(error),
        Err(cleanup) => Err(Error::native_storage(
            path,
            format!("{error}; native staging cleanup failed: {cleanup}"),
        )),
    }
}

pub(super) fn abort_owned_transaction(
    staging: StagedSnapshot,
    wal_owner: &mut ActiveWal,
) -> Result<()> {
    let path = staging.path().to_owned();
    let staging_result = staging.abort();
    let wal_result = wal_owner.abort();
    match (staging_result, wal_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(staging), Err(wal)) => Err(Error::native_storage(
            path,
            format!("staging cleanup failed: {staging}; WAL abort also failed: {wal}"),
        )),
    }
}

pub(super) fn check_limits(
    plan: &NativeWritePlan,
    new_source_bytes: u64,
    new_snapshot_bytes: u64,
    total_source_bytes: u64,
) -> Result<()> {
    let final_bytes = plan
        .inherited_storage_bytes
        .checked_add(new_snapshot_bytes)
        .ok_or_else(|| Error::ResourceExhausted("native final byte count overflow".to_owned()))?;
    let final_with_catalog = final_bytes
        .checked_add(CATALOG_HEADROOM_BYTES)
        .ok_or_else(|| Error::ResourceExhausted("native final byte count overflow".to_owned()))?;
    let final_limit = storage_limit(total_source_bytes, 2, "final")?;
    if final_with_catalog > final_limit {
        return Err(Error::ResourceExhausted(format!(
            "native storage and catalog reserve {final_with_catalog} bytes exceeds the 2x plus metadata allowance limit of {final_limit} bytes"
        )));
    }
    let peak = plan
        .retained_old_storage_bytes
        .checked_add(plan.inherited_storage_bytes)
        .and_then(|bytes| bytes.checked_add(CATALOG_HEADROOM_BYTES))
        .and_then(|bytes| bytes.checked_add(new_source_bytes))
        .and_then(|bytes| bytes.checked_add(new_snapshot_bytes))
        .ok_or_else(|| Error::ResourceExhausted("native peak byte count overflow".to_owned()))?;
    let peak_source_bytes = plan
        .retained_old_source_bytes
        .checked_add(total_source_bytes)
        .ok_or_else(|| {
            Error::ResourceExhausted("native peak source byte count overflow".to_owned())
        })?;
    let limit = storage_limit(peak_source_bytes, 3, "peak")?;
    if peak > limit {
        return Err(Error::ResourceExhausted(format!(
            "native write peak {peak} bytes exceeds the 3x plus metadata allowance limit of {limit} bytes"
        )));
    }
    Ok(())
}

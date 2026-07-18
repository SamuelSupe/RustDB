use std::{path::PathBuf, sync::Arc};

use crate::{Error, Result};

use super::{
    NativeDatabase, StagedSnapshot,
    active_wal::ActiveWal,
    disk_budget::DiskBudget,
    table::{self, DeleteVector, TableSnapshot},
    table_writer::{PreparedSnapshot, abort_owned_transaction, check_limits},
    write_plan::NativeWritePlan,
};

pub(crate) struct NativeDeleteWriter {
    wal: Arc<super::wal::Wal>,
    wal_owner: Option<ActiveWal>,
    staging: Option<StagedSnapshot>,
    plan: Option<NativeWritePlan>,
    disk_budget: DiskBudget,
    changed: bool,
}

impl NativeDeleteWriter {
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
            disk_budget,
            changed: false,
        })
    }

    pub(crate) fn write_vector(&mut self, segment_id: &str, vector: &DeleteVector) -> Result<()> {
        let plan = self.plan.as_mut().expect("delete plan exists");
        let segment = plan
            .inherited_segments
            .iter_mut()
            .find(|segment| segment.segment_id() == segment_id)
            .ok_or_else(|| {
                Error::Internal(format!(
                    "native DELETE segment '{segment_id}' is absent from its write plan"
                ))
            })?;
        if vector.row_count() != segment.rows() {
            return Err(Error::Internal(format!(
                "native DELETE vector for segment '{segment_id}' covers {} rows, expected {}",
                vector.row_count(),
                segment.rows()
            )));
        }
        if vector.deleted_rows() == 0 {
            return Ok(());
        }
        let path = self
            .staging
            .as_ref()
            .expect("delete staging exists")
            .delete_vector_path(segment_id);
        let descriptor = vector.write(&path, plan.version, &plan.snapshot_id, &self.disk_budget)?;
        *segment = segment.clone().with_delete_vector(descriptor);
        self.changed = true;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Option<PreparedSnapshot>> {
        if !self.changed {
            self.abort()?;
            return Ok(None);
        }
        let plan = self.plan.as_ref().expect("delete plan exists");
        let database_id = self
            .staging
            .as_ref()
            .expect("delete staging exists")
            .database_id()
            .to_owned();
        let snapshot = TableSnapshot::new(
            database_id,
            plan.table_id.clone(),
            plan.version,
            plan.snapshot_id.clone(),
            plan.parent.clone(),
            plan.operation,
            Arc::clone(&plan.schema),
            plan.inherited_source_bytes,
            plan.inherited_segments.clone(),
        );
        let mut snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => return self.abort_with(error),
        };
        let plan = self.plan.take().expect("delete plan exists");
        let mut wal_owner = self.wal_owner.take().expect("delete WAL owner exists");
        let staging = self.staging.take().expect("delete staging exists");
        if let Err(error) = table::write_staged_with_budget(
            staging.snapshot_directory(),
            &mut snapshot,
            &self.disk_budget,
        ) {
            return abort_staging(staging, &mut wal_owner, error);
        }
        if let Err(error) = check_limits(
            &plan,
            0,
            self.disk_budget.used(),
            plan.inherited_source_bytes,
        ) {
            return abort_staging(staging, &mut wal_owner, error);
        }
        Ok(Some(PreparedSnapshot {
            wal: Arc::clone(&self.wal),
            wal_owner,
            staging,
            snapshot,
            name: plan.name,
        }))
    }

    pub(crate) fn abort(mut self) -> Result<()> {
        self.plan.take();
        let Some(staging) = self.staging.take() else {
            return Ok(());
        };
        let mut wal_owner = self.wal_owner.take().expect("delete WAL owner exists");
        abort_owned_transaction(staging, &mut wal_owner)
    }

    fn abort_with<T>(self, error: Error) -> Result<T> {
        let path = self
            .staging
            .as_ref()
            .map(StagedSnapshot::path)
            .map(PathBuf::from);
        match self.abort() {
            Ok(()) => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                path.as_deref()
                    .unwrap_or_else(|| std::path::Path::new("native DELETE staging")),
                format!("{error}; native DELETE staging cleanup failed: {cleanup}"),
            )),
        }
    }
}

fn abort_staging<T>(staging: StagedSnapshot, wal_owner: &mut ActiveWal, error: Error) -> Result<T> {
    let path = staging.path().to_owned();
    let cleanup = abort_owned_transaction(staging, wal_owner);
    match cleanup {
        Ok(()) => Err(error),
        Err(cleanup) => Err(Error::native_storage(
            path,
            format!("{error}; native DELETE staging cleanup failed: {cleanup}"),
        )),
    }
}

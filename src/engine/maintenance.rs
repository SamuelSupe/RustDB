use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session};
use crate::{Error, Result, command::MaintenanceCommand};

impl Session {
    pub(super) async fn execute_maintenance(
        &self,
        command: MaintenanceCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let engine = self.engine.clone();
        let message = self.engine.inner.spill_io.run(move || {
            engine.ensure_native_healthy()?;
            let _gate = engine.inner.native_commit.lock();
            engine.ensure_native_healthy()?;
            let database = engine.inner.database.as_ref().ok_or_else(|| {
                Error::Unsupported(
                    "native maintenance requires Engine::open(path, config)".to_owned(),
                )
            })?;
            match command {
                MaintenanceCommand::Checkpoint => {
                    let removed = database.checkpoint()?;
                    Ok(format!("CHECKPOINT ({removed} WAL records removed)"))
                }
                MaintenanceCommand::Vacuum { table } => {
                    let removed = database.vacuum(table.as_deref())?;
                    Ok(format!("VACUUM ({removed} snapshots removed)"))
                }
                MaintenanceCommand::Analyze { table } => {
                    let count = match table {
                        Some(table) => {
                            database.table_snapshot(&table)?;
                            1
                        }
                        None => database.table_infos().len(),
                    };
                    Ok(format!(
                        "ANALYZE ({count} tables; Native statistics are maintained at commit)"
                    ))
                }
            }
        })?;
        self.batch_result(crate::command::status(&message)?, permit, context)
    }
}

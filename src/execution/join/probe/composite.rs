use crate::{Result, runtime::QueryContext};

use super::ProbeCursor;

impl<'a> ProbeCursor<'a> {
    pub(super) async fn prepare_composite(&mut self, context: &QueryContext) -> Result<()> {
        if self.composite_probe.is_some() {
            return Ok(());
        }
        let Some(table) = self.hash_table.composite() else {
            return Ok(());
        };
        let estimate = table.probe_workspace_bytes(self.left_keys)?;
        let workspace = context
            .reserve_memory_while_holding(
                estimate,
                self.held_bytes,
                "composite Join probe row encoding",
            )
            .await?;
        let probe = {
            let _permit = context.acquire_compute().await?;
            let _active = context.scheduler.enter_lane();
            table.encode_probe(self.left_keys, workspace)?
        };
        context.metrics.observe_memory(context.memory.used());
        self.composite_probe = Some(probe);
        Ok(())
    }

    pub(super) fn composite_probe_memory_size(&self) -> usize {
        self.composite_probe
            .as_ref()
            .map_or(0, |probe| probe.memory_size())
    }

    pub(super) fn matches(&self, row: usize) -> Result<Option<&'a [u32]>> {
        match (self.hash_table.composite(), self.composite_probe.as_ref()) {
            (Some(table), Some(probe)) => {
                table.lookup(probe, self.left_keys, row, self.null_equal_keys)
            }
            (Some(_), None) => Err(crate::Error::Internal(
                "composite Join probe batch was not prepared".into(),
            )),
            (None, _) => self
                .hash_table
                .matches(self.left_keys, row, self.null_equal_keys),
        }
    }
}

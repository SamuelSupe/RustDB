use std::sync::Arc;

use crate::Result;

use super::wal::Wal;

/// Owns one active WAL transaction until it is explicitly committed,
/// transferred to restart recovery, or aborted.
pub(super) struct ActiveWal {
    wal: Arc<Wal>,
    transaction_id: String,
    armed: bool,
}

impl ActiveWal {
    pub(super) fn new(wal: Arc<Wal>, transaction_id: impl Into<String>) -> Self {
        Self {
            wal,
            transaction_id: transaction_id.into(),
            armed: true,
        }
    }

    pub(super) fn abort(&mut self) -> Result<()> {
        if !self.armed {
            return Ok(());
        }
        self.wal.abort(&self.transaction_id)?;
        self.armed = false;
        Ok(())
    }

    /// Stops automatic rollback once a durable outcome may need restart
    /// recovery to finish catalog publication.
    pub(super) fn release_to_recovery(&mut self) {
        self.armed = false;
    }
}

impl Drop for ActiveWal {
    fn drop(&mut self) {
        if self.armed
            && let Err(error) = self.abort()
        {
            tracing::error!(
                %error,
                transaction_id = %self.transaction_id,
                "failed to abort abandoned Native WAL transaction"
            );
        }
    }
}

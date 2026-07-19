use crate::{Error, NativeImportIntent, NativeImportReceipt, Result};

use super::{NativeCommit, NativeDatabase, PreparedSnapshot, quota};

impl NativeDatabase {
    pub(crate) fn check_import(
        &self,
        intent: &NativeImportIntent,
    ) -> Result<Option<NativeImportReceipt>> {
        let state = self.state.lock();
        if let Some(receipt) = state.catalog.imports().get(&intent.import_id) {
            return if receipt.matches(intent) {
                Ok(Some(receipt.clone()))
            } else {
                Err(Error::native_import_conflict(&intent.import_id))
            };
        }
        if state.tables.contains_key(&intent.table) || state.views.contains_key(&intent.table) {
            return Err(Error::Catalog(format!(
                "Native import target '{}' already exists; import never overwrites a catalog object",
                intent.table
            )));
        }
        if state.external_sources.contains_key(&intent.table) {
            return Err(Error::Catalog(format!(
                "Native import target '{}' conflicts with a persistent external source",
                intent.table
            )));
        }
        Ok(None)
    }

    pub(crate) fn import_receipt(&self, import_id: &str) -> Option<NativeImportReceipt> {
        self.state.lock().catalog.imports().get(import_id).cloned()
    }

    pub(crate) fn commit_import(
        &self,
        prepared: PreparedSnapshot,
        intent: NativeImportIntent,
    ) -> Result<NativeCommit> {
        let _publication = self.quota_publication_gate.lock();
        if let Err(error) = quota::check_prepared(self, &prepared) {
            return match prepared.abort() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(Error::native_storage(
                    self.path(),
                    format!("{error}; quota rejection cleanup failed: {cleanup}"),
                )),
            };
        }
        #[cfg(test)]
        super::quota_publication_test_hook::hit();
        super::commit::commit_import(self, prepared, intent)
    }
}

mod persistent;

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::{Result, datasource::TableProvider};

pub(crate) use persistent::{PersistentCatalog, PersistentCatalogSnapshot};

#[derive(Clone)]
pub struct TableEntry {
    name: String,
    provider: Arc<dyn TableProvider>,
}

impl TableEntry {
    pub fn new(name: impl Into<String>, provider: Arc<dyn TableProvider>) -> Self {
        Self {
            name: name.into(),
            provider,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn provider(&self) -> &Arc<dyn TableProvider> {
        &self.provider
    }
}

#[derive(Clone, Default)]
pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, TableEntry>>>,
    views: Arc<RwLock<HashMap<String, String>>>,
    persistent: Option<PersistentCatalog>,
    pinned: Option<PersistentCatalogSnapshot>,
}

impl Catalog {
    pub(crate) fn with_persistent(persistent: PersistentCatalog) -> Self {
        Self {
            persistent: Some(persistent),
            ..Self::default()
        }
    }

    /// Fixes persistent table lookup to one immutable generation.
    ///
    /// Session-local tables and views are copied as well, so one query block
    /// cannot observe a concurrent registration or view replacement.
    #[must_use]
    pub(crate) fn pin(&self) -> Self {
        if self.pinned.is_some() {
            return self.clone();
        }
        Self {
            tables: Arc::new(RwLock::new(self.tables.read().clone())),
            views: Arc::new(RwLock::new(self.views.read().clone())),
            persistent: self.persistent.clone(),
            pinned: self.persistent.as_ref().map(PersistentCatalog::snapshot),
        }
    }

    pub(crate) fn persistent_generation(&self) -> Option<u64> {
        self.persistent_snapshot()
            .map(|snapshot| snapshot.generation())
    }

    pub fn register(&self, entry: TableEntry) -> Result<()> {
        let key = normalize(entry.name());
        let mut tables = self.tables.write();
        let mut views = self.views.write();
        tables.insert(key.clone(), entry);
        views.remove(&key);
        Ok(())
    }

    pub fn unregister(&self, name: &str) -> Option<TableEntry> {
        let key = normalize(name);
        let mut tables = self.tables.write();
        let mut views = self.views.write();
        let entry = tables.remove(&key);
        views.remove(&key);
        entry
    }

    pub fn table(&self, name: &str) -> Option<TableEntry> {
        let key = normalize(name);
        self.tables.read().get(&key).cloned().or_else(|| {
            self.persistent_snapshot()
                .and_then(|snapshot| snapshot.table(&key))
        })
    }

    pub(crate) fn local_table(&self, name: &str) -> Option<TableEntry> {
        self.tables.read().get(&normalize(name)).cloned()
    }

    pub(crate) fn persistent_table(&self, name: &str) -> Option<TableEntry> {
        self.persistent_snapshot()
            .and_then(|snapshot| snapshot.table(&normalize(name)))
    }

    pub(crate) fn replace_provider(
        &self,
        name: &str,
        expected: &Arc<dyn TableProvider>,
        replacement: Arc<dyn TableProvider>,
    ) -> Result<()> {
        let key = normalize(name);
        let mut tables = self.tables.write();
        let entry = tables
            .get_mut(&key)
            .ok_or_else(|| crate::Error::Catalog(format!("table '{name}' does not exist")))?;
        if !Arc::ptr_eq(entry.provider(), expected) {
            return Err(crate::Error::Catalog(format!(
                "table '{name}' changed while it was being refreshed"
            )));
        }
        *entry = TableEntry::new(entry.name().to_owned(), replacement);
        Ok(())
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut visible = HashMap::new();
        if let Some(snapshot) = self.persistent_snapshot() {
            visible.extend(
                snapshot
                    .entries()
                    .map(|entry| (normalize(entry.name()), entry.name().to_owned())),
            );
        }
        visible.extend(
            self.tables
                .read()
                .values()
                .map(|entry| (normalize(entry.name()), entry.name().to_owned())),
        );
        let mut names = visible.into_values().collect::<Vec<_>>();
        names.sort_unstable();
        names
    }

    pub fn register_view(
        &self,
        entry: TableEntry,
        sql: impl Into<String>,
        replace: bool,
    ) -> Result<()> {
        let key = normalize(entry.name());
        let mut tables = self.tables.write();
        let mut views = self.views.write();
        let table_exists = tables.contains_key(&key);
        let view_exists = views.contains_key(&key);
        if table_exists && !view_exists {
            return Err(crate::Error::Catalog(format!(
                "cannot replace table '{}' with a view",
                entry.name()
            )));
        }
        if (table_exists || view_exists) && !replace {
            return Err(crate::Error::Catalog(format!(
                "view '{}' already exists",
                entry.name()
            )));
        }
        tables.insert(key.clone(), entry);
        views.insert(key, sql.into());
        Ok(())
    }

    pub fn drop_view(&self, name: &str) -> bool {
        let key = normalize(name);
        let mut tables = self.tables.write();
        let mut views = self.views.write();
        if views.remove(&key).is_none() {
            return false;
        }
        tables.remove(&key);
        true
    }

    #[cfg(test)]
    pub fn is_view(&self, name: &str) -> bool {
        self.views.read().contains_key(&normalize(name))
    }

    fn persistent_snapshot(&self) -> Option<PersistentCatalogSnapshot> {
        self.pinned
            .clone()
            .or_else(|| self.persistent.as_ref().map(PersistentCatalog::snapshot))
    }
}

fn normalize(name: &str) -> String {
    name.to_ascii_lowercase()
}

#[cfg(test)]
mod tests;

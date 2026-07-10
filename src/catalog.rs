use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::{Result, datasource::TableProvider};

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
}

impl Catalog {
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
        self.tables.read().get(&normalize(name)).cloned()
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .tables
            .read()
            .values()
            .map(|entry| entry.name().to_owned())
            .collect();
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
}

fn normalize(name: &str) -> String {
    name.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::{Catalog, normalize};

    #[test]
    fn catalog_keys_are_ascii_case_insensitive() {
        assert_eq!(normalize("Orders"), "orders");
    }

    #[test]
    fn dropping_unknown_view_does_not_remove_tables() {
        let catalog = Catalog::default();
        assert!(!catalog.drop_view("external"));
        assert!(!catalog.is_view("external"));
    }
}

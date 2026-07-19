use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use super::{TableEntry, normalize};
use crate::{Error, Result};

#[derive(Clone)]
pub(crate) struct PersistentCatalog {
    current: Arc<RwLock<PersistentCatalogSnapshot>>,
}

impl Default for PersistentCatalog {
    fn default() -> Self {
        Self {
            current: Arc::new(RwLock::new(PersistentCatalogSnapshot::empty())),
        }
    }
}

impl PersistentCatalog {
    pub(crate) fn new(
        generation: u64,
        entries: impl IntoIterator<Item = TableEntry>,
    ) -> Result<Self> {
        Ok(Self {
            current: Arc::new(RwLock::new(PersistentCatalogSnapshot::new(
                generation, entries,
            )?)),
        })
    }

    #[cfg(test)]
    pub(crate) fn generation(&self) -> u64 {
        self.current.read().generation()
    }

    pub(crate) fn snapshot(&self) -> PersistentCatalogSnapshot {
        self.current.read().clone()
    }

    /// Atomically replaces the visible persistent table set.
    ///
    /// The expected generation makes concurrent manifest publishers fail
    /// instead of silently overwriting a newer catalog generation.
    pub(crate) fn publish(
        &self,
        expected_generation: u64,
        entries: impl IntoIterator<Item = TableEntry>,
    ) -> Result<u64> {
        let next_generation = expected_generation
            .checked_add(1)
            .ok_or_else(|| Error::Catalog("persistent catalog generation overflowed".to_owned()))?;
        let next = PersistentCatalogSnapshot::new(next_generation, entries)?;
        let mut current = self.current.write();
        if current.generation() != expected_generation {
            return Err(Error::Catalog(format!(
                "persistent catalog changed while publishing generation {next_generation}: expected generation {expected_generation}, found {}",
                current.generation()
            )));
        }
        *current = next;
        Ok(next_generation)
    }

    /// Replaces auxiliary catalog entries without advancing the Native table
    /// generation. Callers serialize this with Native commit publication.
    pub(crate) fn replace(
        &self,
        expected_generation: u64,
        entries: impl IntoIterator<Item = TableEntry>,
    ) -> Result<()> {
        let next = PersistentCatalogSnapshot::new(expected_generation, entries)?;
        let mut current = self.current.write();
        if current.generation() != expected_generation {
            return Err(Error::Catalog(format!(
                "persistent catalog changed while replacing auxiliary entries: expected generation {expected_generation}, found {}",
                current.generation()
            )));
        }
        *current = next;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct PersistentCatalogSnapshot {
    generation: u64,
    tables: Arc<HashMap<String, TableEntry>>,
}

impl PersistentCatalogSnapshot {
    fn empty() -> Self {
        Self {
            generation: 0,
            tables: Arc::new(HashMap::new()),
        }
    }

    fn new(generation: u64, entries: impl IntoIterator<Item = TableEntry>) -> Result<Self> {
        let mut tables = HashMap::new();
        for entry in entries {
            let key = normalize(entry.name());
            if tables.insert(key, entry.clone()).is_some() {
                return Err(Error::Catalog(format!(
                    "persistent catalog contains duplicate table '{}'",
                    entry.name()
                )));
            }
        }
        Ok(Self {
            generation,
            tables: Arc::new(tables),
        })
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn table(&self, normalized_name: &str) -> Option<TableEntry> {
        self.tables.get(normalized_name).cloned()
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = &TableEntry> {
        self.tables.values()
    }
}

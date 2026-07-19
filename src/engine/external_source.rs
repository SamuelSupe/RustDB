use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::{Engine, Session};
use crate::{
    CsvOptions, Error, ExternalSourceDefinition, ParquetOptions, Result,
    datasource::{RegisteredCsvTable, RegisteredParquetTable},
    external_source::StoredExternalSource,
    storage::NativeDatabase,
};

#[path = "external_source/catalog.rs"]
mod catalog;
pub(super) use catalog::persistent_entries;
use catalog::{install_catalog, refresh_source};

impl Engine {
    pub async fn add_external_csv<I, S>(
        &self,
        name: impl Into<String>,
        locations: I,
        options: CsvOptions,
    ) -> Result<SchemaRef>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.session()
            .add_external_csv(name, locations, options)
            .await
    }

    pub async fn add_external_parquet<I, S>(
        &self,
        name: impl Into<String>,
        locations: I,
        options: ParquetOptions,
    ) -> Result<SchemaRef>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.session()
            .add_external_parquet(name, locations, options)
            .await
    }

    pub fn list_external_sources(&self) -> Vec<ExternalSourceDefinition> {
        self.inner
            .database
            .as_ref()
            .map(|database| {
                database
                    .external_source_definitions()
                    .into_iter()
                    .map(|source| source.definition())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn remove_external_source(&self, name: &str) -> Result<bool> {
        self.session().remove_external_source(name)
    }

    pub async fn refresh_external_source(&self, name: &str) -> Result<SchemaRef> {
        self.session().refresh_external_source(name).await
    }
}

impl Session {
    pub async fn add_external_csv<I, S>(
        &self,
        name: impl Into<String>,
        locations: I,
        options: CsvOptions,
    ) -> Result<SchemaRef>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let name = normalized_name(&name.into())?;
        self.reject_local_shadow(&name)?;
        let locations = locations.into_iter().map(Into::into).collect::<Vec<_>>();
        let provider = RegisteredCsvTable::try_new(
            locations.clone(),
            options.clone(),
            &self.engine.inner.config,
        )
        .await?;
        let (schema, physical_schema, has_header) = provider.persisted_state();
        let source = StoredExternalSource::Csv {
            name,
            locations,
            options,
            schema: Arc::clone(&schema),
            physical_schema,
            has_header,
        };
        self.install_new_source(source)?;
        Ok(schema)
    }

    pub async fn add_external_parquet<I, S>(
        &self,
        name: impl Into<String>,
        locations: I,
        options: ParquetOptions,
    ) -> Result<SchemaRef>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let name = normalized_name(&name.into())?;
        self.reject_local_shadow(&name)?;
        let locations = locations.into_iter().map(Into::into).collect::<Vec<_>>();
        let provider = RegisteredParquetTable::try_new(
            locations.clone(),
            options.clone(),
            &self.engine.inner.config,
            self.engine.inner.metadata_cache.clone(),
        )
        .await?;
        let (schema, physical_schema) = provider.persisted_state();
        let source = StoredExternalSource::Parquet {
            name,
            locations,
            options,
            schema: Arc::clone(&schema),
            physical_schema,
        };
        self.install_new_source(source)?;
        Ok(schema)
    }

    pub fn list_external_sources(&self) -> Vec<ExternalSourceDefinition> {
        self.engine.list_external_sources()
    }

    pub fn remove_external_source(&self, name: &str) -> Result<bool> {
        let name = normalized_name(name)?;
        self.engine.ensure_native_healthy()?;
        let _gate = self.engine.inner.native_commit.lock();
        let database = native_database(&self.engine)?;
        let removed = database
            .remove_external_source(&name)
            .inspect_err(|error| {
                poison_ambiguous_publication(&self.engine, error);
            })?;
        if removed {
            install_catalog(&self.engine, database)?;
        }
        Ok(removed)
    }

    pub async fn refresh_external_source(&self, name: &str) -> Result<SchemaRef> {
        let name = normalized_name(name)?;
        self.engine.ensure_native_healthy()?;
        let database = native_database(&self.engine)?;
        let (generation, current) = database
            .external_source_snapshot(&name)
            .ok_or_else(|| Error::Catalog(format!("external source '{name}' does not exist")))?;
        let (replacement, schema) = refresh_source(&self.engine, current).await?;
        let _gate = self.engine.inner.native_commit.lock();
        self.engine.ensure_native_healthy()?;
        let database = native_database(&self.engine)?;
        database
            .replace_external_source(generation, replacement)
            .inspect_err(|error| {
                poison_ambiguous_publication(&self.engine, error);
            })?;
        install_catalog(&self.engine, database)?;
        Ok(schema)
    }

    pub async fn execute_http_read_only(&self, sql: &str) -> Result<super::QueryResult> {
        crate::HttpReadOnlyPolicy::validate(sql)?;
        self.execute_http_read_only_direct(sql).await
    }

    pub(crate) async fn execute_http_read_only_with_memory_limit(
        &self,
        sql: &str,
        memory_limit: usize,
    ) -> Result<super::QueryResult> {
        crate::HttpReadOnlyPolicy::validate(sql)?;
        self.execute_http_read_only_direct_with_memory_limit(sql, Some(memory_limit))
            .await
    }

    fn install_new_source(&self, source: StoredExternalSource) -> Result<()> {
        self.engine.ensure_native_healthy()?;
        let _gate = self.engine.inner.native_commit.lock();
        self.engine.ensure_native_healthy()?;
        let database = native_database(&self.engine)?;
        database.add_external_source(source).inspect_err(|error| {
            poison_ambiguous_publication(&self.engine, error);
        })?;
        install_catalog(&self.engine, database)
    }

    fn reject_local_shadow(&self, name: &str) -> Result<()> {
        if self.catalog.local_table(name).is_some() {
            return Err(Error::Catalog(format!(
                "session-local table or view '{name}' already exists"
            )));
        }
        Ok(())
    }
}

fn native_database(engine: &Engine) -> Result<&NativeDatabase> {
    engine.inner.database.as_deref().ok_or_else(|| {
        Error::Unsupported(
            "persistent external sources require Engine::open(path, config)".to_owned(),
        )
    })
}

fn normalized_name(name: &str) -> Result<String> {
    Ok(crate::catalog_name::local(name, "external source")?.to_ascii_lowercase())
}

fn poison_ambiguous_publication(engine: &Engine, error: &Error) {
    if matches!(error, Error::CommitOutcomeUnknown { .. }) {
        engine
            .inner
            .native_poisoned
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
#[path = "external_source/tests.rs"]
mod tests;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::Engine;
use crate::{
    Error, Result, TableEntry,
    datasource::{MetadataCache, NativeSegmentTable, RegisteredCsvTable, RegisteredParquetTable},
    external_source::StoredExternalSource,
    storage::NativeDatabase,
};

pub(crate) fn persistent_entries(
    config: &crate::EngineConfig,
    metadata_cache: MetadataCache,
    database: &NativeDatabase,
) -> Result<Vec<TableEntry>> {
    let mut entries = database
        .table_snapshots()
        .into_iter()
        .map(|(name, snapshot)| {
            let provider =
                NativeSegmentTable::new(database.path(), snapshot, config, metadata_cache.clone());
            TableEntry::new(name, Arc::new(provider))
        })
        .chain(database.view_definitions().into_iter().map(|(name, view)| {
            let provider = crate::command::ViewTable::persistent(
                name.clone(),
                view.sql().to_owned(),
                view.schema(),
                config.clone(),
                metadata_cache.clone(),
            );
            TableEntry::new(name, Arc::new(provider))
        }))
        .collect::<Vec<_>>();
    for source in database.external_source_definitions() {
        entries.push(source_entry(config, metadata_cache.clone(), source)?);
    }
    Ok(entries)
}

pub(super) async fn refresh_source(
    engine: &Engine,
    source: StoredExternalSource,
) -> Result<(StoredExternalSource, SchemaRef)> {
    match source {
        StoredExternalSource::Csv {
            name,
            locations,
            options,
            schema,
            ..
        } => {
            let provider = RegisteredCsvTable::refresh_persisted(
                locations.clone(),
                options.clone(),
                &engine.inner.config,
                &schema,
            )
            .await?;
            let (schema, physical_schema, has_header) = provider.persisted_state();
            Ok((
                StoredExternalSource::Csv {
                    name,
                    locations,
                    options,
                    schema: Arc::clone(&schema),
                    physical_schema,
                    has_header,
                },
                schema,
            ))
        }
        StoredExternalSource::Parquet {
            name,
            locations,
            options,
            schema,
            physical_schema,
        } => {
            let provider = RegisteredParquetTable::refresh_persisted(
                locations.clone(),
                options.clone(),
                &engine.inner.config,
                engine.inner.metadata_cache.clone(),
                &schema,
                &physical_schema,
            )
            .await?;
            let (schema, physical_schema) = provider.persisted_state();
            Ok((
                StoredExternalSource::Parquet {
                    name,
                    locations,
                    options,
                    schema: Arc::clone(&schema),
                    physical_schema,
                },
                schema,
            ))
        }
    }
}

pub(super) fn install_catalog(engine: &Engine, database: &NativeDatabase) -> Result<()> {
    let entries = persistent_entries(
        &engine.inner.config,
        engine.inner.metadata_cache.clone(),
        database,
    )?;
    if let Err(error) = engine
        .inner
        .persistent_catalog
        .replace(database.catalog_generation(), entries)
    {
        engine
            .inner
            .native_poisoned
            .store(true, std::sync::atomic::Ordering::Release);
        return Err(Error::native_storage(
            database.path(),
            format!(
                "external source was persisted but could not be installed; reopen the engine: {error}"
            ),
        ));
    }
    Ok(())
}

fn source_entry(
    config: &crate::EngineConfig,
    metadata_cache: MetadataCache,
    source: StoredExternalSource,
) -> Result<TableEntry> {
    Ok(match source {
        StoredExternalSource::Csv {
            name,
            locations,
            options,
            schema,
            physical_schema,
            has_header,
        } => {
            let provider = RegisteredCsvTable::from_persisted(
                locations,
                options,
                config,
                schema,
                physical_schema,
                has_header,
            );
            TableEntry::new(name, Arc::new(provider))
        }
        StoredExternalSource::Parquet {
            name,
            locations,
            options,
            schema,
            physical_schema,
        } => {
            let provider = RegisteredParquetTable::from_persisted(
                locations,
                options,
                config,
                metadata_cache,
                schema,
                physical_schema,
            )?;
            TableEntry::new(name, Arc::new(provider))
        }
    })
}

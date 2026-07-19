use std::{collections::BTreeMap, path::Path};

use arrow::datatypes::{Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result, external_source::StoredExternalSource};

use super::{io, schema};

mod options;
use options::{
    StoredCsvOptions, StoredParquetOptions, decode_csv_options, decode_parquet_options,
    encode_csv_options, encode_parquet_options,
};

pub(super) const FILE_NAME: &str = "external-sources.json";
const FORMAT_VERSION: u32 = 1;
const MAX_FILE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct State {
    pub(super) generation: u64,
    pub(super) sources: BTreeMap<String, StoredExternalSource>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredCatalog {
    database_id: String,
    format_version: u32,
    generation: u64,
    sources: BTreeMap<String, StoredSource>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
enum StoredSource {
    Csv {
        locations: Vec<String>,
        options: StoredCsvOptions,
        schema: StoredSchema,
        physical_schema: StoredSchema,
        has_header: bool,
    },
    Parquet {
        locations: Vec<String>,
        options: StoredParquetOptions,
        schema: StoredSchema,
        physical_schema: StoredSchema,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredSchema {
    ipc_hex: String,
    sha256: String,
}

impl StoredSchema {
    pub(super) fn from_schema(value: &Schema) -> Self {
        let bytes = schema::encode(value);
        Self {
            ipc_hex: schema::encode_hex(&bytes),
            sha256: schema::sha256(&bytes),
        }
    }

    pub(super) fn decode(&self, path: &Path) -> Result<SchemaRef> {
        schema::decode(path, &self.ipc_hex, &self.sha256)
    }
}

pub(super) fn path(root: &Path) -> std::path::PathBuf {
    root.join("catalog").join(FILE_NAME)
}

pub(super) fn load(root: &Path, database_id: &str) -> Result<State> {
    let path = path(root);
    let bytes = match io::read_bounded(&path, MAX_FILE_BYTES, "external source catalog") {
        Ok(bytes) => bytes,
        Err(Error::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(State {
                generation: 0,
                sources: BTreeMap::new(),
            });
        }
        Err(error) => return Err(error),
    };
    let catalog: StoredCatalog = serde_json::from_slice(&bytes).map_err(|error| {
        Error::native_storage(&path, format!("invalid external source catalog: {error}"))
    })?;
    if catalog.database_id != database_id {
        return Err(Error::native_storage(
            path,
            "external source catalog belongs to another database",
        ));
    }
    if catalog.format_version != FORMAT_VERSION {
        return Err(Error::native_storage(
            path,
            format!(
                "unsupported external source catalog version {}",
                catalog.format_version
            ),
        ));
    }
    let sources = catalog
        .sources
        .into_iter()
        .map(|(name, source)| decode_source(&path, name, source))
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(State {
        generation: catalog.generation,
        sources,
    })
}

pub(super) fn store(
    root: &Path,
    database_id: &str,
    generation: u64,
    sources: &BTreeMap<String, StoredExternalSource>,
) -> Result<u64> {
    let next_generation = generation.checked_add(1).ok_or_else(|| {
        Error::ResourceExhausted("external source generation counter is exhausted".to_owned())
    })?;
    let path = path(root);
    let catalog = StoredCatalog {
        database_id: database_id.to_owned(),
        format_version: FORMAT_VERSION,
        generation: next_generation,
        sources: sources
            .iter()
            .map(|(name, source)| {
                encode_source(&path, name, source).map(|value| (name.clone(), value))
            })
            .collect::<Result<_>>()?,
    };
    let bytes = io::encode_json_bounded(
        &path,
        &catalog,
        MAX_FILE_BYTES,
        "external source catalog",
        true,
        true,
    )?;
    let transaction_id = Uuid::new_v4().to_string();
    let publication = if path.exists() {
        io::atomic_replace(&path, &bytes, &transaction_id)
    } else {
        io::atomic_create(&path, &bytes)
    };
    if let Err(error) = publication {
        if load(root, database_id).is_ok_and(|persisted| {
            persisted.generation == next_generation && persisted.sources == *sources
        }) {
            return Err(Error::commit_outcome_unknown(
                &path,
                transaction_id,
                format!(
                    "external source catalog may have been published but durability could not be confirmed: {error}"
                ),
            ));
        }
        return Err(error);
    }
    Ok(next_generation)
}

pub(super) fn validate_location(path: &Path, location: &str) -> Result<()> {
    if location.is_empty() || location.contains('\0') {
        return Err(Error::native_storage(
            path,
            "external source location is empty or contains NUL",
        ));
    }
    let Some((scheme, _)) = location.split_once("://") else {
        return Ok(());
    };
    if !matches!(scheme.to_ascii_lowercase().as_str(), "file" | "s3") {
        return Err(Error::native_storage(
            path,
            format!("unsupported external source URI scheme '{scheme}'"),
        ));
    }
    let url = url::Url::parse(location).map_err(|error| {
        Error::native_storage(path, format!("invalid external source URI: {error}"))
    })?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::native_storage(
            path,
            "external source URIs cannot contain credentials, query parameters, or fragments",
        ));
    }
    Ok(())
}

fn encode_source(path: &Path, name: &str, source: &StoredExternalSource) -> Result<StoredSource> {
    validate_name(path, name)?;
    match source {
        StoredExternalSource::Csv {
            name: stored_name,
            locations,
            options,
            schema,
            physical_schema,
            has_header,
        } => {
            validate_identity(path, name, stored_name, locations)?;
            Ok(StoredSource::Csv {
                locations: locations.clone(),
                options: encode_csv_options(options),
                schema: StoredSchema::from_schema(schema),
                physical_schema: StoredSchema::from_schema(physical_schema),
                has_header: *has_header,
            })
        }
        StoredExternalSource::Parquet {
            name: stored_name,
            locations,
            options,
            schema,
            physical_schema,
        } => {
            validate_identity(path, name, stored_name, locations)?;
            options.effective_schema_mode()?;
            Ok(StoredSource::Parquet {
                locations: locations.clone(),
                options: encode_parquet_options(options),
                schema: StoredSchema::from_schema(schema),
                physical_schema: StoredSchema::from_schema(physical_schema),
            })
        }
    }
}

fn decode_source(
    path: &Path,
    name: String,
    source: StoredSource,
) -> Result<(String, StoredExternalSource)> {
    validate_name(path, &name)?;
    let source = match source {
        StoredSource::Csv {
            locations,
            options,
            schema,
            physical_schema,
            has_header,
        } => {
            validate_identity(path, &name, &name, &locations)?;
            StoredExternalSource::Csv {
                name: name.clone(),
                locations,
                options: decode_csv_options(path, options)?,
                schema: schema.decode(path)?,
                physical_schema: physical_schema.decode(path)?,
                has_header,
            }
        }
        StoredSource::Parquet {
            locations,
            options,
            schema,
            physical_schema,
        } => {
            validate_identity(path, &name, &name, &locations)?;
            let options = decode_parquet_options(path, options)?;
            options.effective_schema_mode()?;
            StoredExternalSource::Parquet {
                name: name.clone(),
                locations,
                options,
                schema: schema.decode(path)?,
                physical_schema: physical_schema.decode(path)?,
            }
        }
    };
    Ok((name, source))
}

fn validate_identity(
    path: &Path,
    key: &str,
    stored_name: &str,
    locations: &[String],
) -> Result<()> {
    if key != stored_name {
        return Err(Error::native_storage(
            path,
            "external source key and name differ",
        ));
    }
    if locations.is_empty() {
        return Err(Error::native_storage(
            path,
            format!("external source '{key}' has no locations"),
        ));
    }
    for location in locations {
        validate_location(path, location)?;
    }
    Ok(())
}

fn validate_name(path: &Path, name: &str) -> Result<()> {
    let normalized = crate::catalog_name::local(name, "external source")?.to_ascii_lowercase();
    if normalized != name || name.len() > 255 {
        return Err(Error::native_storage(
            path,
            format!("external source name '{name}' is not normalized"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_location;

    #[test]
    fn persisted_locations_cannot_embed_credentials() {
        let path = std::path::Path::new("external-sources.json");
        for location in [
            "s3://user:secret@bucket/data.parquet",
            "s3://bucket/data.parquet?token=secret",
            "file://user:secret@localhost/data.csv",
            "https://example.test/data.csv",
        ] {
            assert!(validate_location(path, location).is_err(), "{location}");
        }
        for location in [
            "data/*.csv",
            "file:///data/events.csv",
            "s3://bucket/data/*.parquet",
        ] {
            validate_location(path, location).unwrap();
        }
    }
}

use arrow::datatypes::SchemaRef;

use crate::{CsvOptions, ParquetOptions};

/// A CSV or Parquet source stored in a Native database catalog.
///
/// Definitions contain locations and format options only. S3 credentials and
/// endpoint configuration remain process configuration and are never stored.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ExternalSourceDefinition {
    Csv {
        name: String,
        locations: Vec<String>,
        options: CsvOptions,
        schema: SchemaRef,
    },
    Parquet {
        name: String,
        locations: Vec<String>,
        options: ParquetOptions,
        schema: SchemaRef,
    },
}

impl ExternalSourceDefinition {
    pub fn name(&self) -> &str {
        match self {
            Self::Csv { name, .. } | Self::Parquet { name, .. } => name,
        }
    }

    pub fn locations(&self) -> &[String] {
        match self {
            Self::Csv { locations, .. } | Self::Parquet { locations, .. } => locations,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Csv { schema, .. } | Self::Parquet { schema, .. } => schema.clone(),
        }
    }

    pub fn format(&self) -> &'static str {
        match self {
            Self::Csv { .. } => "csv",
            Self::Parquet { .. } => "parquet",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum StoredExternalSource {
    Csv {
        name: String,
        locations: Vec<String>,
        options: CsvOptions,
        schema: SchemaRef,
        physical_schema: SchemaRef,
        has_header: bool,
    },
    Parquet {
        name: String,
        locations: Vec<String>,
        options: ParquetOptions,
        schema: SchemaRef,
        physical_schema: SchemaRef,
    },
}

impl StoredExternalSource {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Csv { name, .. } | Self::Parquet { name, .. } => name,
        }
    }

    pub(crate) fn definition(&self) -> ExternalSourceDefinition {
        match self {
            Self::Csv {
                name,
                locations,
                options,
                schema,
                ..
            } => ExternalSourceDefinition::Csv {
                name: name.clone(),
                locations: locations.clone(),
                options: options.clone(),
                schema: schema.clone(),
            },
            Self::Parquet {
                name,
                locations,
                options,
                schema,
                ..
            } => ExternalSourceDefinition::Parquet {
                name: name.clone(),
                locations: locations.clone(),
                options: options.clone(),
                schema: schema.clone(),
            },
        }
    }
}

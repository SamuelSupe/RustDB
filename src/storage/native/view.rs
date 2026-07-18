use std::{path::Path, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{Error, Result};

use super::manifest::ViewReference;

#[derive(Clone, Debug)]
pub(crate) struct NativeView {
    version: u64,
    sql: Arc<str>,
    schema: SchemaRef,
    reference: ViewReference,
}

impl NativeView {
    pub(super) fn load(root: &Path, reference: &ViewReference) -> Result<Self> {
        let schema = reference.schema(root)?;
        Ok(Self {
            version: reference.version(),
            sql: Arc::from(reference.sql()),
            schema,
            reference: reference.clone(),
        })
    }

    pub(crate) fn new(version: u64, sql: String, schema: SchemaRef) -> Result<Self> {
        if version == 0 {
            return Err(Error::InvalidArgument(
                "persistent view version must be positive".to_owned(),
            ));
        }
        if sql.is_empty() || sql.len() > super::manifest::MAX_VIEW_SQL_BYTES {
            return Err(Error::InvalidArgument(
                "persistent view SQL is empty or too large".to_owned(),
            ));
        }
        let reference = ViewReference::new(version, sql.clone(), schema.as_ref());
        Ok(Self {
            version,
            sql: Arc::from(sql),
            schema,
            reference,
        })
    }

    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(super) fn reference(&self) -> ViewReference {
        self.reference.clone()
    }
}

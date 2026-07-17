use std::{collections::HashSet, sync::Arc};

use arrow::datatypes::{DataType, SchemaRef};
use uuid::Uuid;

use crate::{Error, Result};

use super::{
    NativeDatabase,
    manifest::{self, TableReference},
    table::{NativeSegment, SnapshotOperation},
};

pub(super) const CATALOG_HEADROOM_BYTES: u64 =
    (manifest::CATALOG_GENERATION_BUDGET_PER_TABLE_BYTES * 2) as u64;
pub(super) const FIXED_METADATA_ALLOWANCE_BYTES: u64 = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeWriteMode {
    Create,
    Replace,
    Append,
}

pub(crate) struct NativeWritePlan {
    pub(super) name: String,
    pub(super) expected_generation: u64,
    pub(super) table_id: String,
    pub(super) version: u64,
    pub(super) snapshot_id: String,
    pub(super) parent: Option<TableReference>,
    pub(super) operation: SnapshotOperation,
    pub(super) schema: SchemaRef,
    pub(super) inherited_source_bytes: u64,
    pub(super) new_source_bytes: u64,
    pub(super) retained_old_source_bytes: u64,
    pub(super) retained_old_storage_bytes: u64,
    pub(super) inherited_storage_bytes: u64,
    pub(super) new_snapshot_limit: u64,
    pub(super) inherited_segments: Vec<NativeSegment>,
}

impl NativeWritePlan {
    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(super) fn limit_for_measured_source(&self, new_source_bytes: u64) -> Result<u64> {
        new_snapshot_limit(
            self.inherited_source_bytes,
            new_source_bytes,
            self.inherited_storage_bytes,
            self.retained_old_source_bytes,
            self.retained_old_storage_bytes,
        )
    }
}

pub(super) fn plan(
    database: &NativeDatabase,
    name: &str,
    mode: NativeWriteMode,
    expected_generation: u64,
    source_schema: SchemaRef,
    new_source_bytes: u64,
) -> Result<NativeWritePlan> {
    let normalized = name.to_ascii_lowercase();
    if normalized.is_empty()
        || normalized != name
        || normalized.len() > manifest::MAX_TABLE_NAME_BYTES
    {
        return Err(Error::Catalog(format!(
            "native table name must be normalized, non-empty, and at most {} UTF-8 bytes",
            manifest::MAX_TABLE_NAME_BYTES
        )));
    }
    validate_schema(&source_schema)?;
    let state = database.state.lock();
    if state.catalog.generation() != expected_generation {
        return Err(Error::Catalog(format!(
            "catalog generation changed: expected {expected_generation}, found {}",
            state.catalog.generation()
        )));
    }
    let existing = state.tables.get(&normalized);
    let (
        table_id,
        version,
        parent,
        operation,
        schema,
        inherited_source_bytes,
        retained_old_source_bytes,
        retained_old_storage_bytes,
        inherited_storage_bytes,
        inherited_segments,
    ) = match (mode, existing) {
        (NativeWriteMode::Create, Some(_)) => {
            return Err(Error::Catalog(format!("table '{name}' already exists")));
        }
        (NativeWriteMode::Append, None) => {
            return Err(Error::Catalog(format!(
                "native table '{name}' does not exist"
            )));
        }
        (NativeWriteMode::Create | NativeWriteMode::Replace, None) => (
            Uuid::new_v4().to_string(),
            1,
            None,
            SnapshotOperation::Import,
            source_schema,
            0,
            0,
            0,
            0,
            Vec::new(),
        ),
        (NativeWriteMode::Replace, Some(snapshot)) => (
            snapshot.table_id().to_owned(),
            next_version(snapshot.version())?,
            Some(snapshot.table_reference()),
            SnapshotOperation::Replace,
            source_schema,
            0,
            snapshot.source_bytes(),
            snapshot.storage_bytes(),
            0,
            Vec::new(),
        ),
        (NativeWriteMode::Append, Some(snapshot)) => {
            validate_append_schema(&source_schema, &snapshot.schema())?;
            (
                snapshot.table_id().to_owned(),
                next_version(snapshot.version())?,
                Some(snapshot.table_reference()),
                SnapshotOperation::Append,
                snapshot.schema(),
                snapshot.source_bytes(),
                0,
                0,
                snapshot.storage_bytes(),
                snapshot.segments().to_vec(),
            )
        }
    };
    let snapshot_id = Uuid::new_v4().to_string();
    manifest::validate_planned_update(
        database.path(),
        &state.catalog,
        &normalized,
        TableReference::new(
            table_id.clone(),
            version,
            snapshot_id.clone(),
            "0".repeat(64),
        ),
    )?;
    let retired_source_bytes = super::retired_source_bytes(&state, &table_id)?;
    let retained_old_source_bytes = retained_old_source_bytes
        .checked_add(retired_source_bytes)
        .ok_or_else(|| {
            Error::ResourceExhausted("retired native source byte count overflow".to_owned())
        })?;
    let retired_storage_bytes = super::retired_storage_bytes(database.path(), &state, &table_id)?;
    let retained_old_storage_bytes = retained_old_storage_bytes
        .checked_add(retired_storage_bytes)
        .ok_or_else(|| {
            Error::ResourceExhausted("retired native storage byte count overflow".to_owned())
        })?;
    let new_snapshot_limit = new_snapshot_limit(
        inherited_source_bytes,
        new_source_bytes,
        inherited_storage_bytes,
        retained_old_source_bytes,
        retained_old_storage_bytes,
    )?;
    Ok(NativeWritePlan {
        name: normalized,
        expected_generation,
        table_id,
        version,
        snapshot_id,
        parent,
        operation,
        schema,
        inherited_source_bytes,
        new_source_bytes,
        retained_old_source_bytes,
        retained_old_storage_bytes,
        inherited_storage_bytes,
        new_snapshot_limit,
        inherited_segments,
    })
}

fn new_snapshot_limit(
    inherited_source_bytes: u64,
    new_source_bytes: u64,
    inherited_storage_bytes: u64,
    retained_old_source_bytes: u64,
    retained_old_storage_bytes: u64,
) -> Result<u64> {
    let source_bytes = inherited_source_bytes
        .checked_add(new_source_bytes)
        .ok_or_else(|| Error::ResourceExhausted("native source byte count overflow".to_owned()))?;
    let final_limit = storage_limit(source_bytes, 2, "final")?;
    let final_remaining = final_limit.checked_sub(inherited_storage_bytes).ok_or_else(|| {
        Error::ResourceExhausted(format!(
            "existing native storage {inherited_storage_bytes} bytes already exceeds the 2x plus metadata allowance limit of {final_limit} bytes"
        ))
    })?;
    let peak_source_bytes = source_bytes
        .checked_add(retained_old_source_bytes)
        .ok_or_else(|| {
            Error::ResourceExhausted("native peak source byte count overflow".to_owned())
        })?;
    let peak_limit = storage_limit(peak_source_bytes, 3, "peak")?;
    let peak_used = retained_old_storage_bytes
        .checked_add(inherited_storage_bytes)
        .ok_or_else(|| Error::ResourceExhausted("native peak byte count overflow".to_owned()))?
        .checked_add(new_source_bytes)
        .ok_or_else(|| Error::ResourceExhausted("native peak byte count overflow".to_owned()))?;
    let peak_remaining = peak_limit.checked_sub(peak_used).ok_or_else(|| {
        Error::ResourceExhausted(format!(
            "native write requires {peak_used} bytes before creating a new snapshot, exceeding the 3x plus metadata allowance limit of {peak_limit} bytes"
        ))
    })?;
    final_remaining
        .min(peak_remaining)
        .checked_sub(CATALOG_HEADROOM_BYTES)
        .ok_or_else(|| {
            Error::ResourceExhausted(format!(
                "native write has insufficient space under the 2x/3x plus metadata allowance limits for {CATALOG_HEADROOM_BYTES} bytes of catalog metadata"
            ))
        })
}

pub(super) fn storage_limit(source_bytes: u64, multiplier: u64, kind: &str) -> Result<u64> {
    source_bytes
        .checked_mul(multiplier)
        .and_then(|bytes| bytes.checked_add(FIXED_METADATA_ALLOWANCE_BYTES))
        .ok_or_else(|| Error::ResourceExhausted(format!("native {kind} byte limit overflow")))
}

fn next_version(current: u64) -> Result<u64> {
    current
        .checked_add(1)
        .ok_or_else(|| Error::ResourceExhausted("native table version is exhausted".to_owned()))
}

fn validate_schema(schema: &SchemaRef) -> Result<()> {
    let mut names = HashSet::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let normalized = field.name().to_ascii_lowercase();
        if normalized.is_empty() || !names.insert(normalized) {
            return Err(Error::InvalidArgument(format!(
                "native table output contains an empty or duplicate column name '{}'",
                field.name()
            )));
        }
        if !supported_type(field.data_type()) {
            return Err(Error::Unsupported(format!(
                "native table column '{}' has unsupported type {}",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn validate_append_schema(source: &SchemaRef, target: &SchemaRef) -> Result<()> {
    if source.fields().len() != target.fields().len()
        || source
            .fields()
            .iter()
            .zip(target.fields())
            .any(|(source, target)| source.data_type() != target.data_type())
    {
        return Err(Error::InvalidArgument(
            "INSERT source columns must match the native table types by position".to_owned(),
        ));
    }
    Ok(())
}

fn supported_type(data_type: &DataType) -> bool {
    if let DataType::Decimal128(precision, _) = data_type {
        return *precision <= 38;
    }
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Interval(_)
    )
}

#[cfg(test)]
#[path = "write_plan/tests.rs"]
mod tests;

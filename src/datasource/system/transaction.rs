use std::{collections::BTreeMap, sync::Arc};

use arrow::{array::UInt64Array, datatypes::SchemaRef, record_batch::RecordBatch};

use super::{SystemTableKind, schema, strings, uints};
use crate::{
    Error, Result,
    storage::{NativeTableSnapshot, NativeView},
};

#[derive(Clone)]
pub(crate) struct NativeSystemSnapshot {
    pub(super) schemas: Arc<std::collections::BTreeSet<String>>,
    pub(super) tables: Arc<BTreeMap<String, Arc<NativeTableSnapshot>>>,
    pub(super) views: Arc<BTreeMap<String, Arc<NativeView>>>,
}

impl NativeSystemSnapshot {
    pub(crate) fn new(
        schemas: std::collections::BTreeSet<String>,
        tables: BTreeMap<String, Arc<NativeTableSnapshot>>,
        views: BTreeMap<String, Arc<NativeView>>,
    ) -> Self {
        Self {
            schemas: Arc::new(schemas),
            tables: Arc::new(tables),
            views: Arc::new(views),
        }
    }
}

pub(super) fn information_schemata(snapshot: &NativeSystemSnapshot) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema(SystemTableKind::InformationSchemata),
        vec![strings(snapshot.schemas.iter().map(String::as_str))],
    )
    .map_err(Error::from)
}

pub(super) fn information_tables(snapshot: &NativeSystemSnapshot) -> Result<RecordBatch> {
    let mut objects = objects(snapshot);
    objects.sort_by(|left, right| left.0.cmp(&right.0));
    let len = objects.len();
    RecordBatch::try_new(
        schema(SystemTableKind::InformationTables),
        vec![
            strings(std::iter::repeat_n("rustdb", len)),
            strings(
                objects
                    .iter()
                    .map(|object| crate::catalog_name::schema_of(&object.0)),
            ),
            strings(
                objects
                    .iter()
                    .map(|object| crate::catalog_name::object_of(&object.0)),
            ),
            strings(objects.iter().map(|object| object.1)),
        ],
    )
    .map_err(Error::from)
}

pub(super) fn information_columns(snapshot: &NativeSystemSnapshot) -> Result<RecordBatch> {
    let mut objects = objects(snapshot);
    objects.sort_by(|left, right| left.0.cmp(&right.0));
    let mut table_names = Vec::new();
    let mut table_schemas = Vec::new();
    let mut column_names = Vec::new();
    let mut ordinals = Vec::new();
    let mut data_types = Vec::new();
    let mut nullable = Vec::new();
    for (name, _, schema) in objects {
        for (index, field) in schema.fields().iter().enumerate() {
            table_schemas.push(crate::catalog_name::schema_of(&name).to_owned());
            table_names.push(crate::catalog_name::object_of(&name).to_owned());
            column_names.push(field.name().clone());
            ordinals.push(u64::try_from(index + 1).unwrap_or(u64::MAX));
            data_types.push(field.data_type().to_string());
            nullable.push(if field.is_nullable() { "YES" } else { "NO" });
        }
    }
    let len = table_names.len();
    RecordBatch::try_new(
        schema(SystemTableKind::InformationColumns),
        vec![
            strings(std::iter::repeat_n("rustdb", len)),
            strings(table_schemas.iter().map(String::as_str)),
            strings(table_names.iter().map(String::as_str)),
            strings(column_names.iter().map(String::as_str)),
            Arc::new(UInt64Array::from(ordinals)),
            strings(data_types.iter().map(String::as_str)),
            strings(nullable),
        ],
    )
    .map_err(Error::from)
}

pub(super) fn native_tables(snapshot: &NativeSystemSnapshot) -> Result<RecordBatch> {
    let mut tables = snapshot.tables.iter().collect::<Vec<_>>();
    tables.sort_by(|left, right| left.0.cmp(right.0));
    RecordBatch::try_new(
        schema(SystemTableKind::NativeTables),
        vec![
            strings(tables.iter().map(|(name, _)| name.as_str())),
            strings(tables.iter().map(|(_, table)| table.table_id())),
            Arc::new(UInt64Array::from_iter_values(
                tables.iter().map(|(_, table)| table.version()),
            )),
            strings(tables.iter().map(|(_, table)| table.snapshot_id())),
            uints(tables.iter().map(|(_, table)| table.row_count())),
            uints(tables.iter().map(|(_, table)| table.physical_row_count())),
            uints(tables.iter().map(|(_, table)| table.deleted_row_count())),
            uints(
                tables
                    .iter()
                    .map(|(_, table)| u64::try_from(table.segment_count()).unwrap_or(u64::MAX)),
            ),
            uints(tables.iter().map(|(_, table)| table.source_bytes())),
            uints(tables.iter().map(|(_, table)| table.storage_bytes())),
        ],
    )
    .map_err(Error::from)
}

fn objects(snapshot: &NativeSystemSnapshot) -> Vec<(String, &'static str, SchemaRef)> {
    snapshot
        .tables
        .iter()
        .map(|(name, table)| (name.clone(), "BASE TABLE", table.schema()))
        .chain(
            snapshot
                .views
                .iter()
                .map(|(name, view)| (name.clone(), "VIEW", view.schema())),
        )
        .collect()
}

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_trait::async_trait;

use super::{ScanRequest, TableProvider, TableStatistics};
use crate::{
    Error, Result,
    runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream},
    storage::NativeDatabase,
};

#[path = "system/transaction.rs"]
mod transaction;
pub(crate) use transaction::NativeSystemSnapshot;
use transaction::{
    information_columns as transaction_information_columns,
    information_tables as transaction_information_tables,
    native_tables as transaction_native_tables,
};

#[derive(Clone, Copy)]
pub(crate) enum SystemTableKind {
    InformationSchemata,
    InformationTables,
    InformationColumns,
    NativeTables,
    Wal,
}

pub(crate) struct NativeSystemTable {
    source: NativeSystemSource,
    kind: SystemTableKind,
    schema: SchemaRef,
}

enum NativeSystemSource {
    Database(Arc<NativeDatabase>),
    Transaction(NativeSystemSnapshot),
}

impl NativeSystemTable {
    pub(crate) fn new(database: Arc<NativeDatabase>, kind: SystemTableKind) -> Self {
        Self {
            source: NativeSystemSource::Database(database),
            kind,
            schema: schema(kind),
        }
    }

    pub(crate) fn for_transaction(snapshot: NativeSystemSnapshot, kind: SystemTableKind) -> Self {
        Self {
            source: NativeSystemSource::Transaction(snapshot),
            kind,
            schema: schema(kind),
        }
    }

    fn batch(&self) -> Result<RecordBatch> {
        match (&self.source, self.kind) {
            (NativeSystemSource::Database(database), SystemTableKind::InformationSchemata) => {
                information_schemata(database)
            }
            (NativeSystemSource::Database(database), SystemTableKind::InformationTables) => {
                information_tables(database)
            }
            (NativeSystemSource::Database(database), SystemTableKind::InformationColumns) => {
                information_columns(database)
            }
            (NativeSystemSource::Database(database), SystemTableKind::NativeTables) => {
                native_tables(database)
            }
            (NativeSystemSource::Database(database), SystemTableKind::Wal) => wal(database),
            (NativeSystemSource::Transaction(snapshot), SystemTableKind::InformationTables) => {
                transaction_information_tables(snapshot)
            }
            (NativeSystemSource::Transaction(snapshot), SystemTableKind::InformationSchemata) => {
                transaction::information_schemata(snapshot)
            }
            (NativeSystemSource::Transaction(snapshot), SystemTableKind::InformationColumns) => {
                transaction_information_columns(snapshot)
            }
            (NativeSystemSource::Transaction(snapshot), SystemTableKind::NativeTables) => {
                transaction_native_tables(snapshot)
            }
            (NativeSystemSource::Transaction(_), SystemTableKind::Wal) => Err(Error::Internal(
                "transactional WAL system provider is invalid".to_owned(),
            )),
        }
    }
}

#[async_trait]
impl TableProvider for NativeSystemTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    fn explain_scan(&self) -> Option<String> {
        Some("format=rustdb-system snapshot=query-start".to_owned())
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        request.reject_unsupported_exact("system table")?;
        context.check_cancelled()?;
        let mut batch = self.batch()?;
        if let Some(projection) = request.projection.as_deref() {
            batch = project(batch, projection)?;
        }
        if let Some(limit) = request.limit
            && batch.num_rows() > limit
        {
            batch = batch.slice(0, limit);
        }
        Ok(boxed_record_batch_stream(futures::stream::once(
            async move { Ok(batch) },
        )))
    }
}

fn schema(kind: SystemTableKind) -> SchemaRef {
    let fields = match kind {
        SystemTableKind::InformationSchemata => vec![text("schema_name", false)],
        SystemTableKind::InformationTables => vec![
            text("table_catalog", false),
            text("table_schema", false),
            text("table_name", false),
            text("table_type", false),
        ],
        SystemTableKind::InformationColumns => vec![
            text("table_catalog", false),
            text("table_schema", false),
            text("table_name", false),
            text("column_name", false),
            Field::new("ordinal_position", DataType::UInt64, false),
            text("data_type", false),
            text("is_nullable", false),
        ],
        SystemTableKind::NativeTables => vec![
            text("table_name", false),
            text("table_id", false),
            Field::new("snapshot_version", DataType::UInt64, false),
            text("snapshot_id", false),
            Field::new("rows", DataType::UInt64, false),
            Field::new("physical_rows", DataType::UInt64, false),
            Field::new("deleted_rows", DataType::UInt64, false),
            Field::new("segments", DataType::UInt64, false),
            Field::new("source_bytes", DataType::UInt64, false),
            Field::new("storage_bytes", DataType::UInt64, false),
        ],
        SystemTableKind::Wal => vec![
            Field::new("next_lsn", DataType::UInt64, false),
            Field::new("tracked_transactions", DataType::UInt64, false),
            Field::new("active_writes", DataType::UInt64, false),
        ],
    };
    Arc::new(Schema::new(fields))
}

fn information_schemata(database: &NativeDatabase) -> Result<RecordBatch> {
    let names = database.schema_names();
    RecordBatch::try_new(
        schema(SystemTableKind::InformationSchemata),
        vec![strings(names.iter().map(String::as_str))],
    )
    .map_err(Error::from)
}

fn text(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

fn information_tables(database: &NativeDatabase) -> Result<RecordBatch> {
    let mut objects = database.catalog_object_infos();
    objects.sort_by(|left, right| left.name.cmp(&right.name));
    let len = objects.len();
    RecordBatch::try_new(
        schema(SystemTableKind::InformationTables),
        vec![
            strings(std::iter::repeat_n("rustdb", len)),
            strings(
                objects
                    .iter()
                    .map(|object| crate::catalog_name::schema_of(&object.name)),
            ),
            strings(
                objects
                    .iter()
                    .map(|object| crate::catalog_name::object_of(&object.name)),
            ),
            strings(objects.iter().map(|object| object.object_type)),
        ],
    )
    .map_err(Error::from)
}

fn information_columns(database: &NativeDatabase) -> Result<RecordBatch> {
    let mut objects = database.catalog_object_infos();
    objects.sort_by(|left, right| left.name.cmp(&right.name));
    let mut table_names = Vec::new();
    let mut table_schemas = Vec::new();
    let mut column_names = Vec::new();
    let mut ordinals = Vec::new();
    let mut data_types = Vec::new();
    let mut nullable = Vec::new();
    for object in objects {
        for (index, field) in object.schema.fields().iter().enumerate() {
            table_schemas.push(crate::catalog_name::schema_of(&object.name).to_owned());
            table_names.push(crate::catalog_name::object_of(&object.name).to_owned());
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

fn native_tables(database: &NativeDatabase) -> Result<RecordBatch> {
    let mut tables = database.table_infos();
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    RecordBatch::try_new(
        schema(SystemTableKind::NativeTables),
        vec![
            strings(tables.iter().map(|table| table.name.as_str())),
            strings(tables.iter().map(|table| table.table_id.as_str())),
            Arc::new(UInt64Array::from_iter_values(
                tables.iter().map(|table| table.version),
            )),
            strings(tables.iter().map(|table| table.snapshot_id.as_str())),
            uints(tables.iter().map(|table| table.rows)),
            uints(tables.iter().map(|table| table.physical_rows)),
            uints(tables.iter().map(|table| table.deleted_rows)),
            uints(tables.iter().map(|table| table.segments)),
            uints(tables.iter().map(|table| table.source_bytes)),
            uints(tables.iter().map(|table| table.storage_bytes)),
        ],
    )
    .map_err(Error::from)
}

fn wal(database: &NativeDatabase) -> Result<RecordBatch> {
    let wal = database.wal_info()?;
    RecordBatch::try_new(
        schema(SystemTableKind::Wal),
        vec![
            uints([wal.next_lsn]),
            uints([wal.tracked_transactions]),
            uints([wal.active_writes]),
        ],
    )
    .map_err(Error::from)
}

fn strings<'a>(values: impl IntoIterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn uints(values: impl IntoIterator<Item = u64>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter_values(values))
}

fn project(batch: RecordBatch, projection: &[usize]) -> Result<RecordBatch> {
    let fields = projection
        .iter()
        .map(|index| {
            batch
                .schema()
                .fields()
                .get(*index)
                .cloned()
                .ok_or_else(|| Error::Internal(format!("system projection {index} is invalid")))
        })
        .collect::<Result<Vec<_>>>()?;
    let columns = projection
        .iter()
        .map(|index| {
            batch
                .columns()
                .get(*index)
                .cloned()
                .ok_or_else(|| Error::Internal(format!("system projection {index} is invalid")))
        })
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        columns,
        &options,
    )?)
}

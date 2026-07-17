use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    basic::{Encoding, Type as PhysicalType},
};

use super::{parquet_metadata::schema_memory_size, schema_evolution::canonical_type};
use crate::{
    Result,
    runtime::{MemoryReservation, QueryContext},
};

/// Query-scoped Arrow metadata whose schema asks the Parquet reader to retain
/// proven physical string dictionaries. The reservation lives as long as any
/// row-group morsel that shares the derived schema.
#[derive(Clone)]
pub(super) struct DictionaryDecode {
    inner: Arc<DictionaryDecodeInner>,
}

struct DictionaryDecodeInner {
    metadata: Option<ArrowReaderMetadata>,
    output_columns: Arc<[usize]>,
    _reservation: Option<MemoryReservation>,
}

impl DictionaryDecode {
    pub(super) fn try_new(
        base: &ArrowReaderMetadata,
        file_schema: &Schema,
        table_schema: &Schema,
        output_schema: &Schema,
        requested_columns: &[usize],
        context: &QueryContext,
    ) -> Result<Option<Self>> {
        context.check_cancelled()?;
        let columns = eligible_columns(
            base,
            file_schema,
            table_schema,
            output_schema,
            requested_columns,
        );
        if columns.is_empty() {
            return Ok(None);
        }

        let changed = columns.iter().any(|column| {
            !matches!(
                base.schema().field(column.file).data_type(),
                DataType::Dictionary(_, _)
            )
        });

        let mut reservation = None;
        let metadata = if changed {
            // The Parquet metadata and footer remain shared. Account only for
            // the independently derived Arrow schema and field-level reader
            // description. If optional admission fails, retain the canonical
            // reader rather than failing an otherwise valid query.
            let bytes = schema_memory_size(base.schema())
                .saturating_mul(2)
                .saturating_add(4 * 1024);
            let Ok(lease) = context.memory.try_reserve(bytes) else {
                return Ok(None);
            };
            context.metrics.observe_memory(context.memory.used());
            let mut fields = base.schema().fields().to_vec();
            for column in &columns {
                let field = &fields[column.file];
                if matches!(field.data_type(), DataType::Dictionary(_, _)) {
                    continue;
                }
                fields[column.file] =
                    Arc::new(field.as_ref().clone().with_data_type(DataType::Dictionary(
                        Box::new(DataType::UInt32),
                        Box::new(field.data_type().clone()),
                    )));
            }
            let schema = Arc::new(Schema::new_with_metadata(
                fields,
                base.schema().metadata().clone(),
            ));
            let hinted = match ArrowReaderMetadata::try_new(
                Arc::clone(base.metadata()),
                ArrowReaderOptions::new().with_schema(schema),
            ) {
                Ok(metadata) => metadata,
                Err(_) => return Ok(None),
            };
            reservation = Some(lease);
            Some(hinted)
        } else {
            None
        };

        Ok(Some(Self {
            inner: Arc::new(DictionaryDecodeInner {
                metadata,
                output_columns: columns
                    .into_iter()
                    .map(|column| column.output)
                    .collect::<Vec<_>>()
                    .into(),
                _reservation: reservation,
            }),
        }))
    }

    pub(super) fn reader_metadata<'a>(
        &'a self,
        fallback: &'a ArrowReaderMetadata,
    ) -> &'a ArrowReaderMetadata {
        self.inner.metadata.as_ref().unwrap_or(fallback)
    }

    pub(super) fn output_columns(&self) -> &[usize] {
        &self.inner.output_columns
    }
}

#[derive(Clone, Copy)]
struct DictionaryColumn {
    file: usize,
    output: usize,
}

fn eligible_columns(
    metadata: &ArrowReaderMetadata,
    file_schema: &Schema,
    table_schema: &Schema,
    output_schema: &Schema,
    requested_columns: &[usize],
) -> Vec<DictionaryColumn> {
    let parquet_schema = metadata.parquet_schema();
    let mut columns = Vec::new();
    for requested in requested_columns {
        let Some(table_field) = table_schema.fields().get(*requested) else {
            continue;
        };
        if !dictionary_value_type(table_field.data_type()) {
            continue;
        }
        let Ok(file) = file_schema.index_of(table_field.name()) else {
            continue;
        };
        if canonical_type(file_schema.field(file).data_type()) != *table_field.data_type() {
            continue;
        }
        let Ok(output) = output_schema.index_of(table_field.name()) else {
            continue;
        };
        let mut leaves = (0..parquet_schema.num_columns())
            .filter(|leaf| parquet_schema.get_column_root_idx(*leaf) == file);
        let Some(leaf) = leaves.next() else {
            continue;
        };
        if leaves.next().is_some()
            || parquet_schema.column(leaf).physical_type() != PhysicalType::BYTE_ARRAY
            || !all_row_groups_dictionary_encoded(metadata, leaf)
        {
            continue;
        }
        if columns
            .iter()
            .all(|column: &DictionaryColumn| column.file != file)
        {
            columns.push(DictionaryColumn { file, output });
        }
    }
    columns
}

fn all_row_groups_dictionary_encoded(metadata: &ArrowReaderMetadata, leaf: usize) -> bool {
    let row_groups = metadata.metadata().row_groups();
    !row_groups.is_empty()
        && row_groups.iter().all(|row_group| {
            let column = row_group.column(leaf);
            column.dictionary_page_offset().is_some()
                && column
                    .page_encoding_stats_mask()
                    .map_or_else(|| dictionary_only(column.encodings_mask()), dictionary_only)
        })
}

fn dictionary_only(mask: &parquet::basic::EncodingMask) -> bool {
    mask.is_only(Encoding::PLAIN_DICTIONARY) || mask.is_only(Encoding::RLE_DICTIONARY)
}

fn dictionary_value_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

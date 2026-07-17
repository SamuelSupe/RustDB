use std::sync::Arc;

use arrow::datatypes::{DECIMAL64_MAX_PRECISION, DataType, Schema};
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    basic::Type as PhysicalType,
    schema::types::SchemaDescriptor,
};

use super::parquet_metadata::schema_memory_size;
use crate::{
    Result,
    runtime::{MemoryReservation, QueryContext},
};

/// Query-local metadata that decodes narrow logical decimals at their native
/// 64-bit width. Public batches are widened back to Decimal128 by alignment.
#[derive(Clone)]
pub(super) struct NarrowDecimalDecode {
    inner: Arc<NarrowDecimalDecodeInner>,
}

struct NarrowDecimalDecodeInner {
    metadata: ArrowReaderMetadata,
    _reservation: MemoryReservation,
}

impl NarrowDecimalDecode {
    pub(super) fn try_new(
        base: &ArrowReaderMetadata,
        context: &QueryContext,
    ) -> Result<Option<Self>> {
        let Some((schema, columns)) = narrow_schema(base.schema(), base.parquet_schema()) else {
            return Ok(None);
        };
        let bytes = schema_memory_size(&schema)
            .saturating_mul(2)
            .saturating_add(4 * 1024);
        let Ok(reservation) = context.memory.try_reserve(bytes) else {
            return Ok(None);
        };
        context.metrics.observe_memory(context.memory.used());
        let metadata = match ArrowReaderMetadata::try_new(
            Arc::clone(base.metadata()),
            ArrowReaderOptions::new().with_schema(Arc::new(schema)),
        ) {
            Ok(metadata) => metadata,
            Err(_) => return Ok(None),
        };
        context.metrics.add_parquet_narrow_decimal_columns(columns);
        Ok(Some(Self {
            inner: Arc::new(NarrowDecimalDecodeInner {
                metadata,
                _reservation: reservation,
            }),
        }))
    }

    pub(super) fn reader_metadata(&self) -> &ArrowReaderMetadata {
        &self.inner.metadata
    }
}

fn narrow_schema(schema: &Schema, parquet: &SchemaDescriptor) -> Option<(Schema, u64)> {
    let mut columns = 0_u64;
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(column, field)| match field.data_type() {
            DataType::Decimal128(precision, scale)
                if *precision <= DECIMAL64_MAX_PRECISION && supports_decimal64(parquet, column) =>
            {
                columns = columns.saturating_add(1);
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Decimal64(*precision, *scale)),
                )
            }
            _ => Arc::clone(field),
        })
        .collect::<Vec<_>>();
    (columns != 0).then(|| {
        (
            Schema::new_with_metadata(fields, schema.metadata().clone()),
            columns,
        )
    })
}

fn supports_decimal64(parquet: &SchemaDescriptor, root: usize) -> bool {
    let mut leaves =
        (0..parquet.num_columns()).filter(|leaf| parquet.get_column_root_idx(*leaf) == root);
    let Some(leaf) = leaves.next() else {
        return false;
    };
    if leaves.next().is_some() {
        return false;
    }
    let column = parquet.column(leaf);
    match column.physical_type() {
        PhysicalType::INT32 | PhysicalType::INT64 => true,
        PhysicalType::FIXED_LEN_BYTE_ARRAY => (1..=8).contains(&column.type_length()),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Decimal64Array, Decimal128Array},
        compute::cast,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use parquet::{
        arrow::{
            ArrowWriter,
            arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
        },
        basic::Type as PhysicalType,
        file::properties::WriterProperties,
    };

    use super::{narrow_schema, supports_decimal64};

    #[test]
    fn parquet_decimal64_hint_round_trips_decimal128_values() {
        let logical = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(15, 2),
            true,
        )]));
        let values = Decimal128Array::from(vec![Some(123_i128), None, Some(-456_i128)])
            .with_precision_and_scale(15, 2)
            .unwrap();
        let batch = RecordBatch::try_new(Arc::clone(&logical), vec![Arc::new(values)]).unwrap();
        let mut encoded = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut encoded,
            Arc::clone(&logical),
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let bytes = Bytes::from(encoded);
        let base = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
        let (physical, columns) = narrow_schema(&logical, base.parquet_schema()).unwrap();
        assert_eq!(columns, 1);
        let physical = Arc::new(physical);
        let options = ArrowReaderOptions::new().with_schema(physical);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, options)
            .unwrap()
            .build()
            .unwrap();
        let decoded = reader.next().unwrap().unwrap();
        let narrow = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Decimal64Array>()
            .unwrap();
        assert_eq!(
            narrow.iter().collect::<Vec<_>>(),
            vec![Some(123), None, Some(-456)]
        );

        let widened = cast(decoded.column(0), &DataType::Decimal128(15, 2)).unwrap();
        assert_eq!(widened.as_ref(), batch.column(0).as_ref());
    }

    #[test]
    fn wide_fixed_decimal_keeps_decimal128_decode() {
        let logical = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(19, 2),
            false,
        )]));
        let values = Decimal128Array::from(vec![123_i128])
            .with_precision_and_scale(19, 2)
            .unwrap();
        let batch = RecordBatch::try_new(Arc::clone(&logical), vec![Arc::new(values)]).unwrap();
        let mut encoded = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut encoded, Arc::clone(&logical), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let base = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(encoded)).unwrap();
        let column = base.parquet_schema().column(0);
        assert_eq!(column.physical_type(), PhysicalType::FIXED_LEN_BYTE_ARRAY);
        assert!(column.type_length() > 8);
        assert!(!supports_decimal64(base.parquet_schema(), 0));
        assert!(narrow_schema(&logical, base.parquet_schema()).is_none());
    }
}

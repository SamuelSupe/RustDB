use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Array, RecordBatch},
    compute::cast,
    datatypes::{DataType, Schema},
    row::{RowConverter, SortField},
};
use rustdb::{Error, Result};
use sha2::{Digest, Sha256};

pub(super) const ALGORITHM: &str = "rustdb-typed-multiset-sha256-v1";

pub(super) struct TypedChecksum {
    source_types: Vec<DataType>,
    schema_digest: [u8; 32],
    converter: Option<RowConverter>,
    rows: u64,
    sum: [u64; 4],
    xor: [u64; 4],
    quadratic_sum: [u64; 4],
}

impl TypedChecksum {
    pub(super) fn new(schema: &Schema) -> Result<Self> {
        let source_types: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let row_types: Vec<_> = source_types.iter().map(normalized_row_type).collect();
        let converter = if row_types.is_empty() {
            None
        } else {
            Some(RowConverter::new(
                row_types.into_iter().map(SortField::new).collect(),
            )?)
        };
        Ok(Self {
            schema_digest: schema_type_digest(&source_types),
            source_types,
            converter,
            rows: 0,
            sum: [0; 4],
            xor: [0; 4],
            quadratic_sum: [0; 4],
        })
    }

    pub(super) fn update_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.validate_schema(batch)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let columns = batch
            .columns()
            .iter()
            .zip(&self.source_types)
            .map(|(array, data_type)| normalize_array(array, data_type))
            .collect::<Result<Vec<_>>>()?;

        if let Some(converter) = &self.converter {
            let encoded = converter.convert_columns(&columns)?;
            for row in 0..batch.num_rows() {
                self.update_row(encoded.row(row).data())?;
            }
        } else {
            for _ in 0..batch.num_rows() {
                self.update_row(&[])?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> String {
        let mut final_digest = Sha256::new();
        final_digest.update(ALGORITHM.as_bytes());
        final_digest.update(self.schema_digest);
        final_digest.update(self.rows.to_le_bytes());
        for values in [self.sum, self.xor, self.quadratic_sum] {
            for value in values {
                final_digest.update(value.to_le_bytes());
            }
        }
        format!("{:x}", final_digest.finalize())
    }

    fn validate_schema(&self, batch: &RecordBatch) -> Result<()> {
        if batch.num_columns() != self.source_types.len() {
            return Err(Error::Execution(format!(
                "benchmark result schema changed from {} to {} columns",
                self.source_types.len(),
                batch.num_columns()
            )));
        }
        for (index, (array, expected)) in batch.columns().iter().zip(&self.source_types).enumerate()
        {
            if array.data_type() != expected {
                return Err(Error::Execution(format!(
                    "benchmark result column {index} changed type from {expected} to {}",
                    array.data_type()
                )));
            }
        }
        Ok(())
    }

    fn update_row(&mut self, encoded: &[u8]) -> Result<()> {
        let mut row_digest = Sha256::new();
        row_digest.update(b"rustdb-typed-row-v1");
        row_digest.update(encoded);
        let digest: [u8; 32] = row_digest.finalize().into();
        for index in 0..4 {
            let offset = index * 8;
            let limb = u64::from_le_bytes(
                digest[offset..offset + 8]
                    .try_into()
                    .expect("SHA-256 digest has four u64 limbs"),
            );
            self.sum[index] = self.sum[index].wrapping_add(limb);
            self.xor[index] ^= limb;
            self.quadratic_sum[index] =
                self.quadratic_sum[index].wrapping_add(limb.wrapping_mul(limb.rotate_left(17)));
        }
        self.rows = self.rows.checked_add(1).ok_or_else(|| {
            Error::Execution("benchmark checksum row count overflowed u64".to_owned())
        })?;
        Ok(())
    }
}

fn normalized_row_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => normalized_row_type(value_type),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => DataType::Float64,
        other => other.clone(),
    }
}

fn normalize_array(array: &ArrayRef, source_type: &DataType) -> Result<ArrayRef> {
    match source_type {
        DataType::Dictionary(_, value_type) => {
            let decoded = cast(array, value_type)?;
            normalize_array(&decoded, value_type)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let casted = cast(array, &DataType::Float64)?;
            let values = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Internal("Float64 cast returned another type".to_owned()))?;
            let normalized = Float64Array::from_iter(values.iter().map(|value| {
                value.map(|value| {
                    if value.is_nan() {
                        f64::NAN
                    } else if value == 0.0 {
                        0.0
                    } else {
                        value
                    }
                })
            }));
            Ok(Arc::new(normalized))
        }
        _ => Ok(Arc::clone(array)),
    }
}

fn schema_type_digest(types: &[DataType]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"rustdb-typed-schema-v1");
    digest.update((types.len() as u64).to_le_bytes());
    for data_type in types {
        let descriptor = format!("{data_type:?}");
        digest.update((descriptor.len() as u64).to_le_bytes());
        digest.update(descriptor.as_bytes());
    }
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::TypedChecksum;

    #[test]
    fn checksum_is_independent_of_batches_and_row_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let one = batch(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        );
        let two_a = batch(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(StringArray::from(vec![Some("c")])),
            ],
        );
        let two_b = batch(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![2, 1])),
                Arc::new(StringArray::from(vec![None, Some("a")])),
            ],
        );

        assert_eq!(
            checksum(&schema, &[one]),
            checksum(&schema, &[two_a, two_b])
        );
    }

    #[test]
    fn checksum_keeps_types_and_nulls_distinct() {
        let int_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let uint_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            true,
        )]));
        let null_batch = batch(
            Arc::clone(&int_schema),
            vec![Arc::new(Int64Array::from(vec![None]))],
        );
        let value_batch = batch(
            Arc::clone(&int_schema),
            vec![Arc::new(Int64Array::from(vec![Some(0)]))],
        );
        let uint_batch = batch(
            Arc::clone(&uint_schema),
            vec![Arc::new(UInt64Array::from(vec![Some(0)]))],
        );

        assert_ne!(
            checksum(&int_schema, &[null_batch]),
            checksum(&int_schema, std::slice::from_ref(&value_batch))
        );
        assert_ne!(
            checksum(&int_schema, &[value_batch]),
            checksum(&uint_schema, &[uint_batch])
        );
    }

    #[test]
    fn checksum_normalizes_signed_zero_and_nan_payloads() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let left = batch(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![-0.0, f64::NAN]))],
        );
        let right = batch(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![
                0.0,
                f64::from_bits(0x7ff8_0000_0000_0042),
            ]))],
        );
        assert_eq!(checksum(&schema, &[left]), checksum(&schema, &[right]));
    }

    fn checksum(schema: &Schema, batches: &[RecordBatch]) -> String {
        let mut checksum = TypedChecksum::new(schema).unwrap();
        for batch in batches {
            checksum.update_batch(batch).unwrap();
        }
        checksum.finish()
    }

    fn batch(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(schema, columns).unwrap()
    }
}

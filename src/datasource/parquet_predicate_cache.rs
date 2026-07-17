use arrow::datatypes::Schema;

use super::parquet_row_filter::ParquetRowFilter;
use crate::runtime::{MemoryPool, MemoryReservation, estimate_array_bytes};

const CACHE_MEMORY_DIVISOR: usize = 16;
const CACHE_ENTRY_OVERHEAD_BYTES: usize = 128;
const CACHE_BASE_OVERHEAD_BYTES: usize = 4 << 10;

/// A row-group cache admission computed before the reader allocates decoded
/// arrays. Arrow accounts only array buffers, while the reservation also
/// covers the cache maps and handles that retain them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct PredicateCachePlan {
    pub(super) arrow_bytes: usize,
    pub(super) reservation_bytes: usize,
}

impl PredicateCachePlan {
    /// Predicate caching is optional. Failure to obtain its complete credit
    /// disables the Arrow cache instead of waiting while a scan preclaim is
    /// already held.
    pub(super) fn try_reserve(self, memory: &MemoryPool) -> (usize, Option<MemoryReservation>) {
        if self.arrow_bytes == 0 || self.reservation_bytes == 0 {
            return (0, None);
        }
        match memory.try_reserve(self.reservation_bytes) {
            Ok(reservation) => (self.arrow_bytes, Some(reservation)),
            Err(_) => (0, None),
        }
    }
}

pub(super) fn plan(
    filter: Option<&ParquetRowFilter>,
    projection: &[usize],
    file_schema: &Schema,
    row_count: usize,
    decode_batch_size: usize,
    query_memory_limit: usize,
    configured_lanes: usize,
) -> PredicateCachePlan {
    let Some(filter) = filter else {
        return PredicateCachePlan::default();
    };
    let Some(columns) = filter.sparse_decimal_cache_columns(projection, file_schema) else {
        return PredicateCachePlan::default();
    };
    // This shape is deliberately sparse. Retaining one full-row-group column
    // to reuse only the selected rows costs more than decoding that one column
    // again; require at least two reusable predicate columns.
    if columns.len() < 2 {
        return PredicateCachePlan::default();
    }
    let Some((arrow_bytes, entries)) =
        estimate_cache(&columns, file_schema, row_count, decode_batch_size)
    else {
        return PredicateCachePlan::default();
    };
    let Some(reservation_bytes) = entries
        .checked_mul(CACHE_ENTRY_OVERHEAD_BYTES)
        .and_then(|overhead| arrow_bytes.checked_add(overhead))
        .and_then(|bytes| bytes.checked_add(CACHE_BASE_OVERHEAD_BYTES))
    else {
        return PredicateCachePlan::default();
    };
    let per_lane_budget = query_memory_limit
        .checked_div(CACHE_MEMORY_DIVISOR)
        .unwrap_or(0)
        .checked_div(configured_lanes.max(1))
        .unwrap_or(0);
    if reservation_bytes > per_lane_budget {
        return PredicateCachePlan::default();
    }
    PredicateCachePlan {
        arrow_bytes,
        reservation_bytes,
    }
}

fn estimate_cache(
    columns: &[usize],
    schema: &Schema,
    row_count: usize,
    batch_size: usize,
) -> Option<(usize, usize)> {
    if columns.is_empty() || row_count == 0 || batch_size == 0 {
        return None;
    }
    let full_batches = row_count / batch_size;
    let tail_rows = row_count % batch_size;
    let batches = full_batches.checked_add(usize::from(tail_rows != 0))?;
    let entries = columns.len().checked_mul(batches)?;
    let mut bytes = 0usize;
    for column in columns {
        let data_type = schema.fields().get(*column)?.data_type();
        let full_bytes = estimate_array_bytes(data_type, batch_size);
        bytes = bytes.checked_add(full_bytes.checked_mul(full_batches)?)?;
        if tail_rows != 0 {
            bytes = bytes.checked_add(estimate_array_bytes(data_type, tail_rows))?;
        }
    }
    Some((bytes, entries))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{PredicateCachePlan, plan};
    use crate::{
        datasource::{
            ComparisonOp, PredicateValue, ScanPredicate, parquet_row_filter::ParquetRowFilter,
        },
        runtime::MemoryPool,
    };

    const ROWS: usize = 122_880;
    const BATCH_SIZE: usize = 8_192;

    #[test]
    fn q6_fixed_width_cache_is_estimated_per_decode_batch() {
        let (schema, filter) = q6_filter();
        let plan = plan(
            Some(&filter),
            &[0, 1, 2, 3],
            &schema,
            ROWS,
            BATCH_SIZE,
            2 << 30,
            4,
        );

        assert_eq!(plan.arrow_bytes, 4_481_280);
        assert_eq!(plan.reservation_bytes, 4_491_136);
    }

    #[test]
    fn cache_projection_intersects_final_projection() {
        let (schema, filter) = q6_filter();
        let plan = plan(
            Some(&filter),
            &[0, 1, 3],
            &schema,
            ROWS,
            BATCH_SIZE,
            2 << 30,
            4,
        );

        assert_eq!(plan.arrow_bytes, 2_496_000);
        assert_eq!(plan.reservation_bytes, 2_503_936);
    }

    #[test]
    fn one_reusable_sparse_column_does_not_admit_a_cache() {
        let (schema, filter) = q6_filter();
        assert_eq!(
            plan(
                Some(&filter),
                &[1, 3],
                &schema,
                ROWS,
                BATCH_SIZE,
                2 << 30,
                4,
            ),
            PredicateCachePlan::default()
        );
    }

    #[test]
    fn small_budget_and_overflow_disable_cache() {
        let (schema, filter) = q6_filter();
        assert_eq!(
            plan(
                Some(&filter),
                &[0, 1, 2, 3],
                &schema,
                ROWS,
                BATCH_SIZE,
                64 << 20,
                4,
            ),
            PredicateCachePlan::default()
        );
        assert_eq!(
            plan(
                Some(&filter),
                &[0, 1, 2, 3],
                &schema,
                usize::MAX,
                1,
                usize::MAX,
                1,
            ),
            PredicateCachePlan::default()
        );
    }

    #[test]
    fn cache_is_narrowly_limited_to_strict_fused_sparse_decimal_scans() {
        let (schema, predicate) = q6_inputs();
        let unfused =
            ParquetRowFilter::try_new(Some(&predicate), &schema, &schema).expect("row filter");
        assert_eq!(
            plan(
                Some(&unfused),
                &[0, 1, 2, 3],
                &schema,
                ROWS,
                BATCH_SIZE,
                2 << 30,
                4,
            ),
            PredicateCachePlan::default()
        );

        let strict = ParquetRowFilter::try_new_strict(Some(&predicate), &schema, &schema)
            .expect("strict row filter");
        assert_eq!(
            plan(
                Some(&strict),
                &[0, 1, 2],
                &schema,
                ROWS,
                BATCH_SIZE,
                2 << 30,
                4,
            ),
            PredicateCachePlan::default()
        );
    }

    #[test]
    fn reservation_failure_falls_back_and_success_releases() {
        let plan = PredicateCachePlan {
            arrow_bytes: 8_192,
            reservation_bytes: 9_216,
        };
        let small = MemoryPool::new(9_215);
        let (limit, reservation) = plan.try_reserve(&small);
        assert_eq!(limit, 0);
        assert!(reservation.is_none());
        assert_eq!(small.used(), 0);

        let memory = MemoryPool::new(9_216);
        let (limit, reservation) = plan.try_reserve(&memory);
        assert_eq!(limit, 8_192);
        assert_eq!(memory.used(), 9_216);
        drop(reservation);
        assert_eq!(memory.used(), 0);
    }

    fn q6_filter() -> (Schema, ParquetRowFilter) {
        let (schema, predicate) = q6_inputs();
        let filter = ParquetRowFilter::try_new_strict(Some(&predicate), &schema, &schema).unwrap();
        assert!(filter.has_unfiltered_decimal_payload(&[0, 1, 2, 3], &schema));
        (schema, filter)
    }

    fn q6_inputs() -> (Schema, ScanPredicate) {
        let schema = Schema::new(vec![
            Field::new("l_shipdate", DataType::Date32, true),
            Field::new("l_discount", DataType::Decimal128(15, 2), true),
            Field::new("l_quantity", DataType::Decimal128(15, 2), true),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), true),
        ]);
        let decimal = |column, op, value| ScanPredicate::Comparison {
            column,
            op,
            value: PredicateValue::Decimal128 {
                value,
                precision: 15,
                scale: 2,
            },
        };
        let predicate = ScanPredicate::And(vec![
            ScanPredicate::Comparison {
                column: 0,
                op: ComparisonOp::GtEq,
                value: PredicateValue::Date32(8_766),
            },
            decimal(1, ComparisonOp::GtEq, 5),
            decimal(1, ComparisonOp::LtEq, 7),
            decimal(2, ComparisonOp::Lt, 2_400),
        ]);
        (schema, predicate)
    }
}

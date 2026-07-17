use arrow::datatypes::Schema;

use super::ScanRequest;
use super::parquet_row_filter::workspace_bytes as row_filter_workspace_bytes;
use crate::runtime::estimate_schema_batch_bytes;

const DECODE_MEMORY_DIVISOR: usize = 16;

/// Admits a larger private decode batch only when all active readers can keep
/// their conservative preclaims within a small slice of the query budget.
pub(super) fn admitted_size(
    request: &ScanRequest,
    output_schema: &Schema,
    table_schema: &Schema,
    query_memory: usize,
    lanes: usize,
) -> usize {
    let Some(hint) = request
        .decode_batch_size
        .filter(|hint| *hint > request.batch_size)
    else {
        return request.batch_size;
    };
    let per_lane = estimate_schema_batch_bytes(output_schema, hint).saturating_add(
        row_filter_workspace_bytes(request.predicate.as_ref(), table_schema, hint),
    );
    let preclaims = per_lane.saturating_mul(lanes.max(1));
    if preclaims <= query_memory / DECODE_MEMORY_DIVISOR {
        hint
    } else {
        request.batch_size
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::admitted_size;
    use crate::datasource::{ComparisonOp, PredicateValue, ScanPredicate, ScanRequest};

    #[test]
    fn dictionary_decode_batches_are_bounded_by_query_memory() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
            Field::new("quantity", DataType::Decimal128(15, 2), true),
        ]);
        let mut request = ScanRequest::new(8_192);
        assert_eq!(admitted_size(&request, &schema, &schema, 2 << 30, 4), 8_192);

        request.decode_batch_size = Some(65_536);
        assert_eq!(
            admitted_size(&request, &schema, &schema, 2 << 30, 4),
            65_536
        );
        assert_eq!(
            admitted_size(&request, &schema, &schema, 128 << 20, 4),
            8_192
        );
    }

    #[test]
    fn exact_filter_workspace_can_force_the_decode_hint_to_fall_back() {
        let mut fields = (0..5)
            .map(|column| {
                Field::new(
                    format!("predicate_{column}"),
                    DataType::Decimal128(15, 2),
                    false,
                )
            })
            .collect::<Vec<_>>();
        fields.push(Field::new("payload", DataType::Int64, false));
        let table_schema = Schema::new(fields);
        let output_schema = Schema::new(vec![Field::new("payload", DataType::Int64, false)]);
        let mut request = ScanRequest::new(8_192);
        request.decode_batch_size = Some(65_536);
        request.predicate = Some(ScanPredicate::And(
            (0..5)
                .map(|column| ScanPredicate::Comparison {
                    column,
                    op: ComparisonOp::GtEq,
                    value: PredicateValue::Decimal128 {
                        value: 1,
                        precision: 15,
                        scale: 2,
                    },
                })
                .collect(),
        ));

        assert_eq!(
            admitted_size(&request, &output_schema, &table_schema, 2 << 30, 4),
            65_536
        );
        assert_eq!(
            admitted_size(&request, &output_schema, &table_schema, 128 << 20, 4,),
            8_192,
            "predicate workspace must participate in admission"
        );
    }
}

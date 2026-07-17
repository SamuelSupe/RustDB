use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, TimestampMicrosecondArray},
    datatypes::DataType,
};
use futures::TryStreamExt;

use super::to_timestamp_seconds;
use crate::{
    Catalog,
    execution::execute,
    runtime::{MemoryPool, QueryContext},
};

#[test]
fn timestamp_seconds_converts_nulls_and_checks_overflow() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(90), None]));
    let output = to_timestamp_seconds(&input).unwrap();
    let output = output
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(output.value(0), 90_000_000);
    assert!(output.is_null(1));

    let overflow: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
    let error = to_timestamp_seconds(&overflow).unwrap_err();
    assert!(error.to_string().contains("overflowed TIMESTAMP at row 0"));
}

#[tokio::test]
async fn integer_date_cast_and_temporal_literal_comparison_are_strict() {
    let plan = crate::sql::plan_sql(
        &Catalog::default(),
        "SELECT CAST(0 AS DATE) = '1970-01-01', \
                extract(minute FROM to_timestamp_seconds(90))",
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), directory.path()).unwrap();
    let batches: Vec<RecordBatch> = execute(plan, context)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let batch = &batches[0];
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(batch.column(1).data_type(), &DataType::Int64);
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

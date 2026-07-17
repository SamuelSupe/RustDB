use std::sync::Arc;

use arrow::{
    array::{Array, Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::{register, run};
use crate::Catalog;

#[tokio::test]
async fn integer_sum_widens_at_the_public_arrow_boundary() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "wide_sum",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![i64::MAX, 1]))],
        )
        .unwrap(),
    );

    let batches = run(&catalog, "SELECT sum(value) FROM wide_sum", 1 << 20).await;
    let sum = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(sum.data_type(), &DataType::Decimal128(38, 0));
    assert_eq!(sum.value(0), i128::from(i64::MAX) + 1);
}

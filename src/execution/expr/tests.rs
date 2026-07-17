use std::sync::Arc;

use arrow::{
    array::{
        Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, RecordBatch,
        StringArray, TimestampMicrosecondArray,
    },
    datatypes::{DataType, Field, Schema},
};
use futures::TryStreamExt;

use super::{evaluate, project};
use crate::Catalog;
use crate::execution::execute;
use crate::runtime::{MemoryPool, QueryContext};
use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

#[test]
fn empty_projection_preserves_input_row_count() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let projected = project(&[], Arc::new(Schema::empty()), &batch).unwrap();

    assert_eq!(projected.num_columns(), 0);
    assert_eq!(projected.num_rows(), 3);
}

#[test]
fn evaluates_vectorized_arithmetic() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let expression = BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(0, DataType::Int64, "x")),
            op: BinaryOp::Add,
            right: Box::new(BoundExpr::literal(ScalarValue::Int64(10))),
        },
        data_type: DataType::Int64,
        display_name: "x + 10".into(),
    };
    let result = evaluate(&expression, &batch).unwrap();
    assert_eq!(
        result.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![11, 12, 13])
    );
}

#[test]
fn compares_wide_decimals_with_different_scales_exactly() {
    let left = Decimal128Array::from(vec![
        Some(100),
        Some(100),
        Some(10_i128.pow(35) - 1),
        None,
        Some(-100),
        Some(-100),
        Some(0),
        Some(1),
    ])
    .with_precision_and_scale(35, 2)
    .unwrap();
    let right = Decimal128Array::from(vec![
        Some(999_999),
        Some(1_000_001),
        Some(10_i128.pow(38) - 1),
        Some(0),
        Some(-999_999),
        Some(-1_000_001),
        Some(0),
        Some(10_000),
    ])
    .with_precision_and_scale(38, 6)
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("left", DataType::Decimal128(35, 2), true),
            Field::new("right", DataType::Decimal128(38, 6), true),
        ])),
        vec![Arc::new(left), Arc::new(right)],
    )
    .unwrap();
    let expression = BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(0, DataType::Decimal128(35, 2), "left")),
            op: BinaryOp::Gt,
            right: Box::new(BoundExpr::column(1, DataType::Decimal128(38, 6), "right")),
        },
        data_type: DataType::Boolean,
        display_name: "left > right".into(),
    };

    let actual = evaluate(&expression, &batch).unwrap();
    assert_eq!(
        actual.as_any().downcast_ref::<BooleanArray>().unwrap(),
        &BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(false),
            Some(false),
        ])
    );
}

#[tokio::test]
async fn evaluates_tpch_scalar_semantics() {
    let catalog = Catalog::default();
    let plan = crate::sql::plan_sql(
        &catalog,
        "SELECT \
            CAST('42' AS BIGINT), \
            CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END, \
            CASE WHEN 'alpha' LIKE 'a_ph%' THEN TRUE ELSE FALSE END, \
            'a_b' LIKE 'a!_b' ESCAPE '!', \
            'alpha' NOT LIKE 'z%', \
            DATE '1998-12-01' - INTERVAL '90' DAY, \
            CAST(12.34 AS DECIMAL(8, 2)) * 2",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let batches = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let batch = &batches[0];

    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "two"
    );
    for index in 2..=4 {
        assert!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        10_471
    );
    let decimal = batch
        .column(6)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(decimal.value(0), 2_468);
    assert_eq!(decimal.data_type(), &DataType::Decimal128(10, 2));
}

#[tokio::test]
async fn evaluates_v03_scalar_functions_and_substring_boundaries() {
    let catalog = Catalog::default();
    let plan = crate::sql::plan_sql(
        &catalog,
        "SELECT \
            substring('abcdef' FROM 0 FOR 2), \
            substring('abcdef' FROM 0 FOR 1), \
            substring('abcdef' FROM 0 FOR 3), \
            substring('abcdef' FROM -8 FOR 5), \
            substring('abcdef' FROM -1 FOR 3), \
            length('你好'), lower('AbC'), upper('AbC'), trim('  x  '), \
            concat('a', NULL, 'b'), replace('abcabc', 'b', 'x'), \
            starts_with('alpha', 'al'), ends_with('alpha', 'ha'), contains('alpha', 'ph'), \
            coalesce(NULL, 'fallback'), nullif('same', 'same'), \
            abs(-7), ceil(CAST(1.2 AS DOUBLE)), floor(CAST(-1.2 AS DOUBLE)), \
            round(CAST(1.25 AS DOUBLE), 1)",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let batches = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let batch = &batches[0];
    let strings = ["a", "", "ab", "abc", "f"];
    for (index, expected) in strings.iter().enumerate() {
        assert_eq!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            *expected
        );
    }
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    for (index, expected) in [
        (6, "abc"),
        (7, "ABC"),
        (8, "x"),
        (9, "ab"),
        (10, "axcaxc"),
        (14, "fallback"),
    ] {
        assert_eq!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            expected
        );
    }
    for index in 11..=13 {
        assert!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }
    assert!(batch.column(15).is_null(0));
    assert_eq!(
        batch
            .column(16)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    for (index, expected) in [(17, 2.0), (18, -2.0), (19, 1.3)] {
        assert_eq!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
}

#[tokio::test]
async fn evaluates_microsecond_timestamp_casts_functions_and_aggregate_wrapper() {
    let catalog = Catalog::default();
    let plan = crate::sql::plan_sql(
        &catalog,
        "SELECT \
            extract(year FROM min(TIMESTAMP '1995-03-15 12:34:56.123456')), \
            date_part('month', min(TIMESTAMP '1995-03-15 12:34:56.123456')), \
            day(min(TIMESTAMP '1995-03-15 12:34:56.123456')), \
            date_trunc('day', min(TIMESTAMP '1995-03-15 12:34:56.123456')), \
            CAST(CAST('1998-12-01 12:30:45.123456' AS TIMESTAMP) AS VARCHAR), \
            CAST(TIMESTAMP '1998-12-01 12:30:45.123456' AS DATE), \
            TIMESTAMP '2000-01-31 01:02:03' + INTERVAL '1' MONTH, \
            TIMESTAMP '1998-12-01 12:30:45.123456' - INTERVAL '1' DAY",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let batches = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let batch = &batches[0];
    for (index, expected) in [(0, 1995), (1, 3), (2, 15)] {
        assert_eq!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    let parse = crate::sql::temporal::parse_timestamp_microsecond;
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        crate::sql::temporal::parse_date32("1995-03-15").unwrap()
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "1998-12-01 12:30:45.123456"
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        crate::sql::temporal::parse_date32("1998-12-01").unwrap()
    );
    for (index, expected) in [
        (6, "2000-02-29 01:02:03"),
        (7, "1998-11-30 12:30:45.123456"),
    ] {
        assert_eq!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            parse(expected).unwrap()
        );
    }
}

#[tokio::test]
async fn executes_decimal_sum_and_average_semantics() {
    let catalog = Catalog::default();
    let plan = crate::sql::plan_sql(
        &catalog,
        "SELECT \
            sum(CAST(1.25 AS DECIMAL(5, 2))), \
            avg(CAST(1.25 AS DECIMAL(5, 2))), \
            sum(CAST(NULL AS DECIMAL(5, 2)))",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let batches = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let sum = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(sum.data_type(), &DataType::Decimal128(38, 2));
    assert_eq!(sum.value(0), 125);

    let average = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(average.value(0), 1.25);
    assert!(batches[0].column(2).is_null(0));
    assert_eq!(
        batches[0].column(2).data_type(),
        &DataType::Decimal128(38, 2)
    );
}

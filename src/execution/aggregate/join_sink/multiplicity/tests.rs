use std::{cell::Cell, sync::Arc};

use arrow::{
    array::{Decimal128Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::{
    execution::value::CellValue,
    runtime::{MemoryPool, QueryContext},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::{AggregateState, update_range};

#[test]
fn one_lookup_updates_all_probe_side_aggregates_by_multiplicity() {
    let probe = batch(
        vec![Some(10), Some(20), None, Some(40)],
        vec![Some(1), None, Some(3), Some(4)],
        vec![Some(100), Some(200), Some(300), None],
    );
    let aggregates = vec![
        count_star(),
        sum(0, DataType::Int64, 0, "signed"),
        sum(1, DataType::UInt64, 0, "unsigned"),
        sum(2, DataType::Decimal128(10, 2), 2, "decimal"),
    ];
    let mut states = aggregates
        .iter()
        .map(AggregateState::new)
        .collect::<Vec<_>>();
    let counts = [2, 1, 0, 3];
    let calls = Cell::new(0usize);
    let directory = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();

    let matched = update_range(
        &mut states,
        &aggregates,
        &probe,
        0..probe.num_rows(),
        |row| {
            calls.set(calls.get() + 1);
            Ok(counts[row])
        },
        &context,
    )
    .unwrap();

    assert_eq!(calls.get(), probe.num_rows());
    assert_eq!(matched, 6);
    let values = states
        .iter()
        .map(AggregateState::finish)
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        values,
        vec![
            CellValue::Int64(6),
            CellValue::Decimal128(160),
            CellValue::Decimal128(14),
            CellValue::Decimal128(400),
        ]
    );
}

#[test]
fn count_overflow_is_reported() {
    let probe = batch(vec![Some(1)], vec![Some(1)], vec![Some(1)]);
    let aggregates = vec![count_star()];
    let mut states = vec![AggregateState::Count(i64::MAX)];
    let directory = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();

    let error = update_range(
        &mut states,
        &aggregates,
        &probe,
        0..probe.num_rows(),
        |_| Ok(1),
        &context,
    )
    .unwrap_err();
    assert!(error.to_string().contains("count overflowed INT64"));
}

fn batch(
    signed: Vec<Option<i64>>,
    unsigned: Vec<Option<u64>>,
    decimal: Vec<Option<i128>>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("signed", DataType::Int64, true),
        Field::new("unsigned", DataType::UInt64, true),
        Field::new("decimal", DataType::Decimal128(10, 2), true),
    ]));
    let decimal = Decimal128Array::from(decimal)
        .with_precision_and_scale(10, 2)
        .unwrap();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(signed)),
            Arc::new(UInt64Array::from(unsigned)),
            Arc::new(decimal),
        ],
    )
    .unwrap()
}

fn count_star() -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct: false,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    }
}

fn sum(column: usize, input: DataType, scale: i8, name: &str) -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(column, input, name)),
        distinct: false,
        data_type: DataType::Decimal128(38, scale),
        display_name: format!("sum({name})"),
    }
}

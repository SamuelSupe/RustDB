use std::{cell::Cell, sync::Arc};

use arrow::{
    array::{Decimal128Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use tempfile::tempdir;

use crate::{
    execution::value::CellValue,
    runtime::{MemoryPool, QueryContext},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::{AggregateState, update};

#[test]
fn one_lookup_per_probe_row_updates_all_numeric_aggregates() {
    let probe = batch(
        vec![Some(10), Some(20), None, Some(40)],
        vec![Some(1), None, Some(3), Some(4)],
        vec![Some(100), Some(200), Some(300), None],
    );
    let build = batch(
        vec![Some(2), None, Some(5)],
        vec![Some(7), Some(8), None],
        vec![Some(11), None, Some(13)],
    );
    let aggregates = vec![
        count_star(),
        sum(0, DataType::Int64, 0, "probe_i64"),
        sum(1, DataType::UInt64, 0, "probe_u64"),
        sum(2, DataType::Decimal128(10, 2), 2, "probe_decimal"),
        sum(3, DataType::Int64, 0, "build_i64"),
        sum(4, DataType::UInt64, 0, "build_u64"),
        sum(5, DataType::Decimal128(10, 2), 2, "build_decimal"),
    ];
    let mut states = aggregate_states(&aggregates);
    let calls = Cell::new(0usize);
    let directory = tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();

    let matched = update(
        &mut states,
        &aggregates,
        &probe,
        &build,
        |row| {
            calls.set(calls.get() + 1);
            match row {
                0 => Some(&[0, 1][..]),
                1 => Some(&[2][..]),
                2 => None, // A NULL probe key never reaches a match group.
                3 => Some(&[0, 2][..]),
                _ => unreachable!(),
            }
        },
        &context,
    )
    .unwrap();

    assert_eq!(calls.get(), probe.num_rows());
    assert_eq!(matched, 5);
    assert_values(
        &states,
        &[
            CellValue::Int64(5),
            CellValue::Decimal128(120),
            CellValue::Decimal128(10),
            CellValue::Decimal128(400),
            CellValue::Decimal128(14),
            CellValue::Decimal128(22),
            CellValue::Decimal128(48),
        ],
    );
}

#[test]
fn matched_null_values_keep_sum_null() {
    let probe = batch(vec![None], vec![None], vec![None]);
    let build = batch(vec![None], vec![None], vec![None]);
    let aggregates = vec![
        count_star(),
        sum(0, DataType::Int64, 0, "probe"),
        sum(3, DataType::Int64, 0, "build"),
    ];
    let mut states = aggregate_states(&aggregates);
    let directory = tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();

    update(
        &mut states,
        &aggregates,
        &probe,
        &build,
        |_| Some(&[0]),
        &context,
    )
    .unwrap();

    assert_values(
        &states,
        &[CellValue::Int64(1), CellValue::Null, CellValue::Null],
    );
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

fn aggregate_states(aggregates: &[AggregateExpr]) -> Vec<AggregateState> {
    aggregates.iter().map(AggregateState::new).collect()
}

fn assert_values(states: &[AggregateState], expected: &[CellValue]) {
    let actual = states
        .iter()
        .map(AggregateState::finish)
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(actual, expected);
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

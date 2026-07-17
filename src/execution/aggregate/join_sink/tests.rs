use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::{GroupState, selected};
use crate::{
    execution::value::CellValue,
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

#[test]
fn selection_counts_pairs_and_sums_both_sides_without_materializing() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]))],
    )
    .unwrap();
    let right = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(2), Some(3)]))],
    )
    .unwrap();
    let aggregates = vec![count_star(), sum(0, "left"), sum(1, "right")];
    let mut state = GroupState::new(Vec::new(), &aggregates);

    selected::update(
        &mut state.aggregates,
        &aggregates,
        &left,
        &right,
        &[0, 0, 1, 2],
        &[Some(0), Some(1), Some(0), Some(1)],
    )
    .unwrap();

    assert_eq!(state.aggregates[0].finish().unwrap(), CellValue::Int64(4));
    assert_eq!(
        state.aggregates[1].finish().unwrap(),
        CellValue::Decimal128(50)
    );
    assert_eq!(
        state.aggregates[2].finish().unwrap(),
        CellValue::Decimal128(10)
    );
}

#[test]
fn selection_rejects_unmatched_rows_for_the_inner_join_sink() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let right = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2]))]).unwrap();
    let aggregates = vec![sum(1, "right")];
    let mut state = GroupState::new(Vec::new(), &aggregates);

    let error = selected::update(
        &mut state.aggregates,
        &aggregates,
        &left,
        &right,
        &[0],
        &[None],
    )
    .unwrap_err();

    assert!(error.to_string().contains("unmatched build row"));
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

fn sum(column: usize, name: &str) -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(column, DataType::Int64, name)),
        distinct: false,
        data_type: DataType::Decimal128(38, 0),
        display_name: format!("sum({name})"),
    }
}

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array},
    datatypes::DataType,
};

use super::{supports, update};
use crate::{
    execution::{aggregate::state::AggregateState, value::CellValue},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

#[test]
fn mixed_count_sum_and_average_preserve_null_semantics() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), None, Some(4)]));
    let aggregates = vec![
        expression(AggregateFunction::Count, None, DataType::Int64),
        expression(
            AggregateFunction::Count,
            Some(DataType::Int64),
            DataType::Int64,
        ),
        expression(
            AggregateFunction::Sum,
            Some(DataType::Int64),
            DataType::Decimal128(38, 0),
        ),
        expression(
            AggregateFunction::Avg,
            Some(DataType::Int64),
            DataType::Float64,
        ),
    ];
    let mut states = aggregates
        .iter()
        .map(AggregateState::new)
        .collect::<Vec<_>>();

    update(
        &mut states,
        &aggregates,
        &[
            None,
            Some(Arc::clone(&values)),
            Some(Arc::clone(&values)),
            Some(values),
        ],
        3,
    )
    .unwrap();

    assert_eq!(states[0].finish().unwrap(), CellValue::Int64(3));
    assert_eq!(states[1].finish().unwrap(), CellValue::Int64(2));
    assert_eq!(states[2].finish().unwrap(), CellValue::Decimal128(6));
    assert_eq!(states[3].finish().unwrap(), CellValue::Float64(3.0));
}

#[test]
fn decimal_batch_updates_keep_scale_and_nulls() {
    let data_type = DataType::Decimal128(10, 2);
    let values = Arc::new(
        Decimal128Array::from(vec![Some(125), None, Some(275)])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    ) as ArrayRef;
    let aggregates = vec![
        expression(
            AggregateFunction::Sum,
            Some(data_type.clone()),
            DataType::Decimal128(38, 2),
        ),
        expression(AggregateFunction::Avg, Some(data_type), DataType::Float64),
    ];
    let mut states = aggregates
        .iter()
        .map(AggregateState::new)
        .collect::<Vec<_>>();

    update(
        &mut states,
        &aggregates,
        &[Some(Arc::clone(&values)), Some(values)],
        3,
    )
    .unwrap();

    assert_eq!(states[0].finish().unwrap(), CellValue::Decimal128(400));
    assert_eq!(states[1].finish().unwrap(), CellValue::Float64(2.0));
}

#[test]
fn min_and_max_keep_the_generic_path() {
    let aggregate = expression(
        AggregateFunction::Min,
        Some(DataType::Int64),
        DataType::Int64,
    );
    assert!(!supports(&[aggregate]));
}

fn expression(
    function: AggregateFunction,
    input: Option<DataType>,
    output: DataType,
) -> AggregateExpr {
    AggregateExpr {
        function,
        expr: input.map(|data_type| BoundExpr::column(0, data_type, "value")),
        distinct: false,
        data_type: output,
        display_name: function.to_string(),
    }
}

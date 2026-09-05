use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Float64Array, Int8Array, Int64Array, UInt16Array},
    datatypes::DataType,
};

use super::{supports, update};
use crate::{
    execution::{
        aggregate::state::AggregateState,
        value::{CellValue, cell},
    },
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
fn numeric_min_and_max_use_the_batch_path() {
    let aggregates = vec![
        expression(
            AggregateFunction::Min,
            Some(DataType::Int64),
            DataType::Int64,
        ),
        expression(
            AggregateFunction::Max,
            Some(DataType::Int64),
            DataType::Int64,
        ),
    ];
    assert!(supports(&aggregates));

    let string_min = expression(AggregateFunction::Min, Some(DataType::Utf8), DataType::Utf8);
    assert!(!supports(&[string_min]));
}

#[test]
fn numeric_min_and_max_match_scalar_updates_across_batches() {
    let aggregates = vec![
        expression(AggregateFunction::Min, Some(DataType::Int8), DataType::Int8),
        expression(AggregateFunction::Max, Some(DataType::Int8), DataType::Int8),
        expression(
            AggregateFunction::Min,
            Some(DataType::UInt16),
            DataType::UInt16,
        ),
        expression(
            AggregateFunction::Max,
            Some(DataType::UInt16),
            DataType::UInt16,
        ),
        expression(
            AggregateFunction::Min,
            Some(DataType::Float64),
            DataType::Float64,
        ),
        expression(
            AggregateFunction::Max,
            Some(DataType::Float64),
            DataType::Float64,
        ),
        expression(
            AggregateFunction::Min,
            Some(DataType::Decimal128(10, 2)),
            DataType::Decimal128(10, 2),
        ),
        expression(
            AggregateFunction::Max,
            Some(DataType::Decimal128(10, 2)),
            DataType::Decimal128(10, 2),
        ),
    ];
    let first = vec![
        Arc::new(Int8Array::from(vec![Some(4), None, Some(-2), Some(0)])) as ArrayRef,
        Arc::new(Int8Array::from(vec![Some(4), None, Some(-2), Some(0)])) as ArrayRef,
        Arc::new(UInt16Array::from(vec![Some(4), None, Some(2), Some(0)])) as ArrayRef,
        Arc::new(UInt16Array::from(vec![Some(4), None, Some(2), Some(0)])) as ArrayRef,
        Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(-0.0),
            None,
            Some(7.0),
        ])) as ArrayRef,
        Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(-0.0),
            None,
            Some(7.0),
        ])) as ArrayRef,
        Arc::new(
            Decimal128Array::from(vec![Some(125), None, Some(275), Some(0)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ) as ArrayRef,
        Arc::new(
            Decimal128Array::from(vec![Some(125), None, Some(275), Some(0)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ) as ArrayRef,
    ];
    let second = vec![
        Arc::new(Int8Array::from(vec![Some(7), Some(-8), None, Some(3)])) as ArrayRef,
        Arc::new(Int8Array::from(vec![Some(7), Some(-8), None, Some(3)])) as ArrayRef,
        Arc::new(UInt16Array::from(vec![Some(7), Some(8), None, Some(3)])) as ArrayRef,
        Arc::new(UInt16Array::from(vec![Some(7), Some(8), None, Some(3)])) as ArrayRef,
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-3.5),
            Some(f64::NAN),
            None,
        ])) as ArrayRef,
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-3.5),
            Some(f64::NAN),
            None,
        ])) as ArrayRef,
        Arc::new(
            Decimal128Array::from(vec![Some(300), Some(75), None, Some(250)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ) as ArrayRef,
        Arc::new(
            Decimal128Array::from(vec![Some(300), Some(75), None, Some(250)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ) as ArrayRef,
    ];

    let mut batch_states = aggregates
        .iter()
        .map(AggregateState::new)
        .collect::<Vec<_>>();
    let mut scalar_states = aggregates
        .iter()
        .map(AggregateState::new)
        .collect::<Vec<_>>();
    for arrays in [first, second] {
        let inputs = arrays.iter().cloned().map(Some).collect::<Vec<_>>();
        update(&mut batch_states, &aggregates, &inputs, arrays[0].len()).unwrap();
        for row in 0..arrays[0].len() {
            for ((state, aggregate), array) in
                scalar_states.iter_mut().zip(&aggregates).zip(&arrays)
            {
                state
                    .update(aggregate, Some(cell(array, row).unwrap()))
                    .unwrap();
            }
        }
    }

    for (batch, scalar) in batch_states.iter().zip(&scalar_states) {
        assert_eq!(batch.finish().unwrap(), scalar.finish().unwrap());
    }
    assert_eq!(batch_states[0].finish().unwrap(), CellValue::Int64(-8));
    assert_eq!(batch_states[1].finish().unwrap(), CellValue::Int64(7));
    assert_eq!(batch_states[2].finish().unwrap(), CellValue::UInt64(0));
    assert_eq!(batch_states[3].finish().unwrap(), CellValue::UInt64(8));
    assert_eq!(batch_states[4].finish().unwrap(), CellValue::Float64(-3.5));
    let CellValue::Float64(max_float) = batch_states[5].finish().unwrap() else {
        panic!("float MAX should return FLOAT64");
    };
    assert!(max_float.is_nan());
    assert_eq!(batch_states[6].finish().unwrap(), CellValue::Decimal128(0));
    assert_eq!(
        batch_states[7].finish().unwrap(),
        CellValue::Decimal128(300)
    );
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

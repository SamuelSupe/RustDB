use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{ArrayRef, Decimal128Array, DictionaryArray, StringArray, UInt32Array},
    datatypes::{DataType, UInt32Type},
};

use super::DenseDictionaryBatch;
use crate::{
    Error,
    execution::value::CellValue,
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::super::{key::GroupKeyEncoder, state::GroupState};

#[test]
fn accumulates_count_and_nullable_decimal_sum_by_dictionary_slot() {
    let decimal_type = DataType::Decimal128(10, 2);
    let groups = dictionary(vec![Some(0), Some(0), Some(1), None]);
    let group_arrays = vec![groups];
    let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Utf8, "key")]);
    let encoded = encoder.encode(&group_arrays).unwrap();
    let quantities = Decimal128Array::from(vec![Some(100), None, Some(250), Some(400)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let aggregates = aggregates(decimal_type);
    let arrays = vec![None, Some(Arc::new(quantities) as ArrayRef)];
    let dense = DenseDictionaryBatch::try_new(&encoded, &group_arrays, &aggregates, &arrays, 4)
        .unwrap()
        .expect("eligible dense dictionary batch");
    let mut states = (0..3)
        .map(|_| GroupState::new(Vec::new(), &aggregates))
        .collect::<Vec<_>>();

    dense
        .apply(&[Some(0), Some(1), Some(2)], &mut states)
        .unwrap();

    assert_state(&states[0], 2, Some(100));
    assert_state(&states[1], 1, Some(250));
    assert_state(&states[2], 1, Some(400));
}

#[test]
fn rejects_non_star_count_before_state_updates() {
    let decimal_type = DataType::Decimal128(10, 2);
    let group_arrays = vec![dictionary(vec![Some(0)])];
    let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Utf8, "key")]);
    let encoded = encoder.encode(&group_arrays).unwrap();
    let values = Decimal128Array::from(vec![Some(100)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let expression = AggregateExpr {
        function: AggregateFunction::Count,
        expr: Some(BoundExpr::column(1, decimal_type, "quantity")),
        distinct: false,
        data_type: DataType::Int64,
        display_name: "count(quantity)".into(),
    };

    assert!(
        DenseDictionaryBatch::try_new(
            &encoded,
            &group_arrays,
            &[expression],
            &[Some(Arc::new(values) as ArrayRef)],
            1,
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn dense_decimal_prefix_canonicalizes_duplicate_values_and_nulls() {
    let groups = dictionary_values(
        vec![Some(0), Some(1), Some(2), None],
        vec![Some("A"), Some("A"), None],
    );
    let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Utf8, "key")]);
    let encoded = encoder.encode(&[groups]).unwrap();

    let slots = (0..4)
        .map(|row| encoded.dense_dictionary_slot(row).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(slots[0], slots[1]);
    assert_eq!(slots[2], slots[3]);
    assert_ne!(slots[0], slots[2]);
    assert_eq!(encoded.dense_dictionary_shape(), Some((4, 2)));
    assert_eq!(encoded.borrowed_key(0), encoded.borrowed_key(1));
    assert_eq!(encoded.borrowed_key(2), encoded.borrowed_key(3));
}

#[test]
fn dense_decimal_prefix_duplicate_slots_preserve_successful_row_order() {
    let value = 10_i128.pow(38) - 1;
    let (dense, aggregate) = decimal_sum_batch(
        dictionary_values(vec![Some(0), Some(1), Some(0)], vec![Some("A"), Some("A")]),
        vec![value, -value, value],
    );
    let mut states = vec![GroupState::new(
        Vec::new(),
        std::slice::from_ref(&aggregate),
    )];

    apply_single_group(&dense, &mut states).unwrap();

    assert_eq!(
        states[0].aggregates[0].finish().unwrap(),
        CellValue::Decimal128(value)
    );
}

#[test]
fn dense_decimal_prefix_reports_intermediate_overflow_in_row_order() {
    let value = 10_i128.pow(38) - 1;
    let (dense, aggregate) = decimal_sum_batch(
        dictionary_values(vec![Some(0), Some(1), Some(0)], vec![Some("A"), Some("A")]),
        vec![value, value, -value],
    );
    let mut states = vec![GroupState::new(
        Vec::new(),
        std::slice::from_ref(&aggregate),
    )];

    let error = apply_single_group(&dense, &mut states).unwrap_err();
    assert!(matches!(error, Error::Execution(message) if message == "decimal sum overflowed i128"));
}

#[test]
fn dense_decimal_prefix_checks_existing_state_across_batches() {
    let value = 10_i128.pow(38) - 1;
    let (first, aggregate) = decimal_sum_batch(
        dictionary_values(vec![Some(0)], vec![Some("A")]),
        vec![value],
    );
    let mut states = vec![GroupState::new(
        Vec::new(),
        std::slice::from_ref(&aggregate),
    )];
    apply_single_group(&first, &mut states).unwrap();

    let (second, _) = decimal_sum_batch(
        dictionary_values(vec![Some(0), Some(0)], vec![Some("A")]),
        vec![value, -value],
    );
    let error = apply_single_group(&second, &mut states).unwrap_err();
    assert!(matches!(error, Error::Execution(message) if message == "decimal sum overflowed i128"));
}

#[test]
fn dense_decimal_prefix_keeps_small_values_narrow() {
    let (dense, _) = decimal_sum_batch(
        dictionary_values(vec![Some(0), Some(0), Some(0)], vec![Some("A")]),
        vec![7, -3, 11],
    );

    assert!(!dense.has_wide_decimal());
}

#[test]
fn dense_workspace_estimate_includes_worst_case_wide_payload() {
    let aggregate_count = 3;
    let wide_payload =
        aggregate_count * super::MAX_DENSE_SLOTS * size_of::<super::WideDecimalPartial>();
    let dense_buffers = aggregate_count * size_of::<super::DensePartial>();

    assert!(super::workspace_estimate(aggregate_count) >= wide_payload + dense_buffers);
}

fn dictionary(keys: Vec<Option<u32>>) -> ArrayRef {
    dictionary_values(keys, vec![Some("A"), Some("B")])
}

fn dictionary_values(keys: Vec<Option<u32>>, values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(
        DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from(keys),
            Arc::new(StringArray::from(values)),
        )
        .unwrap(),
    )
}

fn decimal_sum_batch(groups: ArrayRef, values: Vec<i128>) -> (DenseDictionaryBatch, AggregateExpr) {
    let group_arrays = vec![groups];
    let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Utf8, "key")]);
    let encoded = encoder.encode(&group_arrays).unwrap();
    let aggregate = AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(
            1,
            DataType::Decimal128(38, 0),
            "quantity",
        )),
        distinct: false,
        data_type: DataType::Decimal128(38, 0),
        display_name: "sum(quantity)".into(),
    };
    let rows = values.len();
    let values = Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, 0)
            .unwrap(),
    ) as ArrayRef;
    let dense = DenseDictionaryBatch::try_new(
        &encoded,
        &group_arrays,
        std::slice::from_ref(&aggregate),
        &[Some(values)],
        rows,
    )
    .unwrap()
    .expect("eligible dense dictionary batch");
    (dense, aggregate)
}

fn apply_single_group(
    dense: &DenseDictionaryBatch,
    states: &mut [GroupState],
) -> crate::Result<()> {
    let state_by_slot = dense
        .representatives()
        .iter()
        .map(|representative| representative.map(|_| 0))
        .collect::<Vec<_>>();
    dense.apply(&state_by_slot, states)
}

fn aggregates(decimal_type: DataType) -> Vec<AggregateExpr> {
    vec![
        AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        },
        AggregateExpr {
            function: AggregateFunction::Sum,
            expr: Some(BoundExpr::column(1, decimal_type, "quantity")),
            distinct: false,
            data_type: DataType::Decimal128(38, 2),
            display_name: "sum(quantity)".into(),
        },
    ]
}

fn assert_state(state: &GroupState, count: i64, sum: Option<i128>) {
    assert_eq!(
        state.aggregates[0].finish().unwrap(),
        CellValue::Int64(count)
    );
    assert_eq!(
        state.aggregates[1].finish().unwrap(),
        sum.map(CellValue::Decimal128).unwrap_or(CellValue::Null)
    );
}

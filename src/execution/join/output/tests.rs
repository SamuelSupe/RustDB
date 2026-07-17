use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int64Array, UInt32Array, new_null_array},
    compute::take,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::{
    build_output, output_workspace_bytes, selected_rows_bytes, unmatched_right_workspace_bytes,
};
use crate::runtime::estimate_array_bytes;
use crate::sql::{JoinType, UNMATERIALIZED_FIELD_KEY};

#[test]
fn fixed_width_selection_uses_batch_sized_estimate() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow::datatypes::DataType::Int64,
        false,
    )]));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_new(schema, vec![values]).unwrap();

    let bytes = selected_rows_bytes(&batch, [Some(0), Some(0), Some(2)]).unwrap();

    assert_eq!(
        bytes,
        estimate_array_bytes(&arrow::datatypes::DataType::Int64, 3)
    );
}

#[test]
fn join_output_skips_unmaterialized_payload_columns() {
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        arrow::datatypes::DataType::Int64,
        false,
    )]));
    let left = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .unwrap();
    let right =
        RecordBatch::try_new(input_schema, vec![Arc::new(Int64Array::from(vec![9]))]).unwrap();
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("left", arrow::datatypes::DataType::Int64, true).with_metadata(HashMap::from([
            (UNMATERIALIZED_FIELD_KEY.to_owned(), "true".to_owned()),
        ])),
        Field::new("right", arrow::datatypes::DataType::Int64, false),
    ]));
    let estimate = output_workspace_bytes(
        &left,
        &right,
        &[0],
        &[Some(0)],
        JoinType::Inner,
        &output_schema,
    )
    .unwrap();

    let output = build_output(
        &left,
        &right,
        &[0],
        &[Some(0)],
        None,
        JoinType::Inner,
        Arc::clone(&output_schema),
    )
    .unwrap();

    assert!(estimate >= output.get_array_memory_size());
    assert!(output.column(0).is_null(0));
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        9
    );
}

#[test]
fn identity_left_selection_reuses_materialized_arrays() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7, 8, 9]))],
    )
    .unwrap();
    let right = RecordBatch::new_empty(Arc::new(Schema::empty()));

    let output = build_output(
        &left,
        &right,
        &[0, 1, 2],
        &[None, None, None],
        None,
        JoinType::Semi,
        schema,
    )
    .unwrap();

    assert!(Arc::ptr_eq(left.column(0), output.column(0)));
}

#[test]
fn reordered_left_selection_falls_back_to_take() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![7, 8, 9]))],
    )
    .unwrap();
    let right = RecordBatch::new_empty(Arc::new(Schema::empty()));

    let output = build_output(
        &left,
        &right,
        &[2, 1, 0],
        &[None, None, None],
        None,
        JoinType::Semi,
        schema,
    )
    .unwrap();

    assert!(!Arc::ptr_eq(left.column(0), output.column(0)));
    let values = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[9, 8, 7]);
}

#[test]
fn null_outputs_reserve_wide_mixed_schema() {
    const WIDTH: i32 = 4_096;
    let rows = 4usize;
    let left_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let left = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
    )
    .unwrap();
    let wide_schema = Arc::new(Schema::new(vec![
        Field::new("wide", DataType::FixedSizeBinary(WIDTH), true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let empty_wide = RecordBatch::new_empty(Arc::clone(&wide_schema));
    let left_output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("wide", DataType::FixedSizeBinary(WIDTH), true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let left_indices = (0..rows as u32).collect::<Vec<_>>();
    let right_indices = vec![None; rows];
    let estimate = output_workspace_bytes(
        &left,
        &empty_wide,
        &left_indices,
        &right_indices,
        JoinType::Left,
        &left_output_schema,
    )
    .unwrap();
    let output = build_output(
        &left,
        &empty_wide,
        &left_indices,
        &right_indices,
        None,
        JoinType::Left,
        left_output_schema,
    )
    .unwrap();
    assert!(estimate >= output.get_array_memory_size());

    let right_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let right = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(Int64Array::from(vec![9]))],
    )
    .unwrap();
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("wide", DataType::FixedSizeBinary(WIDTH), true),
        Field::new("text", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
    ]));
    let estimate =
        unmatched_right_workspace_bytes(&wide_schema, &right, &[0], &full_schema).unwrap();
    let indices = UInt32Array::from(vec![0_u32]);
    let actual = RecordBatch::try_new(
        full_schema,
        vec![
            new_null_array(&DataType::FixedSizeBinary(WIDTH), 1),
            new_null_array(&DataType::Utf8, 1),
            take(right.column(0).as_ref(), &indices, None).unwrap(),
        ],
    )
    .unwrap();
    assert!(estimate >= actual.get_array_memory_size());
}

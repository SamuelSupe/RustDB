use arrow::{
    array::{Array, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array},
    datatypes::DataType,
};

use super::{
    ComparisonOp, EncodedPredicateBlock, Encoding, Predicate, PredicateSidecarError, PredicateType,
};

fn retained(data_type: PredicateType, values: &[Option<i64>]) -> EncodedPredicateBlock {
    EncodedPredicateBlock::encode(data_type, values)
        .expect("encode predicate block")
        .expect("block should pass the half-width retention threshold")
}

#[test]
fn dictionary_round_trip_preserves_nulls_and_little_endian_format() {
    let values = (0..2_048)
        .map(|row| {
            (row % 17 != 0).then_some(match row % 3 {
                0 => -1_000_000,
                1 => 0,
                _ => 1_000_000,
            })
        })
        .collect::<Vec<_>>();
    let encoded = retained(PredicateType::Int32, &values);

    assert_eq!(encoded.encoding().unwrap(), Encoding::Dictionary);
    assert_eq!(&encoded.as_bytes()[..8], b"RDBPSC01");
    assert_eq!(&encoded.as_bytes()[8..10], &1_u16.to_le_bytes());
    let decoded = encoded.decode().unwrap();
    assert_eq!(decoded.data_type(), PredicateType::Int32);
    assert_eq!(decoded.values(), values);
    assert_eq!(
        EncodedPredicateBlock::from_bytes(encoded.as_bytes().to_vec())
            .unwrap()
            .decode()
            .unwrap(),
        decoded
    );
}

#[test]
fn frame_of_reference_round_trip_handles_full_signed_i64_span() {
    let values = (0..4_096)
        .map(|row| Some(i64::MIN + i64::from(row % 8)))
        .collect::<Vec<_>>();
    let encoded = retained(PredicateType::Int64, &values);

    assert_eq!(encoded.encoding().unwrap(), Encoding::FrameOfReference);
    assert_eq!(encoded.decode().unwrap().values(), values);
}

#[test]
fn supports_fixed_width_types_and_rejects_wide_decimal() {
    let supported = [
        (DataType::Int8, PredicateType::Int8),
        (DataType::Int16, PredicateType::Int16),
        (DataType::Int32, PredicateType::Int32),
        (DataType::Int64, PredicateType::Int64),
        (DataType::Date32, PredicateType::Date32),
        (
            DataType::Decimal128(18, 2),
            PredicateType::Decimal128 {
                precision: 18,
                scale: 2,
            },
        ),
    ];
    for (arrow, expected) in supported {
        assert_eq!(PredicateType::from_arrow(&arrow).unwrap(), expected);
    }
    assert!(matches!(
        PredicateType::from_arrow(&DataType::Decimal128(19, 2)),
        Err(PredicateSidecarError::UnsupportedType(_))
    ));

    for (data_type, pattern) in [
        (PredicateType::Int8, vec![Some(-128), None, Some(127)]),
        (
            PredicateType::Int16,
            vec![Some(-32_768), None, Some(32_767)],
        ),
        (
            PredicateType::Int32,
            vec![Some(i64::from(i32::MIN)), None, Some(i64::from(i32::MAX))],
        ),
        (
            PredicateType::Int64,
            vec![Some(i64::MIN), None, Some(i64::MAX)],
        ),
        (
            PredicateType::Date32,
            vec![Some(-10_000), None, Some(20_000)],
        ),
    ] {
        let values = pattern.repeat(2_048);
        assert_eq!(
            retained(data_type, &values).decode().unwrap().values(),
            values
        );
    }

    let decimal = [Some(12_345), None, Some(-700)].repeat(1_024);
    let encoded = retained(
        PredicateType::Decimal128 {
            precision: 18,
            scale: 2,
        },
        &decimal,
    );
    assert_eq!(encoded.decode().unwrap().values(), decimal);
}

#[test]
fn comparison_and_null_predicates_use_sql_filter_semantics() {
    let values = [Some(-2), None, Some(0), Some(4)].repeat(512);
    let encoded = retained(PredicateType::Int64, &values);

    assert_eq!(
        &encoded
            .evaluate(Predicate::Compare {
                op: ComparisonOp::GreaterOrEqual,
                value: 0,
            })
            .unwrap()[..4],
        &[false, false, true, true]
    );
    assert_eq!(
        &encoded.evaluate(Predicate::IsNull).unwrap()[..4],
        &[false, true, false, false]
    );
    assert_eq!(
        &encoded.evaluate(Predicate::IsNotNull).unwrap()[..4],
        &[true, false, true, true]
    );

    let decoded = encoded.decode().unwrap();
    for (op, expected) in [
        (ComparisonOp::Equal, [false, false, true, false]),
        (ComparisonOp::NotEqual, [true, false, false, true]),
        (ComparisonOp::Less, [true, false, false, false]),
        (ComparisonOp::LessOrEqual, [true, false, true, false]),
        (ComparisonOp::Greater, [false, false, false, true]),
        (ComparisonOp::GreaterOrEqual, [false, false, true, true]),
    ] {
        assert_eq!(
            &decoded.evaluate(Predicate::Compare { op, value: 0 })[..4],
            &expected
        );
    }
}

#[test]
fn borrowed_evaluation_rejects_a_misbound_logical_type() {
    let encoded = retained(PredicateType::Int64, &vec![Some(7_i64); 2_048]);

    assert!(matches!(
        EncodedPredicateBlock::evaluate_all_bytes_for_type(
            encoded.as_bytes(),
            &DataType::Date32,
            &[Predicate::IsNotNull],
        ),
        Err(PredicateSidecarError::Corrupt(message)) if message.contains("type mismatch")
    ));
}

#[test]
fn retention_threshold_counts_header_and_future_directory_entry() {
    let incompressible = (-128_i64..128).map(Some).collect::<Vec<_>>();
    assert!(
        EncodedPredicateBlock::encode(PredicateType::Int8, &incompressible)
            .unwrap()
            .is_none()
    );

    let compressible = vec![Some(7_i64); 4_096];
    let encoded = retained(PredicateType::Int8, &compressible);
    assert!((encoded.as_bytes().len() + 32) * 2 <= compressible.len());
}

#[test]
fn corrupted_padding_and_out_of_range_values_fail_closed() {
    let values = vec![Some(1_i64); 2_049];
    let encoded = retained(PredicateType::Int16, &values);
    let mut bytes = encoded.as_bytes().to_vec();
    let validity_last = 40 + values.len().div_ceil(8) - 1;
    bytes[validity_last] |= 0x80;
    assert!(matches!(
        EncodedPredicateBlock::from_bytes(bytes),
        Err(PredicateSidecarError::Corrupt(_))
    ));

    assert!(matches!(
        EncodedPredicateBlock::encode(PredicateType::Int8, &[Some(128)]),
        Err(PredicateSidecarError::ValueOutOfRange { .. })
    ));
    assert!(matches!(
        EncodedPredicateBlock::encode(
            PredicateType::Decimal128 {
                precision: 3,
                scale: 0,
            },
            &[Some(1_000)],
        ),
        Err(PredicateSidecarError::ValueOutOfRange { .. })
    ));
}

#[test]
fn selected_arrow_decode_preserves_type_order_and_nulls() {
    let selection = [true, true, false, true].repeat(1_024);

    let int8 = selected_array(
        PredicateType::Int8,
        &DataType::Int8,
        &[Some(-5), None, Some(0), Some(7)].repeat(1_024),
        &selection,
    );
    assert_eq!(
        int8.as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .take(3)
            .collect::<Vec<_>>(),
        vec![Some(-5), None, Some(7)]
    );

    let int16 = selected_array(
        PredicateType::Int16,
        &DataType::Int16,
        &[Some(-500), None, Some(0), Some(700)].repeat(1_024),
        &selection,
    );
    assert_eq!(
        int16
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .iter()
            .take(3)
            .collect::<Vec<_>>(),
        vec![Some(-500), None, Some(700)]
    );

    let int32 = selected_array(
        PredicateType::Int32,
        &DataType::Int32,
        &[Some(-50_000), None, Some(0), Some(70_000)].repeat(1_024),
        &selection,
    );
    assert_eq!(
        int32
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .take(3)
            .collect::<Vec<_>>(),
        vec![Some(-50_000), None, Some(70_000)]
    );

    let int64 = selected_array(
        PredicateType::Int64,
        &DataType::Int64,
        &[Some(-5_000_000), None, Some(0), Some(7_000_000)].repeat(1_024),
        &selection,
    );
    assert_eq!(
        int64
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .take(3)
            .collect::<Vec<_>>(),
        vec![Some(-5_000_000), None, Some(7_000_000)]
    );

    let date = selected_array(
        PredicateType::Date32,
        &DataType::Date32,
        &[Some(-10_000), None, Some(0), Some(20_000)].repeat(1_024),
        &selection,
    );
    assert_eq!(
        date.as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .iter()
            .take(3)
            .collect::<Vec<_>>(),
        vec![Some(-10_000), None, Some(20_000)]
    );

    let decimal_type = DataType::Decimal128(18, 2);
    let decimal = selected_array(
        PredicateType::Decimal128 {
            precision: 18,
            scale: 2,
        },
        &decimal_type,
        &[Some(-12_345), None, Some(0), Some(67_800)].repeat(1_024),
        &selection,
    );
    let decimal = decimal.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(decimal.data_type(), &decimal_type);
    assert_eq!(
        decimal.iter().take(3).collect::<Vec<_>>(),
        vec![Some(-12_345), None, Some(67_800)]
    );
}

#[test]
fn selected_arrow_decode_streams_frame_values_and_validates_all_rows() {
    let values = (0..4_096)
        .map(|row| (row % 11 != 0).then_some(i64::MIN + i64::from(row % 8)))
        .collect::<Vec<_>>();
    let encoded = retained(PredicateType::Int64, &values);
    assert_eq!(encoded.encoding().unwrap(), Encoding::FrameOfReference);

    let selection = (0..values.len())
        .map(|row| row % 5 == 0)
        .collect::<Vec<_>>();
    let array = EncodedPredicateBlock::decode_selected_bytes_for_type(
        encoded.as_bytes(),
        &DataType::Int64,
        &selection,
    )
    .unwrap();
    let actual = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    let expected = values
        .iter()
        .zip(&selection)
        .filter_map(|(value, selected)| selected.then_some(*value))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);

    let dictionary_values = [None, Some(-100_i64), Some(100)].repeat(1_024);
    let dictionary = retained(PredicateType::Int8, &dictionary_values);
    assert_eq!(dictionary.encoding().unwrap(), Encoding::Dictionary);
    let mut corrupt = dictionary.as_bytes().to_vec();
    let ids_offset = 40 + dictionary_values.len().div_ceil(8) + 16;
    corrupt[ids_offset] |= 1;
    assert!(matches!(
        EncodedPredicateBlock::decode_selected_bytes_for_type(
            &corrupt,
            &DataType::Int8,
            &vec![false; dictionary_values.len()],
        ),
        Err(PredicateSidecarError::Corrupt(message)) if message.contains("null dictionary row")
    ));
}

#[test]
fn selected_arrow_decode_rejects_bad_binding_and_selection_length() {
    let values = vec![Some(7_i64); 2_048];
    let encoded = retained(PredicateType::Int64, &values);

    assert!(matches!(
        EncodedPredicateBlock::decode_selected_bytes_for_type(
            encoded.as_bytes(),
            &DataType::Date32,
            &vec![true; values.len()],
        ),
        Err(PredicateSidecarError::Corrupt(message)) if message.contains("type mismatch")
    ));
    assert!(matches!(
        EncodedPredicateBlock::decode_selected_bytes_for_type(
            encoded.as_bytes(),
            &DataType::Int64,
            &vec![true; values.len() - 1],
        ),
        Err(PredicateSidecarError::Corrupt(message)) if message.contains("selection has")
    ));

    let empty = EncodedPredicateBlock::decode_selected_bytes_for_type(
        encoded.as_bytes(),
        &DataType::Int64,
        &vec![false; values.len()],
    )
    .unwrap();
    assert_eq!(empty.data_type(), &DataType::Int64);
    assert_eq!(empty.len(), 0);
}

fn selected_array(
    predicate_type: PredicateType,
    arrow_type: &DataType,
    values: &[Option<i64>],
    selection: &[bool],
) -> arrow::array::ArrayRef {
    let encoded = retained(predicate_type, values);
    EncodedPredicateBlock::decode_selected_bytes_for_type(encoded.as_bytes(), arrow_type, selection)
        .unwrap()
}

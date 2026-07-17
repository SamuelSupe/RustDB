use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryArray, DictionaryArray, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::{
        DataType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
        UInt64Type,
    },
};

use super::GroupKeyEncoder;
use crate::{
    execution::value::{CellValue, cell},
    sql::BoundExpr,
};

#[test]
fn all_integer_key_widths_hydrate_string_values_and_nulls() {
    macro_rules! check {
        ($key_type:ty, $array_type:ty, $zero:expr, $one:expr, $two:expr) => {{
            let values =
                Arc::new(StringArray::from(vec![Some("red"), Some("blue"), None])) as ArrayRef;
            let keys = <$array_type>::from(vec![Some($zero), Some($one), None, Some($two)]);
            let dictionary = Arc::new(
                DictionaryArray::<$key_type>::try_new(keys, values).expect("valid dictionary"),
            ) as ArrayRef;
            let plain = Arc::new(StringArray::from(vec![
                Some("red"),
                Some("blue"),
                None,
                None,
            ])) as ArrayRef;

            assert_same_rows(DataType::Utf8, &dictionary, &plain);
            assert_eq!(cell(&dictionary, 0).unwrap(), CellValue::Utf8("red".into()));
            assert_eq!(cell(&dictionary, 2).unwrap(), CellValue::Null);
            assert_eq!(cell(&dictionary, 3).unwrap(), CellValue::Null);
        }};
    }

    check!(Int8Type, Int8Array, 0_i8, 1_i8, 2_i8);
    check!(Int16Type, Int16Array, 0_i16, 1_i16, 2_i16);
    check!(Int32Type, Int32Array, 0_i32, 1_i32, 2_i32);
    check!(Int64Type, Int64Array, 0_i64, 1_i64, 2_i64);
    check!(UInt8Type, UInt8Array, 0_u8, 1_u8, 2_u8);
    check!(UInt16Type, UInt16Array, 0_u16, 1_u16, 2_u16);
    check!(UInt32Type, UInt32Array, 0_u32, 1_u32, 2_u32);
    check!(UInt64Type, UInt64Array, 0_u64, 1_u64, 2_u64);
}

#[test]
fn byte_value_families_match_plain_row_encoding() {
    let keys = || Int32Array::from(vec![Some(0), Some(1), None, Some(0)]);

    let utf8 = dictionary::<Int32Type>(keys(), Arc::new(StringArray::from(vec!["alpha", "beta"])));
    let plain_utf8 = Arc::new(StringArray::from(vec![
        Some("alpha"),
        Some("beta"),
        None,
        Some("alpha"),
    ])) as ArrayRef;
    assert_same_rows(DataType::Utf8, &utf8, &plain_utf8);

    let large_utf8 = dictionary::<Int32Type>(
        keys(),
        Arc::new(LargeStringArray::from(vec!["alpha", "beta"])),
    );
    let plain_large_utf8 = Arc::new(LargeStringArray::from(vec![
        Some("alpha"),
        Some("beta"),
        None,
        Some("alpha"),
    ])) as ArrayRef;
    assert_same_rows(DataType::LargeUtf8, &large_utf8, &plain_large_utf8);

    let binary = dictionary::<Int32Type>(
        keys(),
        Arc::new(BinaryArray::from(vec![&b"alpha"[..], &b"beta"[..]])),
    );
    let plain_binary = Arc::new(BinaryArray::from(vec![
        Some(&b"alpha"[..]),
        Some(&b"beta"[..]),
        None,
        Some(&b"alpha"[..]),
    ])) as ArrayRef;
    assert_same_rows(DataType::Binary, &binary, &plain_binary);

    let large_binary = dictionary::<Int32Type>(
        keys(),
        Arc::new(LargeBinaryArray::from(vec![&b"alpha"[..], &b"beta"[..]])),
    );
    let plain_large_binary = Arc::new(LargeBinaryArray::from(vec![
        Some(&b"alpha"[..]),
        Some(&b"beta"[..]),
        None,
        Some(&b"alpha"[..]),
    ])) as ArrayRef;
    assert_same_rows(DataType::LargeBinary, &large_binary, &plain_large_binary);

    assert_eq!(
        cell(&binary, 0).unwrap(),
        CellValue::Binary(b"alpha".to_vec())
    );
    assert_eq!(
        cell(&large_binary, 1).unwrap(),
        CellValue::Binary(b"beta".to_vec())
    );
}

#[test]
fn reordered_dictionaries_and_key_widths_keep_cross_batch_identity() {
    let first = dictionary::<Int8Type>(
        Int8Array::from(vec![Some(0), Some(1), None, Some(0)]),
        Arc::new(StringArray::from(vec!["red", "blue"])),
    );
    let second = dictionary::<UInt64Type>(
        UInt64Array::from(vec![Some(1), Some(0), None, Some(2)]),
        Arc::new(StringArray::from(vec!["blue", "red", "red"])),
    );
    let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Utf8, "key")]);
    let first_rows = encoder.encode(std::slice::from_ref(&first)).unwrap();
    let second_rows = encoder.encode(std::slice::from_ref(&second)).unwrap();

    for row in 0..4 {
        assert_eq!(
            first_rows.borrowed_key(row),
            second_rows.borrowed_key(row),
            "logical row {row} must not depend on dictionary ids"
        );
        assert_eq!(
            encoder
                .key(&first_rows, std::slice::from_ref(&first), row)
                .unwrap(),
            encoder
                .key(&second_rows, std::slice::from_ref(&second), row)
                .unwrap()
        );
    }
}

#[test]
fn incompatible_dictionary_value_type_is_rejected_without_panicking() {
    let dictionary = dictionary::<Int32Type>(
        Int32Array::from(vec![Some(0)]),
        Arc::new(BinaryArray::from(vec![&b"not utf8"[..]])),
    );
    let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Utf8, "key")]);

    let error = match encoder.encode(std::slice::from_ref(&dictionary)) {
        Ok(_) => panic!("physical Binary dictionary must not satisfy logical Utf8"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("incompatible"), "{error}");
}

fn dictionary<K>(keys: arrow::array::PrimitiveArray<K>, values: ArrayRef) -> ArrayRef
where
    K: arrow::datatypes::ArrowDictionaryKeyType,
{
    Arc::new(DictionaryArray::<K>::try_new(keys, values).expect("valid dictionary"))
}

fn assert_same_rows(data_type: DataType, dictionary: &ArrayRef, plain: &ArrayRef) {
    let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, data_type, "key")]);
    let dictionary_rows = encoder
        .encode(std::slice::from_ref(dictionary))
        .expect("dictionary row encoding");
    let plain_rows = encoder
        .encode(std::slice::from_ref(plain))
        .expect("plain row encoding");
    assert_eq!(dictionary.len(), plain.len());
    for row in 0..dictionary.len() {
        assert_eq!(
            dictionary_rows.borrowed_key(row),
            plain_rows.borrowed_key(row),
            "dictionary row {row} differs from its logical value"
        );
    }
}

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BinaryArray, DictionaryArray, StringArray, UInt32Array},
    datatypes::{DataType, UInt32Type},
};

use super::{EncodedGroupRows, GroupKeyEncoder};
use crate::sql::BoundExpr;

#[test]
fn cartesian_rows_normalize_two_columns_nulls_duplicates_and_reordering() {
    let first = [
        dictionary(
            vec![Some(0), Some(1), None, Some(2), Some(2), Some(1)],
            Arc::new(StringArray::from(vec![
                Some("red"),
                Some("blue"),
                Some("red"),
            ])),
        ),
        dictionary(
            vec![Some(0), Some(1), Some(0), None, Some(0), None],
            Arc::new(BinaryArray::from(vec![Some(&b"x"[..]), None])),
        ),
    ];
    let second = [
        dictionary(
            vec![Some(1), Some(0), None, Some(1), Some(1), Some(0)],
            Arc::new(StringArray::from(vec!["blue", "red"])),
        ),
        dictionary(
            vec![Some(1), Some(0), Some(1), None, Some(1), None],
            Arc::new(BinaryArray::from(vec![None, Some(&b"x"[..])])),
        ),
    ];
    let encoder = GroupKeyEncoder::new(&[
        BoundExpr::column(0, DataType::Utf8, "left"),
        BoundExpr::column(1, DataType::Binary, "right"),
    ]);
    let first_rows = encoder.encode(&first).unwrap();
    let second_rows = encoder.encode(&second).unwrap();

    assert!(matches!(&first_rows, EncodedGroupRows::Dictionary(_)));
    assert!(matches!(&second_rows, EncodedGroupRows::Dictionary(_)));
    for row in 0..first[0].len() {
        assert_eq!(first_rows.borrowed_key(row), second_rows.borrowed_key(row));
    }
    assert_eq!(first_rows.borrowed_key(0), first_rows.borrowed_key(4));
    assert_eq!(first_rows.borrowed_key(1), first_rows.borrowed_key(5));
}

#[test]
fn cartesian_product_above_limit_uses_existing_rows_path() {
    let values = Arc::new(StringArray::from_iter_values(
        (0..16).map(|value| format!("key-{value}")),
    ));
    let dictionary = dictionary((0_u32..16).map(Some).collect(), values);
    let encoder = GroupKeyEncoder::new(&[BoundExpr::column(0, DataType::Utf8, "key")]);

    assert!(matches!(
        encoder.encode(&[dictionary]).unwrap(),
        EncodedGroupRows::Rows(_)
    ));
}

fn dictionary(keys: Vec<Option<u32>>, values: ArrayRef) -> ArrayRef {
    Arc::new(
        DictionaryArray::<UInt32Type>::try_new(UInt32Array::from(keys), values)
            .expect("valid UInt32 dictionary"),
    )
}

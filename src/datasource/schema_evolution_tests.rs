use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use super::{ParquetSchemaMode, SchemaSource, merge_file_schemas, merge_types};

fn schema(fields: Vec<(&str, DataType, bool)>) -> Schema {
    Schema::new(
        fields
            .into_iter()
            .map(|(name, data_type, nullable)| Field::new(name, data_type, nullable))
            .collect::<Vec<_>>(),
    )
}

fn merge(
    inputs: &[(&str, &Schema)],
    mode: ParquetSchemaMode,
    previous: Option<&Schema>,
) -> crate::Result<Arc<Schema>> {
    let sources: Vec<_> = inputs
        .iter()
        .map(|(uri, schema)| SchemaSource { uri, schema })
        .collect();
    merge_file_schemas(&sources, mode, previous)
}

#[test]
fn strict_mode_is_uri_deterministic_and_merges_nullability() {
    let a = schema(vec![
        ("z", DataType::Int32, false),
        ("a", DataType::Utf8, false),
    ]);
    let b = schema(vec![
        ("a", DataType::Utf8, true),
        ("z", DataType::Int32, false),
    ]);

    let merged = merge(
        &[("s3://bucket/z.parquet", &b), ("s3://bucket/a.parquet", &a)],
        ParquetSchemaMode::Strict,
        None,
    )
    .unwrap();
    assert_eq!(merged.fields()[0].name(), "z");
    assert_eq!(merged.fields()[1].name(), "a");
    assert!(merged.field_with_name("a").unwrap().is_nullable());
    assert!(!merged.field_with_name("z").unwrap().is_nullable());
}

#[test]
fn strict_conflicts_include_uri_and_column() {
    let a = schema(vec![("id", DataType::Int32, false)]);
    let missing = schema(vec![("other", DataType::Int32, false)]);
    let error = merge(
        &[("file:///a.parquet", &a), ("file:///b.parquet", &missing)],
        ParquetSchemaMode::Strict,
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("column 'id'"), "{error}");
    assert!(error.contains("file:///a.parquet"), "{error}");
    assert!(error.contains("file:///b.parquet"), "{error}");

    let incompatible = schema(vec![("id", DataType::Utf8, false)]);
    let error = merge(
        &[
            ("file:///a.parquet", &a),
            ("file:///c.parquet", &incompatible),
        ],
        ParquetSchemaMode::Strict,
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("column 'id'"), "{error}");
    assert!(error.contains("file:///a.parquet"), "{error}");
    assert!(error.contains("file:///c.parquet"), "{error}");
}

#[test]
fn union_appends_new_columns_by_name_and_marks_missing_nullable() {
    let first = schema(vec![
        ("key", DataType::Int64, false),
        ("value", DataType::Utf8, false),
    ]);
    let second = schema(vec![
        ("z_new", DataType::Boolean, false),
        ("a_new", DataType::Binary, false),
        ("key", DataType::Int64, false),
    ]);
    let merged = merge(
        &[
            ("file:///b.parquet", &second),
            ("file:///a.parquet", &first),
        ],
        ParquetSchemaMode::UnionByName,
        None,
    )
    .unwrap();
    let names: Vec<_> = merged.fields().iter().map(|field| field.name()).collect();
    assert_eq!(names, ["key", "value", "a_new", "z_new"]);
    assert!(!merged.field_with_name("key").unwrap().is_nullable());
    for name in ["value", "a_new", "z_new"] {
        assert!(merged.field_with_name(name).unwrap().is_nullable());
    }
}

#[test]
fn safe_widening_does_not_fill_missing_columns() {
    let complete = schema(vec![
        ("id", DataType::Int32, false),
        ("value", DataType::Int32, false),
    ]);
    let missing = schema(vec![("id", DataType::Int64, false)]);
    let error = merge(
        &[
            ("file:///complete.parquet", &complete),
            ("file:///missing.parquet", &missing),
        ],
        ParquetSchemaMode::SafeWidening,
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("column 'value'"), "{error}");
    assert!(error.contains("only UnionByName"), "{error}");
}

#[test]
fn refresh_order_keeps_surviving_columns_then_sorts_new_names() {
    let previous = schema(vec![
        ("b", DataType::Int32, true),
        ("removed", DataType::Int32, true),
        ("a", DataType::Int32, true),
    ]);
    let current = schema(vec![
        ("d", DataType::Int32, false),
        ("a", DataType::Int32, false),
        ("c", DataType::Int32, false),
        ("b", DataType::Int32, false),
    ]);
    let merged = merge(
        &[("file:///current.parquet", &current)],
        ParquetSchemaMode::UnionByName,
        Some(&previous),
    )
    .unwrap();
    let names: Vec<_> = merged.fields().iter().map(|field| field.name()).collect();
    assert_eq!(names, ["b", "a", "c", "d"]);
}

#[test]
fn safe_integer_widening_matrix_is_lossless() {
    let types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    let decimal20 = DataType::Decimal128(20, 0);
    let expected = [
        [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            decimal20.clone(),
        ],
        [
            DataType::Int16,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            decimal20.clone(),
        ],
        [
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Int64,
            DataType::Int32,
            DataType::Int32,
            DataType::Int64,
            decimal20.clone(),
        ],
        [
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            decimal20.clone(),
        ],
        [
            DataType::Int16,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ],
        [
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt16,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ],
        [
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::UInt32,
            DataType::UInt32,
            DataType::UInt32,
            DataType::UInt64,
        ],
        [
            decimal20.clone(),
            decimal20.clone(),
            decimal20.clone(),
            decimal20,
            DataType::UInt64,
            DataType::UInt64,
            DataType::UInt64,
            DataType::UInt64,
        ],
    ];
    for (left_index, left) in types.iter().enumerate() {
        for (right_index, right) in types.iter().enumerate() {
            assert_eq!(
                merge_types(left, right, ParquetSchemaMode::SafeWidening).unwrap(),
                expected[left_index][right_index],
                "{left:?} + {right:?}"
            );
        }
    }
}

#[test]
fn numeric_widening_is_deterministic_in_uri_order() {
    let first = schema(vec![("value", DataType::Int8, false)]);
    let second = schema(vec![("value", DataType::UInt8, false)]);
    let third = schema(vec![("value", DataType::Decimal128(3, 0), false)]);
    let forward = merge(
        &[
            ("file:///a.parquet", &first),
            ("file:///b.parquet", &second),
            ("file:///c.parquet", &third),
        ],
        ParquetSchemaMode::SafeWidening,
        None,
    )
    .unwrap();
    let reversed = merge(
        &[
            ("file:///c.parquet", &third),
            ("file:///b.parquet", &second),
            ("file:///a.parquet", &first),
        ],
        ParquetSchemaMode::SafeWidening,
        None,
    )
    .unwrap();
    assert_eq!(forward, reversed);
    assert_eq!(forward.field(0).data_type(), &DataType::Decimal128(5, 0));
}

#[test]
fn decimal_widening_preserves_integer_and_fractional_digits() {
    let cases = [
        (
            DataType::Decimal128(8, 2),
            DataType::Decimal128(10, 4),
            DataType::Decimal128(10, 4),
        ),
        (
            DataType::Decimal128(8, 2),
            DataType::Int32,
            DataType::Decimal128(12, 2),
        ),
        (
            DataType::Decimal128(3, -2),
            DataType::UInt16,
            DataType::Decimal128(5, 0),
        ),
    ];
    for (left, right, expected) in cases {
        assert_eq!(
            merge_types(&left, &right, ParquetSchemaMode::SafeWidening).unwrap(),
            expected
        );
    }

    let error = merge_types(
        &DataType::Decimal128(38, 0),
        &DataType::Decimal128(38, 1),
        ParquetSchemaMode::SafeWidening,
    )
    .unwrap_err();
    assert!(error.contains("exceeding 38"), "{error}");
}

#[test]
fn scalar_safe_widening_rules_are_explicit() {
    let utc = Some("UTC".into());
    let cases = [
        (DataType::Float16, DataType::Float32, DataType::Float64),
        (DataType::Utf8, DataType::LargeUtf8, DataType::LargeUtf8),
        (
            DataType::Binary,
            DataType::LargeBinary,
            DataType::LargeBinary,
        ),
        (
            DataType::Timestamp(TimeUnit::Second, utc.clone()),
            DataType::Timestamp(TimeUnit::Nanosecond, utc.clone()),
            DataType::Timestamp(TimeUnit::Nanosecond, utc.clone()),
        ),
    ];
    for (left, right, expected) in cases {
        assert_eq!(
            merge_types(&left, &right, ParquetSchemaMode::SafeWidening).unwrap(),
            expected
        );
    }
    assert!(
        merge_types(
            &DataType::Float64,
            &DataType::Int64,
            ParquetSchemaMode::SafeWidening
        )
        .is_err()
    );
    assert!(
        merge_types(
            &DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Second, None),
            ParquetSchemaMode::SafeWidening
        )
        .is_err()
    );
}

#[test]
fn float_and_timestamp_type_matrices_are_deterministic() {
    let floats = [DataType::Float16, DataType::Float32, DataType::Float64];
    let expected = [
        [DataType::Float16, DataType::Float64, DataType::Float64],
        [DataType::Float64, DataType::Float32, DataType::Float64],
        [DataType::Float64, DataType::Float64, DataType::Float64],
    ];
    for (left_index, left) in floats.iter().enumerate() {
        for (right_index, right) in floats.iter().enumerate() {
            assert_eq!(
                merge_types(left, right, ParquetSchemaMode::SafeWidening).unwrap(),
                expected[left_index][right_index]
            );
        }
    }

    let units = [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    for (left_index, left) in units.iter().enumerate() {
        for (right_index, right) in units.iter().enumerate() {
            let merged = merge_types(
                &DataType::Timestamp(*left, Some("UTC".into())),
                &DataType::Timestamp(*right, Some("UTC".into())),
                ParquetSchemaMode::SafeWidening,
            )
            .unwrap();
            assert_eq!(
                merged,
                DataType::Timestamp(units[left_index.max(right_index)], Some("UTC".into()))
            );
        }
    }
}

#[test]
fn dictionary_values_are_unwrapped_but_nested_types_stay_exact() {
    let dictionary = schema(vec![(
        "name",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        false,
    )]);
    let plain = schema(vec![("name", DataType::Utf8, false)]);
    let merged = merge(
        &[
            ("file:///dictionary.parquet", &dictionary),
            ("file:///plain.parquet", &plain),
        ],
        ParquetSchemaMode::Strict,
        None,
    )
    .unwrap();
    assert_eq!(merged.field(0).data_type(), &DataType::Utf8);

    let list_i8 = DataType::List(Arc::new(Field::new("item", DataType::Int8, true)));
    let list_i16 = DataType::List(Arc::new(Field::new("item", DataType::Int16, true)));
    let left = schema(vec![("items", list_i8, true)]);
    let right = schema(vec![("items", list_i16, true)]);
    let error = merge(
        &[
            ("file:///left.parquet", &left),
            ("file:///right.parquet", &right),
        ],
        ParquetSchemaMode::SafeWidening,
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("column 'items'"), "{error}");
    assert!(error.contains("no lossless top-level"), "{error}");
}

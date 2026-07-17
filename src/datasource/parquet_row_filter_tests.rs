use std::sync::Arc;

use super::*;
use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

fn schema(data_type: DataType) -> Schema {
    Schema::new(vec![Field::new("value", data_type, true)])
}

#[test]
fn fixed_comparison_preserves_nulls_for_reader_filtering() {
    let schema = schema(DataType::Int64);
    let predicate = ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Gt,
        value: PredicateValue::Int64(2),
    };
    let filter = ParquetRowFilter::try_new(Some(&predicate), &schema, &schema).unwrap();
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(3), None]));
    let result = filter.columns[0].evaluate(&input).unwrap();

    assert_eq!(result.len(), 3);
    assert!(!result.value(0));
    assert!(result.value(1));
    assert!(result.is_null(2));
}

#[test]
fn conjunction_keeps_supported_leaves_but_or_is_not_partially_pushed() {
    let schema = schema(DataType::Int64);
    let supported = ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::GtEq,
        value: PredicateValue::Int64(7),
    };
    let unsupported = ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Eq,
        value: PredicateValue::Utf8("7".to_owned()),
    };
    let conjunction = ScanPredicate::And(vec![supported.clone(), unsupported.clone()]);
    let filter = ParquetRowFilter::try_new(Some(&conjunction), &schema, &schema).unwrap();
    assert_eq!(filter.columns.len(), 1);
    assert_eq!(filter.columns[0].leaves.len(), 1);

    let disjunction = ScanPredicate::Or(vec![supported, unsupported]);
    assert!(ParquetRowFilter::try_new(Some(&disjunction), &schema, &schema).is_none());
    assert_eq!(workspace_bytes(Some(&disjunction), &schema, 8_192), 0);
}

#[test]
fn same_physical_column_is_grouped_and_evaluated_in_leaf_order() {
    let schema = Schema::new(vec![
        Field::new("first", DataType::Int64, true),
        Field::new("second", DataType::Int64, true),
    ]);
    let predicate = ScanPredicate::And(vec![
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Gt,
            value: PredicateValue::Int64(1),
        },
        ScanPredicate::Comparison {
            column: 1,
            op: ComparisonOp::Lt,
            value: PredicateValue::Int64(10),
        },
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Lt,
            value: PredicateValue::Int64(4),
        },
    ]);
    let filter = ParquetRowFilter::try_new(Some(&predicate), &schema, &schema).unwrap();

    assert_eq!(filter.columns.len(), 2);
    assert_eq!(filter.columns[0].file_column, 0);
    assert_eq!(filter.columns[0].leaves.len(), 2);
    assert_eq!(filter.columns[0].mask_buffers(), 3);
    assert!(matches!(
        filter.columns[0].leaves[0],
        LeafPredicate::Comparison {
            op: ComparisonOp::Gt,
            ..
        }
    ));
    assert!(matches!(
        filter.columns[0].leaves[1],
        LeafPredicate::Comparison {
            op: ComparisonOp::Lt,
            ..
        }
    ));
    assert_eq!(filter.columns[1].file_column, 1);

    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(4), None]));
    let result = filter.columns[0].evaluate(&input).unwrap();
    assert_eq!(
        result.iter().collect::<Vec<_>>(),
        vec![Some(false), Some(true), Some(false), None,]
    );
}

#[test]
fn range_bounds_preserve_open_and_closed_semantics() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(2), None]));
    for (lower_op, upper_op, expected) in [
        (
            ComparisonOp::Gt,
            ComparisonOp::Lt,
            vec![Some(false), Some(true), Some(false), None],
        ),
        (
            ComparisonOp::GtEq,
            ComparisonOp::Lt,
            vec![Some(true), Some(true), Some(false), None],
        ),
        (
            ComparisonOp::Gt,
            ComparisonOp::LtEq,
            vec![Some(false), Some(true), Some(true), None],
        ),
        (
            ComparisonOp::GtEq,
            ComparisonOp::LtEq,
            vec![Some(true), Some(true), Some(true), None],
        ),
    ] {
        let schema = schema(DataType::Int64);
        let predicate = ScanPredicate::And(vec![
            ScanPredicate::Comparison {
                column: 0,
                op: lower_op,
                value: PredicateValue::Int64(0),
            },
            ScanPredicate::Comparison {
                column: 0,
                op: upper_op,
                value: PredicateValue::Int64(2),
            },
        ]);
        let filter = ParquetRowFilter::try_new(Some(&predicate), &schema, &schema).unwrap();
        assert_eq!(
            filter.columns[0]
                .evaluate(&input)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn unsupported_range_shapes_keep_the_leaf_fallback() {
    let int_schema = schema(DataType::Int64);
    for predicate in [
        ScanPredicate::And(vec![
            ScanPredicate::Comparison {
                column: 0,
                op: ComparisonOp::Gt,
                value: PredicateValue::Int64(0),
            },
            ScanPredicate::Comparison {
                column: 0,
                op: ComparisonOp::GtEq,
                value: PredicateValue::Int64(1),
            },
        ]),
        ScanPredicate::And(vec![
            ScanPredicate::Comparison {
                column: 0,
                op: ComparisonOp::Gt,
                value: PredicateValue::Int64(0),
            },
            ScanPredicate::IsNotNull { column: 0 },
        ]),
    ] {
        let filter = ParquetRowFilter::try_new(Some(&predicate), &int_schema, &int_schema).unwrap();
        assert_eq!(filter.columns[0].mask_buffers(), 3);
    }

    let float_schema = schema(DataType::Float64);
    let float_range = ScanPredicate::And(vec![
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::GtEq,
            value: PredicateValue::Float64(0.0),
        },
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Lt,
            value: PredicateValue::Float64(1.0),
        },
    ]);
    let filter =
        ParquetRowFilter::try_new(Some(&float_range), &float_schema, &float_schema).unwrap();
    assert_eq!(filter.columns[0].mask_buffers(), 3);
}

#[test]
fn schema_widening_and_decimal_mismatch_fall_back() {
    let table = schema(DataType::Int64);
    let file = schema(DataType::Int32);
    let integer = ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Eq,
        value: PredicateValue::Int64(1),
    };
    assert!(ParquetRowFilter::try_new(Some(&integer), &file, &table).is_none());

    let decimal_schema = schema(DataType::Decimal128(12, 2));
    let mismatch = ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Eq,
        value: PredicateValue::Decimal128 {
            value: 100,
            precision: 10,
            scale: 2,
        },
    };
    assert!(ParquetRowFilter::try_new(Some(&mismatch), &decimal_schema, &decimal_schema).is_none());
}

#[test]
fn workspace_accounts_filter_columns_and_boolean_masks() {
    let schema = schema(DataType::Int64);
    let predicate = ScanPredicate::And(vec![
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Gt,
            value: PredicateValue::Int64(1),
        },
        ScanPredicate::IsNotNull { column: 0 },
    ]);

    let expected = estimate_array_bytes(&DataType::Int64, 1024)
        + 3 * estimate_array_bytes(&DataType::Boolean, 1024);
    assert_eq!(workspace_bytes(Some(&predicate), &schema, 1024), expected);
}

#[test]
fn workspace_counts_three_masks_for_a_two_leaf_range() {
    let schema = schema(DataType::Int64);
    let predicate = ScanPredicate::And(vec![
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::GtEq,
            value: PredicateValue::Int64(1),
        },
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Lt,
            value: PredicateValue::Int64(4),
        },
    ]);
    let expected = estimate_array_bytes(&DataType::Int64, 1024)
        + 3 * estimate_array_bytes(&DataType::Boolean, 1024);
    assert_eq!(workspace_bytes(Some(&predicate), &schema, 1024), expected);
}

#[test]
fn strict_three_column_conjunction_fuses_with_the_same_mask() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]);
    let predicate = ScanPredicate::And(vec![
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Gt,
            value: PredicateValue::Int64(1),
        },
        ScanPredicate::Comparison {
            column: 1,
            op: ComparisonOp::Lt,
            value: PredicateValue::Int64(5),
        },
        ScanPredicate::IsNotNull { column: 2 },
    ]);
    let filter = ParquetRowFilter::try_new_strict(Some(&predicate), &schema, &schema).unwrap();
    assert!(filter.fuse_columns);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(3), None])),
            Arc::new(Int64Array::from(vec![Some(4), Some(4), Some(6), Some(1)])),
            Arc::new(Int64Array::from(vec![Some(9), Some(9), Some(9), None])),
        ],
    )
    .unwrap();
    let mask = fused::evaluate(&filter.columns, &batch).unwrap();
    assert_eq!(
        mask.iter().collect::<Vec<_>>(),
        vec![Some(true), Some(false), Some(false), None]
    );
}

#[test]
fn fusion_requires_strict_complete_three_column_predicates() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]);
    let supported = ScanPredicate::And(
        (0..3)
            .map(|column| ScanPredicate::Comparison {
                column,
                op: ComparisonOp::Gt,
                value: PredicateValue::Int64(0),
            })
            .collect(),
    );
    assert!(
        !ParquetRowFilter::try_new(Some(&supported), &schema, &schema)
            .unwrap()
            .fuse_columns
    );

    let two_columns = ScanPredicate::And(match &supported {
        ScanPredicate::And(predicates) => predicates[..2].to_vec(),
        _ => unreachable!(),
    });
    assert!(
        !ParquetRowFilter::try_new_strict(Some(&two_columns), &schema, &schema)
            .unwrap()
            .fuse_columns
    );

    let mut incomplete = match supported {
        ScanPredicate::And(predicates) => predicates,
        _ => unreachable!(),
    };
    incomplete.push(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Eq,
        value: PredicateValue::Utf8("unsupported".to_owned()),
    });
    assert!(
        !ParquetRowFilter::try_new_strict(Some(&ScanPredicate::And(incomplete)), &schema, &schema,)
            .unwrap()
            .fuse_columns
    );
}

#[test]
fn fused_filter_only_hints_sparse_selection_for_unfiltered_decimal_payload() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
        Field::new("payload", DataType::Decimal128(15, 2), true),
    ]);
    let predicate = ScanPredicate::And(
        (0..3)
            .map(|column| ScanPredicate::Comparison {
                column,
                op: ComparisonOp::Gt,
                value: PredicateValue::Int64(0),
            })
            .collect(),
    );
    let strict = ParquetRowFilter::try_new_strict(Some(&predicate), &schema, &schema).unwrap();

    assert!(strict.has_unfiltered_decimal_payload(&[0, 1, 2, 3], &schema));
    assert!(!strict.has_unfiltered_decimal_payload(&[0, 1, 2], &schema));
    assert!(
        !ParquetRowFilter::try_new(Some(&predicate), &schema, &schema)
            .unwrap()
            .has_unfiltered_decimal_payload(&[0, 1, 2, 3], &schema)
    );
}

#[test]
fn exact_constructor_is_fail_closed_for_float_predicates() {
    let schema = Schema::new(vec![Field::new("score", DataType::Float64, true)]);
    let predicate = ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Gt,
        value: PredicateValue::Float64(1.0),
    };
    let error = ParquetRowFilter::try_new_exact(Some(&predicate), &schema, &schema).unwrap_err();
    assert!(error.to_string().contains("not fully supported"));
}

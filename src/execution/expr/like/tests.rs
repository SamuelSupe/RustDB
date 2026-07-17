use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, StringArray};

use super::{evaluate, evaluate_literal};

fn values(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

#[test]
fn literal_matchers_preserve_like_semantics() {
    let input = values(vec![
        Some("forest green"),
        Some("special pending requests"),
        Some("é"),
        Some("a_b"),
        None,
    ]);

    assert_eq!(
        evaluate_literal(&input, "forest%", false, None).unwrap(),
        BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None
        ])
    );
    assert_eq!(
        evaluate_literal(&input, "%special%requests%", true, None).unwrap(),
        BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true), None])
    );
    assert_eq!(
        evaluate_literal(&input, "_", false, None).unwrap(),
        BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            None
        ])
    );
    assert_eq!(
        evaluate_literal(&input, "a!_b", false, Some('!')).unwrap(),
        BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            None
        ])
    );
    assert_eq!(
        evaluate_literal(&input, "%%special%%requests%%", false, None).unwrap(),
        BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            None
        ])
    );

    let unicode = values(vec![Some("é"), Some("e\u{301}")]);
    assert_eq!(
        evaluate_literal(&unicode, "_", false, None).unwrap(),
        BooleanArray::from(vec![Some(true), Some(false)])
    );
    assert_eq!(
        evaluate_literal(&values(vec![Some("\\")]), "\\", false, None).unwrap(),
        BooleanArray::from(vec![Some(true)])
    );

    let patterns: ArrayRef = Arc::new(StringArray::from(vec![
        Some("forest%"),
        Some("%special%requests%"),
        Some("_"),
        Some("a!_b"),
        None,
    ]));
    assert_eq!(
        evaluate(&input, &patterns, false, Some('!')).unwrap(),
        BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(true), None])
    );

    let nulls = values(vec![None, None]);
    assert_eq!(
        evaluate_literal(&nulls, "trailing!", false, Some('!')).unwrap(),
        BooleanArray::from(vec![None, None])
    );
    assert!(evaluate_literal(&input, "trailing!", false, Some('!')).is_err());
}

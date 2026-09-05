use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, UInt32Array},
    compute::take,
    datatypes::{Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};

use crate::{
    Error, Result,
    sql::{BinaryOp, BoundExpr, ExprKind, JoinType},
};

use super::{cell, evaluate};

#[derive(Clone)]
pub(super) struct JoinPredicates {
    residual: Option<BoundExpr>,
    null_aware: Option<(BoundExpr, BoundExpr)>,
    joined_schema: SchemaRef,
    residual_columns: Vec<usize>,
    projected_residual: Option<BoundExpr>,
}

impl JoinPredicates {
    pub(super) fn new(
        residual: Option<BoundExpr>,
        null_aware: Option<(BoundExpr, BoundExpr)>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
    ) -> Self {
        let fields = left_schema
            .fields()
            .iter()
            .chain(right_schema.fields())
            .cloned()
            .collect::<Vec<_>>();
        let mut residual_columns = Vec::new();
        if let Some(predicate) = &residual {
            predicate.referenced_columns(&mut residual_columns);
        }
        residual_columns.sort_unstable();
        residual_columns.dedup();
        let mut projected_residual = residual.clone();
        if let Some(predicate) = &mut projected_residual {
            predicate
                .rewrite_columns(&mut |index| {
                    Ok(residual_columns
                        .binary_search(&index)
                        .expect("referenced column collected"))
                })
                .expect("column projection is infallible");
        }
        Self {
            residual_columns,
            projected_residual,
            residual,
            null_aware,
            joined_schema: Arc::new(Schema::new(fields)),
        }
    }

    pub(super) fn candidate_projection(&self) -> Option<&[usize]> {
        (!self.is_null_aware()).then_some(self.residual_columns.as_slice())
    }

    pub(super) fn residual(&self) -> Option<&BoundExpr> {
        self.residual.as_ref()
    }

    pub(super) fn left_value(&self) -> Option<&BoundExpr> {
        self.null_aware.as_ref().map(|(left, _)| left)
    }

    pub(super) fn right_value(&self) -> Option<&BoundExpr> {
        self.null_aware.as_ref().map(|(_, right)| right)
    }

    pub(super) fn is_null_aware(&self) -> bool {
        self.null_aware.is_some()
    }

    pub(super) fn is_simple_equality(&self) -> bool {
        self.residual.is_none() && self.null_aware.is_none()
    }

    /// Returns the RHS value for the Q21-style existence predicate
    /// `left.value <> right.value`. Two distinct non-NULL RHS values per
    /// equality key are sufficient to answer that predicate for every LHS
    /// value, so the hash build may safely summarize duplicate rows.
    pub(super) fn existence_inequality_right_value(
        &self,
        join_type: JoinType,
        left_width: usize,
    ) -> Option<BoundExpr> {
        if !matches!(join_type, JoinType::Semi | JoinType::Anti) || self.null_aware.is_some() {
            return None;
        }
        let ExprKind::Binary {
            left,
            op: BinaryOp::NotEq,
            right,
        } = &self.residual.as_ref()?.kind
        else {
            return None;
        };
        let (ExprKind::Column(left_index), ExprKind::Column(right_index)) =
            (&left.kind, &right.kind)
        else {
            return None;
        };
        let joined_width = self.joined_schema.fields().len();
        let right_joined_index = match (*left_index < left_width, *right_index < left_width) {
            (true, false) => *right_index,
            (false, true) => *left_index,
            _ => return None,
        };
        if right_joined_index >= joined_width {
            return None;
        }
        let field = self.joined_schema.field(right_joined_index);
        Some(BoundExpr::column(
            right_joined_index - left_width,
            field.data_type().clone(),
            field.name(),
        ))
    }

    pub(super) fn evaluate_candidates(
        &self,
        left: &RecordBatch,
        right: &RecordBatch,
        left_indices: &[u32],
        right_indices: &[u32],
        left_values: Option<&ArrayRef>,
        right_values: Option<&ArrayRef>,
    ) -> Result<Vec<CandidateOutcome>> {
        if left_indices.len() != right_indices.len() {
            return Err(Error::Internal(
                "join candidate index vectors have different lengths".into(),
            ));
        }
        let residual = match &self.projected_residual {
            Some(predicate) => {
                let batch = candidate_batch(
                    left,
                    right,
                    left_indices,
                    right_indices,
                    Arc::new(self.joined_schema.project(&self.residual_columns)?),
                    &self.residual_columns,
                )?;
                let values = evaluate(predicate, &batch)?;
                Some(
                    values
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            Error::Internal(format!(
                                "join residual returned {}, expected BOOLEAN",
                                values.data_type()
                            ))
                        })?
                        .clone(),
                )
            }
            None => None,
        };

        if self.null_aware.is_some() && (left_values.is_none() || right_values.is_none()) {
            return Err(Error::Internal(
                "null-aware join is missing evaluated membership values".into(),
            ));
        }

        (0..left_indices.len())
            .map(|candidate| {
                let qualifies = residual
                    .as_ref()
                    .is_none_or(|values| values.is_valid(candidate) && values.value(candidate));
                let membership = if qualifies && self.null_aware.is_some() {
                    let left_row = left_indices[candidate] as usize;
                    let right_row = right_indices[candidate] as usize;
                    let left = cell(left_values.expect("checked above"), left_row)?;
                    let right = cell(right_values.expect("checked above"), right_row)?;
                    Some(if left.is_null() || right.is_null() {
                        SqlTruth::Unknown
                    } else if left == right {
                        SqlTruth::True
                    } else {
                        SqlTruth::False
                    })
                } else {
                    None
                };
                Ok(CandidateOutcome {
                    qualifies,
                    membership,
                })
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SqlTruth {
    True,
    False,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CandidateOutcome {
    pub(super) qualifies: bool,
    pub(super) membership: Option<SqlTruth>,
}

fn candidate_batch(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[u32],
    schema: SchemaRef,
    projection: &[usize],
) -> Result<RecordBatch> {
    if left_indices.is_empty() || right_indices.is_empty() {
        if left_indices.len() != right_indices.len() {
            return Err(Error::Internal(
                "Join candidate index arrays have different lengths".to_owned(),
            ));
        }
        return Ok(RecordBatch::new_empty(schema));
    }
    let left_indices = UInt32Array::from(left_indices.to_vec());
    let right_indices = UInt32Array::from(right_indices.to_vec());
    let columns = projection
        .iter()
        .map(|index| {
            if *index < left.num_columns() {
                Ok(take(left.column(*index).as_ref(), &left_indices, None)?)
            } else {
                let column = right
                    .columns()
                    .get(*index - left.num_columns())
                    .ok_or_else(|| {
                        Error::Internal("join residual column is outside input".into())
                    })?;
                Ok(take(column.as_ref(), &right_indices, None)?)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(left_indices.len())),
    )?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::{JoinPredicates, SqlTruth, candidate_batch};
    use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

    #[test]
    fn residual_and_membership_are_evaluated_on_candidate_pairs() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Int64, true)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("r", DataType::Int64, true)]));
        let left = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let residual = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(BoundExpr::column(0, DataType::Int64, "l")),
                op: BinaryOp::Eq,
                right: Box::new(BoundExpr::column(1, DataType::Int64, "r")),
            },
            data_type: DataType::Boolean,
            display_name: "l = r".into(),
        };
        let predicates = JoinPredicates::new(
            Some(residual),
            Some((
                BoundExpr::column(0, DataType::Int64, "l"),
                BoundExpr::column(0, DataType::Int64, "r"),
            )),
            &left_schema,
            &right_schema,
        );
        let left_values = left.column(0).clone();
        let right_values = right.column(0).clone();
        let outcomes = predicates
            .evaluate_candidates(
                &left,
                &right,
                &[0, 1],
                &[0, 1],
                Some(&left_values),
                Some(&right_values),
            )
            .unwrap();
        assert!(outcomes[0].qualifies);
        assert_eq!(outcomes[0].membership, Some(SqlTruth::True));
        assert!(!outcomes[1].qualifies);
        assert_eq!(outcomes[1].membership, None);
    }

    #[test]
    fn recognizes_cross_side_inequality_for_existence_summary() {
        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "left_value",
            DataType::Int64,
            true,
        )]));
        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "right_value",
            DataType::Int64,
            true,
        )]));
        let residual = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(BoundExpr::column(0, DataType::Int64, "left_value")),
                op: BinaryOp::NotEq,
                right: Box::new(BoundExpr::column(1, DataType::Int64, "right_value")),
            },
            data_type: DataType::Boolean,
            display_name: "left_value != right_value".into(),
        };
        let predicates = JoinPredicates::new(Some(residual), None, &left_schema, &right_schema);
        let value = predicates
            .existence_inequality_right_value(crate::sql::JoinType::Semi, 1)
            .expect("simple inequality should be summarized");
        assert!(matches!(value.kind, ExprKind::Column(0)));
        assert!(
            predicates
                .existence_inequality_right_value(crate::sql::JoinType::Left, 1)
                .is_none()
        );
        assert!(
            predicates
                .existence_inequality_right_value(crate::sql::JoinType::Mark, 1)
                .is_none(),
            "Mark must retain NULL candidates so it can return UNKNOWN"
        );
    }

    #[test]
    fn empty_candidate_batch_does_not_materialize_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let output = candidate_batch(&input, &input, &[], &[], schema, &[0]).unwrap();
        assert_eq!(output.num_rows(), 0);
        assert_eq!(output.num_columns(), 1);
    }

    #[test]
    fn residual_projection_rewrites_sparse_columns_and_skips_payload() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("left_key", DataType::Int64, true),
            Field::new("left_payload", DataType::Utf8, true),
            Field::new("left_value", DataType::Int64, true),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("right_key", DataType::Int64, true),
            Field::new("right_payload", DataType::Utf8, true),
            Field::new("right_value", DataType::Int64, true),
        ]));
        let left = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("left-a"), Some("left-b")])),
                Arc::new(Int64Array::from(vec![Some(10), Some(2)])),
            ],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("right-a"), Some("right-b")])),
                Arc::new(Int64Array::from(vec![Some(5), Some(3)])),
            ],
        )
        .unwrap();
        let residual = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(BoundExpr::column(2, DataType::Int64, "left_value")),
                op: BinaryOp::Gt,
                right: Box::new(BoundExpr::column(5, DataType::Int64, "right_value")),
            },
            data_type: DataType::Boolean,
            display_name: "left_value > right_value".into(),
        };
        let predicates = JoinPredicates::new(Some(residual), None, &left_schema, &right_schema);
        let outcomes = predicates
            .evaluate_candidates(&left, &right, &[0, 1], &[1, 0], None, None)
            .unwrap();
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| outcome.qualifies)
                .collect::<Vec<_>>(),
            vec![true, false]
        );

        let projected_schema = Arc::new(Schema::new(vec![
            Field::new("left_value", DataType::Int64, true),
            Field::new("right_value", DataType::Int64, true),
        ]));
        let projected =
            candidate_batch(&left, &right, &[0, 1], &[1, 0], projected_schema, &[2, 5]).unwrap();
        assert_eq!(projected.num_rows(), 2);
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.schema().field(0).name(), "left_value");
        assert_eq!(projected.schema().field(1).name(), "right_value");
        assert_eq!(
            projected
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10, 2]
        );
        assert_eq!(
            projected
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[3, 5]
        );
    }

    #[test]
    fn constant_residual_preserves_candidate_rows_without_columns() {
        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "left",
            DataType::Int64,
            false,
        )]));
        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "right",
            DataType::Int64,
            false,
        )]));
        let left = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![Arc::new(Int64Array::from(vec![4, 5]))],
        )
        .unwrap();
        let predicates = JoinPredicates::new(
            Some(BoundExpr::literal(ScalarValue::Boolean(true))),
            None,
            &left_schema,
            &right_schema,
        );
        let outcomes = predicates
            .evaluate_candidates(&left, &right, &[0, 1, 2], &[0, 1, 0], None, None)
            .unwrap();
        assert_eq!(outcomes.len(), 3);
        assert!(outcomes.iter().all(|outcome| outcome.qualifies));
    }
}

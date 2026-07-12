use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, UInt32Array},
    compute::take,
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::{Error, Result, sql::BoundExpr};

use super::{cell, evaluate};

#[derive(Clone)]
pub(super) struct JoinPredicates {
    residual: Option<BoundExpr>,
    null_aware: Option<(BoundExpr, BoundExpr)>,
    joined_schema: SchemaRef,
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
        Self {
            residual,
            null_aware,
            joined_schema: Arc::new(Schema::new(fields)),
        }
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
        let residual = match &self.residual {
            Some(predicate) => {
                let batch = candidate_batch(
                    left,
                    right,
                    left_indices,
                    right_indices,
                    Arc::clone(&self.joined_schema),
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
) -> Result<RecordBatch> {
    let left_indices = UInt32Array::from(left_indices.to_vec());
    let right_indices = UInt32Array::from(right_indices.to_vec());
    let mut columns = Vec::with_capacity(left.num_columns() + right.num_columns());
    for column in left.columns() {
        columns.push(take(column.as_ref(), &left_indices, None)?);
    }
    for column in right.columns() {
        columns.push(take(column.as_ref(), &right_indices, None)?);
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::{JoinPredicates, SqlTruth};
    use crate::sql::{BinaryOp, BoundExpr, ExprKind};

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
}

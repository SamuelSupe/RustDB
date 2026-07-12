use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, UInt64Array, new_null_array},
    compute::{
        filter_record_batch,
        kernels::{boolean, zip::zip},
        take,
    },
    record_batch::RecordBatch,
};

use crate::{
    Error, Result,
    sql::{BinaryOp, BoundExpr, ScalarFunction},
};

use super::{as_boolean, evaluate};

pub(super) fn boolean(
    op: BinaryOp,
    left: &BoundExpr,
    right: &BoundExpr,
    batch: &RecordBatch,
) -> Result<ArrayRef> {
    let left = evaluate(left, batch)?;
    let left_values = as_boolean(&left)?;
    let active = BooleanArray::from_iter((0..left_values.len()).map(|row| {
        let needed = if left_values.is_null(row) {
            true
        } else {
            match op {
                BinaryOp::And => left_values.value(row),
                BinaryOp::Or => !left_values.value(row),
                _ => unreachable!("caller passes only AND/OR"),
            }
        };
        Some(needed)
    }));
    let right = evaluate_masked(right, batch, &active)?;
    let right_values = as_boolean(&right)?;
    let result = match op {
        BinaryOp::And => boolean::and_kleene(left_values, right_values)?,
        BinaryOp::Or => boolean::or_kleene(left_values, right_values)?,
        _ => unreachable!("caller passes only AND/OR"),
    };
    Ok(Arc::new(result))
}

pub(super) fn null_function(
    function: ScalarFunction,
    args: &[BoundExpr],
    expression: &BoundExpr,
    batch: &RecordBatch,
) -> Result<ArrayRef> {
    match function {
        ScalarFunction::Coalesce => coalesce(args, expression, batch),
        ScalarFunction::NullIf => nullif(args, batch),
        _ => Err(Error::Internal(format!(
            "{function} is not a short-circuit NULL function"
        ))),
    }
}

pub(super) fn case(
    when_then: &[(BoundExpr, BoundExpr)],
    else_expr: &BoundExpr,
    expression: &BoundExpr,
    batch: &RecordBatch,
) -> Result<ArrayRef> {
    let mut remaining = all_rows(batch.num_rows());
    let mut output = new_null_array(&expression.data_type, batch.num_rows());
    for (condition, result) in when_then {
        if !any_active(&remaining) {
            break;
        }
        let condition = evaluate_masked(condition, batch, &remaining)?;
        let condition = as_boolean(&condition)?;
        let selected = BooleanArray::from_iter((0..batch.num_rows()).map(|row| {
            Some(remaining.value(row) && condition.is_valid(row) && condition.value(row))
        }));
        if any_active(&selected) {
            let result = evaluate_masked(result, batch, &selected)?;
            output = zip(&selected, &result, &output)?;
        }
        remaining = and_not(&remaining, &selected);
    }
    if any_active(&remaining) {
        let fallback = evaluate_masked(else_expr, batch, &remaining)?;
        output = zip(&remaining, &fallback, &output)?;
    }
    Ok(output)
}

fn coalesce(args: &[BoundExpr], expression: &BoundExpr, batch: &RecordBatch) -> Result<ArrayRef> {
    let mut unresolved = all_rows(batch.num_rows());
    let mut output = new_null_array(&expression.data_type, batch.num_rows());
    for arg in args {
        if !any_active(&unresolved) {
            break;
        }
        let value = evaluate_masked(arg, batch, &unresolved)?;
        let selected = BooleanArray::from_iter(
            (0..batch.num_rows()).map(|row| Some(unresolved.value(row) && value.is_valid(row))),
        );
        output = zip(&selected, &value, &output)?;
        unresolved = and_not(&unresolved, &selected);
    }
    Ok(output)
}

fn nullif(args: &[BoundExpr], batch: &RecordBatch) -> Result<ArrayRef> {
    let [original, comparison_left, comparison_right] = args else {
        return Err(Error::Internal(
            "nullif execution requires three internal arguments".into(),
        ));
    };
    let original = evaluate(original, batch)?;
    let active =
        BooleanArray::from_iter((0..original.len()).map(|row| Some(original.is_valid(row))));
    let comparison_left = evaluate_masked(comparison_left, batch, &active)?;
    let comparison_right = evaluate_masked(comparison_right, batch, &active)?;
    super::super::functions::evaluate(
        ScalarFunction::NullIf,
        &[original, comparison_left, comparison_right],
        &args[0].data_type,
    )
}

fn evaluate_masked(
    expression: &BoundExpr,
    batch: &RecordBatch,
    active: &BooleanArray,
) -> Result<ArrayRef> {
    if active.len() != batch.num_rows() {
        return Err(Error::Internal(
            "short-circuit mask length does not match the input batch".into(),
        ));
    }
    let selected = active.iter().filter(|value| *value == Some(true)).count();
    if selected == 0 {
        return Ok(new_null_array(&expression.data_type, batch.num_rows()));
    }
    if selected == batch.num_rows() {
        return evaluate(expression, batch);
    }

    let compact_batch = filter_record_batch(batch, active)?;
    let compact = evaluate(expression, &compact_batch)?;
    let mut compact_row = 0_u64;
    let indices = UInt64Array::from_iter(active.iter().map(|value| {
        if value == Some(true) {
            let index = compact_row;
            compact_row = compact_row.saturating_add(1);
            Some(index)
        } else {
            None
        }
    }));
    Ok(take(compact.as_ref(), &indices, None)?)
}

fn all_rows(len: usize) -> BooleanArray {
    BooleanArray::from(vec![true; len])
}

fn any_active(mask: &BooleanArray) -> bool {
    mask.values().count_set_bits() != 0
}

fn and_not(left: &BooleanArray, right: &BooleanArray) -> BooleanArray {
    BooleanArray::from_iter((0..left.len()).map(|row| Some(left.value(row) && !right.value(row))))
}

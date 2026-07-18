mod numeric;
mod string;
mod temporal;
mod timezone;
mod uuid;

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, new_null_array},
    compute::{
        cast,
        kernels::{cmp, zip::zip},
    },
    datatypes::{DataType, TimeUnit},
};

use crate::sql::ScalarFunction;
use crate::{Error, Result};

pub(super) fn evaluate(
    function: ScalarFunction,
    args: &[ArrayRef],
    output_type: &DataType,
) -> Result<ArrayRef> {
    match function {
        ScalarFunction::Substring
        | ScalarFunction::Length
        | ScalarFunction::Lower
        | ScalarFunction::Upper
        | ScalarFunction::Trim
        | ScalarFunction::LTrim
        | ScalarFunction::RTrim
        | ScalarFunction::Concat
        | ScalarFunction::Replace
        | ScalarFunction::RegexpReplace
        | ScalarFunction::StartsWith
        | ScalarFunction::EndsWith
        | ScalarFunction::Contains => string::evaluate(function, args),
        ScalarFunction::Coalesce => coalesce(args),
        ScalarFunction::NullIf => nullif(args),
        ScalarFunction::Abs
        | ScalarFunction::Ceil
        | ScalarFunction::Floor
        | ScalarFunction::Round => numeric::evaluate(function, args, output_type),
        ScalarFunction::DatePart(part) => temporal::date_part(part, &args[0]),
        ScalarFunction::DateTrunc(part) => temporal::date_trunc(part, &args[0]),
        ScalarFunction::ToTimestampSeconds => temporal::to_timestamp_seconds(&args[0]),
        ScalarFunction::DistinctTuple => distinct_tuple(args),
        ScalarFunction::AtTimeZone { timezone, attach } => {
            timezone::at_time_zone(timezone, attach, &args[0], output_type)
        }
    }
}

fn distinct_tuple(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 {
        return Err(Error::Internal(
            "DISTINCT tuple execution requires at least two columns".into(),
        ));
    }
    let normalized = args
        .iter()
        .cloned()
        .map(super::value::canonicalize_sort_key)
        .collect::<Result<Vec<_>>>()?;
    let fields = normalized
        .iter()
        .map(|array| arrow::row::SortField::new(array.data_type().clone()))
        .collect();
    let converter = arrow::row::RowConverter::new(fields)?;
    let rows = converter.convert_columns(&normalized)?;
    Ok(Arc::new(arrow::array::BinaryArray::from_iter(
        (0..rows.num_rows()).map(|row| {
            normalized
                .iter()
                .all(|array| array.is_valid(row))
                .then(|| rows.row(row).data())
        }),
    )))
}

pub(super) fn cast_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let output = match (array.data_type(), target) {
        (
            DataType::LargeUtf8,
            DataType::Date32
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::FixedSizeBinary(16),
        ) => {
            let utf8 = cast(array.as_ref(), &DataType::Utf8)?;
            cast_array(&utf8, target)
        }
        (DataType::Utf8, DataType::Date32) => temporal::string_to_date(array),
        (source, DataType::Date32) if temporal::is_integer(source) => {
            temporal::integer_to_date(array)
        }
        (DataType::Utf8, DataType::Timestamp(_, Some(_))) => {
            timezone::string_to_timestamp(array, target)
        }
        (DataType::Utf8, DataType::Timestamp(_, None)) => {
            temporal::string_to_timestamp(array, target)
        }
        (DataType::Utf8, DataType::Time32(_) | DataType::Time64(_)) => {
            temporal::string_to_time(array, target)
        }
        (DataType::Time32(_) | DataType::Time64(_), DataType::Utf8) => {
            temporal::time_to_string(array)
        }
        (DataType::Utf8, DataType::FixedSizeBinary(16)) => uuid::string_to_uuid(array),
        (DataType::FixedSizeBinary(16), DataType::Utf8) => uuid::uuid_to_string(array),
        (DataType::Date32, DataType::Utf8) => temporal::date_to_string(array),
        (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, None)) => {
            temporal::date_to_timestamp(array)
        }
        (DataType::Timestamp(_, None), DataType::Date32) => temporal::timestamp_to_date(array),
        (DataType::Timestamp(_, None), DataType::Utf8) => temporal::timestamp_to_string(array),
        (DataType::Timestamp(_, Some(_)), DataType::Utf8) => timezone::timestamp_to_string(array),
        _ => Ok(cast(array.as_ref(), target)?),
    }?;
    reject_new_cast_nulls(array, &output, target)?;
    Ok(output)
}

fn reject_new_cast_nulls(input: &ArrayRef, output: &ArrayRef, target: &DataType) -> Result<()> {
    if input.data_type() != &DataType::Null
        && let Some(row) = (0..input.len()).find(|row| input.is_valid(*row) && output.is_null(*row))
    {
        return Err(Error::Execution(format!(
            "strict CAST from {} to {target} failed at row {row}: value is out of range or invalid",
            input.data_type(),
        )));
    }
    Ok(())
}

fn coalesce(args: &[ArrayRef]) -> Result<ArrayRef> {
    let first = args
        .first()
        .ok_or_else(|| Error::Internal("coalesce execution has no arguments".into()))?;
    let mut output = new_null_array(first.data_type(), first.len());
    for arg in args.iter().rev() {
        let mask = arrow::compute::kernels::boolean::is_not_null(arg.as_ref())?;
        output = zip(&mask, arg, &output)?;
    }
    Ok(output)
}

fn nullif(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [output, left, right] = args else {
        return Err(Error::Internal(
            "nullif execution requires original, comparison-left, and comparison-right arguments"
                .into(),
        ));
    };
    let equal = cmp::eq(left, right)?;
    let mask = BooleanArray::from_iter(
        (0..equal.len()).map(|row| Some(equal.is_valid(row) && equal.value(row))),
    );
    let nulls = new_null_array(output.data_type(), output.len());
    Ok(zip(&mask, &nulls, output)?)
}

pub(super) fn timestamp_literal(value: i64, len: usize) -> ArrayRef {
    Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
        Some(
            value
        );
        len
    ]))
}

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};

use crate::sql::ScalarFunction;
use crate::{Error, Result};

pub(super) fn evaluate(function: ScalarFunction, args: &[ArrayRef]) -> Result<ArrayRef> {
    match function {
        ScalarFunction::Substring => substring(args),
        ScalarFunction::Length => unary_string(args, |value| {
            i64::try_from(value.chars().count())
                .map_err(|_| Error::Execution("string length exceeds Int64".into()))
        }),
        ScalarFunction::Lower => unary_string(args, |value| Ok(value.to_lowercase())),
        ScalarFunction::Upper => unary_string(args, |value| Ok(value.to_uppercase())),
        ScalarFunction::Trim | ScalarFunction::LTrim | ScalarFunction::RTrim => {
            trim(function, args)
        }
        ScalarFunction::Concat => concat(args),
        ScalarFunction::Replace => ternary_string(args, |value, from, to| value.replace(from, to)),
        ScalarFunction::StartsWith => {
            binary_predicate(args, |value, pattern| value.starts_with(pattern))
        }
        ScalarFunction::EndsWith => {
            binary_predicate(args, |value, pattern| value.ends_with(pattern))
        }
        ScalarFunction::Contains => {
            binary_predicate(args, |value, pattern| value.contains(pattern))
        }
        _ => Err(Error::Internal(format!(
            "{function} is not a string function"
        ))),
    }
}

fn substring(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = strings(&args[0])?;
    let starts = integers(&args[1])?;
    let lengths = args.get(2).map(integers).transpose()?;
    let mut output = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row)
            || starts.is_null(row)
            || lengths.is_some_and(|lengths| lengths.is_null(row))
        {
            output.push(None);
            continue;
        }
        let value = values.value(row);
        let character_count = i64::try_from(value.chars().count())
            .map_err(|_| Error::Execution("substring input is too long".into()))?;
        let start = starts.value(row);
        let logical_start = if start < 0 {
            character_count.saturating_add(start).saturating_add(1)
        } else {
            start
        };
        let length = lengths.map(|lengths| lengths.value(row));
        let length_value = length.unwrap_or(character_count);
        if length_value < 0 {
            return Err(Error::Execution(format!(
                "substring length is negative at row {row}"
            )));
        }
        let logical_end = length.map_or(character_count.saturating_add(1), |length| {
            logical_start.saturating_add(length)
        });
        let visible_start = logical_start.max(1).min(character_count.saturating_add(1));
        let visible_end = logical_end.max(1).min(character_count.saturating_add(1));
        let offset = usize::try_from(visible_start.saturating_sub(1)).unwrap_or(usize::MAX);
        let length = usize::try_from(visible_end.saturating_sub(visible_start)).unwrap_or(0);
        output.push(Some(
            value.chars().skip(offset).take(length).collect::<String>(),
        ));
    }
    Ok(string_array(output))
}

fn trim(function: ScalarFunction, args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = strings(&args[0])?;
    let custom = args.get(1).map(strings).transpose()?;
    let mut output = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) || custom.is_some_and(|custom| custom.is_null(row)) {
            output.push(None);
            continue;
        }
        let value = values.value(row);
        let result = if let Some(custom) = custom {
            let characters = custom.value(row).chars().collect::<Vec<_>>();
            match function {
                ScalarFunction::Trim => value.trim_matches(characters.as_slice()),
                ScalarFunction::LTrim => value.trim_start_matches(characters.as_slice()),
                ScalarFunction::RTrim => value.trim_end_matches(characters.as_slice()),
                _ => unreachable!(),
            }
        } else {
            match function {
                // SQL TRIM's default character is U+0020. Rust's `trim*`
                // helpers remove all Unicode whitespace and would therefore
                // diverge for tabs and newlines.
                ScalarFunction::Trim => value.trim_matches(' '),
                ScalarFunction::LTrim => value.trim_start_matches(' '),
                ScalarFunction::RTrim => value.trim_end_matches(' '),
                _ => unreachable!(),
            }
        };
        output.push(Some(result.to_owned()));
    }
    Ok(string_array(output))
}

fn concat(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args = args.iter().map(strings).collect::<Result<Vec<_>>>()?;
    let len = args[0].len();
    let mut output = Vec::with_capacity(len);
    for row in 0..len {
        let capacity = args
            .iter()
            .filter(|arg| arg.is_valid(row))
            .map(|arg| arg.value(row).len())
            .sum();
        let mut value = String::with_capacity(capacity);
        for arg in &args {
            if arg.is_valid(row) {
                value.push_str(arg.value(row));
            }
        }
        output.push(Some(value));
    }
    Ok(string_array(output))
}

fn unary_string<T, F>(args: &[ArrayRef], mut operation: F) -> Result<ArrayRef>
where
    T: UnaryOutput,
    F: FnMut(&str) -> Result<T>,
{
    let values = strings(&args[0])?;
    let output = (0..values.len())
        .map(|row| {
            values
                .is_valid(row)
                .then(|| operation(values.value(row)))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(T::array(output))
}

trait UnaryOutput: Sized {
    fn array(values: Vec<Option<Self>>) -> ArrayRef;
}

impl UnaryOutput for String {
    fn array(values: Vec<Option<Self>>) -> ArrayRef {
        string_array(values)
    }
}

impl UnaryOutput for i64 {
    fn array(values: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }
}

fn ternary_string<F>(args: &[ArrayRef], mut operation: F) -> Result<ArrayRef>
where
    F: FnMut(&str, &str, &str) -> String,
{
    let first = strings(&args[0])?;
    let second = strings(&args[1])?;
    let third = strings(&args[2])?;
    let output = (0..first.len())
        .map(|row| {
            (first.is_valid(row) && second.is_valid(row) && third.is_valid(row))
                .then(|| operation(first.value(row), second.value(row), third.value(row)))
        })
        .collect();
    Ok(string_array(output))
}

fn binary_predicate<F>(args: &[ArrayRef], predicate: F) -> Result<ArrayRef>
where
    F: Fn(&str, &str) -> bool,
{
    let left = strings(&args[0])?;
    let right = strings(&args[1])?;
    Ok(Arc::new(BooleanArray::from_iter((0..left.len()).map(
        |row| {
            (left.is_valid(row) && right.is_valid(row))
                .then(|| predicate(left.value(row), right.value(row)))
        },
    ))))
}

fn strings(array: &ArrayRef) -> Result<&StringArray> {
    array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        Error::Internal(format!(
            "expected Utf8 function argument, got {}",
            array.data_type()
        ))
    })
}

fn integers(array: &ArrayRef) -> Result<&Int64Array> {
    array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        Error::Internal(format!(
            "expected Int64 function argument, got {}",
            array.data_type()
        ))
    })
}

fn string_array(values: Vec<Option<String>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(
        values.iter().map(|value| value.as_deref()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_trim_removes_spaces_but_preserves_other_whitespace() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![" \tvalue\t "]));
        let output = evaluate(ScalarFunction::Trim, &[input]).unwrap();
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "\tvalue\t");
    }
}

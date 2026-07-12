use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, UInt64Array};
use arrow::datatypes::DataType;

use crate::sql::ScalarFunction;
use crate::{Error, Result};

pub(super) fn evaluate(
    function: ScalarFunction,
    args: &[ArrayRef],
    output_type: &DataType,
) -> Result<ArrayRef> {
    let scale = scale_argument(args.get(1))?;
    match args[0].data_type() {
        DataType::Int64 => integer(function, &args[0], scale),
        DataType::UInt64 => unsigned(function, &args[0], scale),
        DataType::Float64 => float(function, &args[0], scale),
        DataType::Decimal128(_, decimal_scale) => match output_type {
            DataType::Decimal128(output_precision, output_scale) => decimal(
                function,
                &args[0],
                scale,
                *decimal_scale,
                *output_precision,
                *output_scale,
            ),
            other => Err(Error::Internal(format!(
                "{function} DECIMAL output has non-DECIMAL type {other}"
            ))),
        },
        other => Err(Error::Internal(format!(
            "{function} received non-canonical numeric type {other}"
        ))),
    }
}

fn integer(
    function: ScalarFunction,
    array: &ArrayRef,
    scale: Option<&Int64Array>,
) -> Result<ArrayRef> {
    let values = downcast::<Int64Array>(array, "Int64")?;
    let output = (0..values.len())
        .map(|row| {
            if values.is_null(row) || scale.is_some_and(|scale| scale.is_null(row)) {
                return Ok(None);
            }
            let value = values.value(row);
            let digits = scale.map_or(0, |scale| scale.value(row));
            let value = match function {
                ScalarFunction::Abs => value
                    .checked_abs()
                    .ok_or_else(|| Error::Execution(format!("abs overflows Int64 at row {row}")))?,
                ScalarFunction::Round | ScalarFunction::Ceil | ScalarFunction::Floor => {
                    integer_round(value, digits, function)?
                }
                _ => unreachable!(),
            };
            Ok(Some(value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Int64Array::from(output)))
}

fn unsigned(
    function: ScalarFunction,
    array: &ArrayRef,
    scale: Option<&Int64Array>,
) -> Result<ArrayRef> {
    let values = downcast::<UInt64Array>(array, "UInt64")?;
    let output = (0..values.len())
        .map(|row| {
            if values.is_null(row) || scale.is_some_and(|scale| scale.is_null(row)) {
                return Ok(None);
            }
            let value = values.value(row);
            let digits = scale.map_or(0, |scale| scale.value(row));
            let value = match function {
                ScalarFunction::Abs => value,
                ScalarFunction::Round | ScalarFunction::Ceil | ScalarFunction::Floor => {
                    unsigned_round(value, digits, function)?
                }
                _ => unreachable!(),
            };
            Ok(Some(value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(UInt64Array::from(output)))
}

fn float(
    function: ScalarFunction,
    array: &ArrayRef,
    scale: Option<&Int64Array>,
) -> Result<ArrayRef> {
    let values = downcast::<Float64Array>(array, "Float64")?;
    let output = (0..values.len())
        .map(|row| {
            if values.is_null(row) || scale.is_some_and(|scale| scale.is_null(row)) {
                return Ok(None);
            }
            let value = values.value(row);
            let digits = scale.map_or(0, |scale| scale.value(row));
            let value = match function {
                ScalarFunction::Abs => value.abs(),
                ScalarFunction::Round => scaled_float(value, digits, f64::round)?,
                ScalarFunction::Ceil => scaled_float(value, digits, f64::ceil)?,
                ScalarFunction::Floor => scaled_float(value, digits, f64::floor)?,
                _ => unreachable!(),
            };
            if value.is_finite() || values.value(row).is_infinite() {
                Ok(Some(value))
            } else {
                Err(Error::Execution(format!(
                    "{function} produced a non-finite result at row {row}"
                )))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Float64Array::from(output)))
}

fn decimal(
    function: ScalarFunction,
    array: &ArrayRef,
    scale: Option<&Int64Array>,
    decimal_scale: i8,
    output_precision: u8,
    output_scale: i8,
) -> Result<ArrayRef> {
    let values = downcast::<Decimal128Array>(array, "Decimal128")?;
    let output = (0..values.len())
        .map(|row| {
            if values.is_null(row) || scale.is_some_and(|scale| scale.is_null(row)) {
                return Ok(None);
            }
            let value = values.value(row);
            let digits = scale.map_or(0, |scale| scale.value(row));
            let value = match function {
                ScalarFunction::Abs => value.checked_abs().ok_or_else(|| {
                    Error::Execution(format!("abs overflows Decimal128 at row {row}"))
                })?,
                ScalarFunction::Round | ScalarFunction::Ceil | ScalarFunction::Floor => {
                    decimal_round(value, decimal_scale, digits, function)?
                }
                _ => unreachable!(),
            };
            let value = rescale_decimal(value, decimal_scale, output_scale)?;
            validate_decimal_precision(value, output_precision, row)?;
            Ok(Some(value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(
        Decimal128Array::from(output).with_precision_and_scale(output_precision, output_scale)?,
    ))
}

fn rescale_decimal(value: i128, source_scale: i8, output_scale: i8) -> Result<i128> {
    let difference = i16::from(source_scale) - i16::from(output_scale);
    if difference == 0 {
        return Ok(value);
    }
    if difference > 0 {
        let divisor = 10_i128.checked_pow(difference as u32).ok_or_else(|| {
            Error::Execution("decimal function result scale is out of range".into())
        })?;
        if value % divisor != 0 {
            return Err(Error::Internal(
                "rounded DECIMAL result cannot be represented at its output scale".into(),
            ));
        }
        Ok(value / divisor)
    } else {
        let multiplier = 10_i128.checked_pow((-difference) as u32).ok_or_else(|| {
            Error::Execution("decimal function result scale is out of range".into())
        })?;
        value
            .checked_mul(multiplier)
            .ok_or_else(|| Error::Execution("decimal function result overflowed Decimal128".into()))
    }
}

fn validate_decimal_precision(value: i128, precision: u8, row: usize) -> Result<()> {
    let limit = 10_i128
        .checked_pow(u32::from(precision))
        .ok_or_else(|| Error::Execution("DECIMAL precision is out of range".into()))?;
    if value.unsigned_abs() >= limit as u128 {
        return Err(Error::Execution(format!(
            "decimal function result exceeds precision {precision} at row {row}"
        )));
    }
    Ok(())
}

fn scaled_float(value: f64, digits: i64, operation: fn(f64) -> f64) -> Result<f64> {
    let digits = i32::try_from(digits).map_err(|_| {
        Error::Execution("numeric rounding scale is outside the supported range".into())
    })?;
    let factor = 10_f64.powi(digits);
    if !factor.is_finite() || factor == 0.0 {
        return Err(Error::Execution(format!(
            "numeric rounding scale {digits} is outside the supported range"
        )));
    }
    Ok(operation(value * factor) / factor)
}

fn integer_round(value: i64, digits: i64, function: ScalarFunction) -> Result<i64> {
    if digits >= 0 || function == ScalarFunction::Abs {
        return Ok(value);
    }
    let power = u32::try_from(-digits).unwrap_or(u32::MAX);
    let step = 10_i64.checked_pow(power).ok_or_else(|| {
        Error::Execution(format!("numeric rounding scale {digits} is out of range"))
    })?;
    signed_step_round(value, step, function)
}

fn unsigned_round(value: u64, digits: i64, function: ScalarFunction) -> Result<u64> {
    if digits >= 0 || function == ScalarFunction::Abs {
        return Ok(value);
    }
    let power = u32::try_from(-digits).unwrap_or(u32::MAX);
    let step = 10_u64.checked_pow(power).ok_or_else(|| {
        Error::Execution(format!("numeric rounding scale {digits} is out of range"))
    })?;
    let quotient = value / step;
    let remainder = value % step;
    let quotient = match function {
        ScalarFunction::Round if remainder >= step - remainder => quotient.checked_add(1),
        ScalarFunction::Ceil if remainder != 0 => quotient.checked_add(1),
        ScalarFunction::Round | ScalarFunction::Ceil | ScalarFunction::Floor => Some(quotient),
        _ => unreachable!(),
    }
    .ok_or_else(|| Error::Execution("numeric rounding overflow".into()))?;
    quotient
        .checked_mul(step)
        .ok_or_else(|| Error::Execution("numeric rounding overflow".into()))
}

fn decimal_round(
    value: i128,
    source_scale: i8,
    digits: i64,
    function: ScalarFunction,
) -> Result<i128> {
    let exponent = i64::from(source_scale) - digits;
    if exponent <= 0 || function == ScalarFunction::Abs {
        return Ok(value);
    }
    let power = u32::try_from(exponent).map_err(|_| {
        Error::Execution(format!("decimal rounding scale {digits} is out of range"))
    })?;
    let step = 10_i128.checked_pow(power).ok_or_else(|| {
        Error::Execution(format!("decimal rounding scale {digits} is out of range"))
    })?;
    signed_step_round(value, step, function)
}

fn signed_step_round<T>(value: T, step: T, function: ScalarFunction) -> Result<T>
where
    T: SignedStep,
{
    match function {
        ScalarFunction::Floor => value.floor_step(step),
        ScalarFunction::Ceil => value.ceil_step(step),
        ScalarFunction::Round => value.round_step(step),
        _ => unreachable!(),
    }
}

trait SignedStep: Copy {
    fn floor_step(self, step: Self) -> Result<Self>;
    fn ceil_step(self, step: Self) -> Result<Self>;
    fn round_step(self, step: Self) -> Result<Self>;
}

macro_rules! signed_step {
    ($type:ty) => {
        impl SignedStep for $type {
            fn floor_step(self, step: Self) -> Result<Self> {
                self.div_euclid(step)
                    .checked_mul(step)
                    .ok_or_else(|| Error::Execution("numeric floor overflow".into()))
            }

            fn ceil_step(self, step: Self) -> Result<Self> {
                let quotient = self.div_euclid(step);
                let quotient = if self.rem_euclid(step) == 0 {
                    quotient
                } else {
                    quotient
                        .checked_add(1)
                        .ok_or_else(|| Error::Execution("numeric ceil overflow".into()))?
                };
                quotient
                    .checked_mul(step)
                    .ok_or_else(|| Error::Execution("numeric ceil overflow".into()))
            }

            fn round_step(self, step: Self) -> Result<Self> {
                let negative = self.is_negative();
                let magnitude = self.unsigned_abs() as u128;
                let step_magnitude = step as u128;
                let mut quotient = magnitude / step_magnitude;
                let remainder = magnitude % step_magnitude;
                if remainder >= step_magnitude - remainder {
                    quotient = quotient
                        .checked_add(1)
                        .ok_or_else(|| Error::Execution("numeric round overflow".into()))?;
                }
                let rounded = quotient
                    .checked_mul(step_magnitude)
                    .ok_or_else(|| Error::Execution("numeric round overflow".into()))?;
                let rounded = <$type>::try_from(rounded)
                    .map_err(|_| Error::Execution("numeric round overflow".into()))?;
                if negative {
                    rounded
                        .checked_neg()
                        .ok_or_else(|| Error::Execution("numeric round overflow".into()))
                } else {
                    Ok(rounded)
                }
            }
        }
    };
}

signed_step!(i64);
signed_step!(i128);

fn scale_argument(array: Option<&ArrayRef>) -> Result<Option<&Int64Array>> {
    array
        .map(|array| downcast::<Int64Array>(array, "Int64"))
        .transpose()
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, name: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::Internal(format!(
            "expected {name} function argument, got {}",
            array.data_type()
        ))
    })
}

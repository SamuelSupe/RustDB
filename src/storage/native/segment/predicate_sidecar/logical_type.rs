use arrow::datatypes::DataType;

use super::{PredicateSidecarError, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PredicateType {
    Int8,
    Int16,
    Int32,
    Int64,
    Date32,
    Decimal128 { precision: u8, scale: i8 },
}

impl PredicateType {
    pub(crate) fn from_arrow(data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Int8 => Ok(Self::Int8),
            DataType::Int16 => Ok(Self::Int16),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            DataType::Date32 => Ok(Self::Date32),
            DataType::Decimal128(precision, scale) if *precision <= 18 => Ok(Self::Decimal128 {
                precision: *precision,
                scale: *scale,
            }),
            _ => Err(PredicateSidecarError::UnsupportedType(
                data_type.to_string(),
            )),
        }
    }

    #[cfg(test)]
    pub(super) fn raw_width(self) -> usize {
        match self {
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int32 | Self::Date32 => 4,
            Self::Int64 => 8,
            Self::Decimal128 { .. } => 16,
        }
    }

    pub(super) fn validate(self) -> Result<()> {
        match self {
            Self::Decimal128 { precision, .. } if !(1..=18).contains(&precision) => Err(
                PredicateSidecarError::UnsupportedType(format!("Decimal128({precision}, ..)")),
            ),
            _ => Ok(()),
        }
    }

    pub(super) fn validate_value(self, value: i64) -> Result<()> {
        let valid = match self {
            Self::Int8 => i8::try_from(value).is_ok(),
            Self::Int16 => i16::try_from(value).is_ok(),
            Self::Int32 | Self::Date32 => i32::try_from(value).is_ok(),
            Self::Int64 => true,
            Self::Decimal128 { precision, .. } => {
                let limit = 10_i64.pow(u32::from(precision));
                value > -limit && value < limit
            }
        };
        if valid {
            Ok(())
        } else {
            Err(PredicateSidecarError::ValueOutOfRange {
                data_type: self,
                value,
            })
        }
    }

    #[cfg(test)]
    pub(super) fn format_parts(self) -> (u8, u8, i8) {
        match self {
            Self::Int8 => (1, 0, 0),
            Self::Int16 => (2, 0, 0),
            Self::Int32 => (3, 0, 0),
            Self::Int64 => (4, 0, 0),
            Self::Date32 => (5, 0, 0),
            Self::Decimal128 { precision, scale } => (6, precision, scale),
        }
    }

    pub(super) fn from_format(tag: u8, precision: u8, scale: i8) -> Result<Self> {
        if tag != 6 && (precision != 0 || scale != 0) {
            return Err(PredicateSidecarError::Corrupt(
                "non-decimal type has decimal metadata".to_owned(),
            ));
        }
        let data_type = match tag {
            1 => Self::Int8,
            2 => Self::Int16,
            3 => Self::Int32,
            4 => Self::Int64,
            5 => Self::Date32,
            6 => Self::Decimal128 { precision, scale },
            _ => {
                return Err(PredicateSidecarError::Corrupt(format!(
                    "unknown logical type tag {tag}"
                )));
            }
        };
        data_type.validate()?;
        Ok(data_type)
    }
}

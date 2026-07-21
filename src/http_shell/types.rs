use std::{str::FromStr, time::Duration};

use arrow::datatypes::{DataType, TimeUnit};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use crate::{Error, ParameterValue, Result};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<TypedParameter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl QueryRequest {
    pub(crate) fn timeout(&self, maximum: Duration) -> Result<Duration> {
        let requested = self
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(maximum);
        if requested.is_zero() || requested > maximum {
            return Err(Error::InvalidArgument(format!(
                "timeout_ms must be between 1 and {}",
                maximum.as_millis()
            )));
        }
        Ok(requested)
    }

    pub(crate) fn parameter_values(&self) -> Result<Vec<ParameterValue>> {
        self.parameters
            .iter()
            .map(TypedParameter::to_value)
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypedParameter {
    #[serde(rename = "type")]
    pub data_type: String,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<i8>,
}

impl TypedParameter {
    pub(crate) fn to_value(&self) -> Result<ParameterValue> {
        let kind = self.data_type.trim().to_ascii_lowercase();
        if self.value.is_null() {
            return Ok(ParameterValue::Null(parameter_data_type(
                &kind,
                self.precision,
                self.scale,
            )?));
        }
        let invalid = || {
            Error::InvalidArgument(format!(
                "parameter value does not match declared type '{}'",
                self.data_type
            ))
        };
        match kind.as_str() {
            "boolean" | "bool" => self
                .value
                .as_bool()
                .map(ParameterValue::Boolean)
                .ok_or_else(invalid),
            "int64" | "bigint" => self
                .value
                .as_i64()
                .map(ParameterValue::Int64)
                .ok_or_else(invalid),
            "uint64" | "ubigint" => self
                .value
                .as_u64()
                .map(ParameterValue::UInt64)
                .ok_or_else(invalid),
            "float64" | "double" => parse_float(&self.value)
                .map(ParameterValue::Float64)
                .ok_or_else(invalid),
            "decimal128" | "decimal" => {
                let precision = self.precision.ok_or_else(|| {
                    Error::InvalidArgument("decimal parameter requires precision".into())
                })?;
                let scale = self.scale.ok_or_else(|| {
                    Error::InvalidArgument("decimal parameter requires scale".into())
                })?;
                let value = parse_decimal(&self.value, precision, scale).ok_or_else(invalid)?;
                Ok(ParameterValue::Decimal128 {
                    value,
                    precision,
                    scale,
                })
            }
            "utf8" | "string" => self
                .value
                .as_str()
                .map(|value| ParameterValue::Utf8(value.to_owned()))
                .ok_or_else(invalid),
            "binary" => self.value.as_str().ok_or_else(invalid).and_then(|value| {
                STANDARD
                    .decode(value)
                    .map(ParameterValue::Binary)
                    .map_err(|_| invalid())
            }),
            "date32" | "date" => as_i32(&self.value)
                .map(ParameterValue::Date32)
                .ok_or_else(invalid),
            "timestamp_microsecond" | "timestamp_us" | "timestamp" => self
                .value
                .as_i64()
                .map(ParameterValue::TimestampMicrosecond)
                .ok_or_else(invalid),
            _ => Err(Error::InvalidArgument(format!(
                "unsupported parameter type '{}'",
                self.data_type
            ))),
        }
    }
}

fn parameter_data_type(kind: &str, precision: Option<u8>, scale: Option<i8>) -> Result<DataType> {
    Ok(match kind {
        "boolean" | "bool" => DataType::Boolean,
        "int64" | "bigint" => DataType::Int64,
        "uint64" | "ubigint" => DataType::UInt64,
        "float64" | "double" => DataType::Float64,
        "decimal128" | "decimal" => DataType::Decimal128(
            precision
                .ok_or_else(|| Error::InvalidArgument("decimal NULL requires precision".into()))?,
            scale.ok_or_else(|| Error::InvalidArgument("decimal NULL requires scale".into()))?,
        ),
        "utf8" | "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date32" | "date" => DataType::Date32,
        "timestamp_microsecond" | "timestamp_us" | "timestamp" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        _ => {
            return Err(Error::InvalidArgument(format!(
                "unsupported parameter type '{kind}'"
            )));
        }
    })
}

fn parse_float(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| match value.as_str()? {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    })
}

fn parse_decimal(value: &Value, precision: u8, scale: i8) -> Option<i128> {
    if precision == 0 || precision > 38 || scale < 0 || scale as u8 > precision {
        return None;
    }
    let text = match value {
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        _ => return None,
    };
    let (negative, text) = if let Some(text) = text.strip_prefix('-') {
        (true, text)
    } else if let Some(text) = text.strip_prefix('+') {
        (false, text)
    } else {
        (false, text.as_str())
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if text.is_empty()
        || fraction.contains('.')
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
        || whole.is_empty() && fraction.is_empty()
        || fraction.len() > usize::try_from(scale).ok()?
    {
        return None;
    }
    let mut digits = String::with_capacity(whole.len() + usize::try_from(scale).ok()?);
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n(
        '0',
        usize::try_from(scale).ok()? - fraction.len(),
    ));
    let value = i128::from_str(&digits).ok()?;
    if value >= 10_i128.checked_pow(u32::from(precision))? {
        return None;
    }
    Some(if negative { -value } else { value })
}

fn as_i32(value: &Value) -> Option<i32> {
    i32::try_from(value.as_i64()?).ok()
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum QueryState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    /// Execution stopped because the server exited before the Query reached a
    /// normal terminal state. Committed result batches may still be readable.
    Interrupted,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct SubmitResponse {
    pub query_id: String,
    pub state: QueryState,
    pub replayed: bool,
    pub status_url: String,
    pub results_url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct QueryStatusResponse {
    pub query_id: String,
    pub state: QueryState,
    pub created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<crate::http_shell::error::ErrorBody>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<HttpQueryMetrics>,
    pub result_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_expires_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_batches: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[non_exhaustive]
pub struct QueryListRequest {
    pub state: Option<QueryState>,
    pub created_after_ms: Option<u64>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct QueryListResponse {
    pub queries: Vec<QueryStatusResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[non_exhaustive]
pub struct HttpQueryMetrics {
    pub elapsed_ms: u64,
    pub rows_returned: u64,
    pub rows_scanned: u64,
    pub bytes_scanned: u64,
    pub peak_memory_bytes: u64,
    pub spill_read_bytes: u64,
    pub spill_write_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct PageMetadata {
    pub offset: u64,
    pub row_count: usize,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct JsonResultPage {
    pub schema: Vec<SchemaColumn>,
    pub rows: Vec<Vec<Value>>,
    pub page: PageMetadata,
}

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct InfoResponse {
    pub protocol_version: &'static str,
    pub server_version: &'static str,
    pub read_only: bool,
    pub capabilities: &'static [&'static str],
}

pub(crate) fn number(text: String) -> Result<Value> {
    Number::from_str(&text)
        .map(Value::Number)
        .map_err(|_| Error::Internal(format!("failed to encode JSON number '{text}'")))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::TypedParameter;
    use crate::ParameterValue;

    #[test]
    fn parses_typed_parameters_without_float_roundtrip() {
        let decimal = TypedParameter {
            data_type: "decimal128".into(),
            value: json!("-12.30"),
            precision: Some(8),
            scale: Some(2),
        };
        assert!(matches!(
            decimal.to_value().unwrap(),
            ParameterValue::Decimal128 {
                value: -1230,
                precision: 8,
                scale: 2
            }
        ));
    }

    #[test]
    fn rejects_malformed_or_out_of_precision_decimals() {
        for value in [
            json!("."),
            json!("-"),
            json!("+."),
            json!("--1"),
            json!("+-1"),
            json!("-+1"),
            json!("1.2.3"),
            json!("1e2"),
        ] {
            let decimal = TypedParameter {
                data_type: "decimal128".into(),
                value,
                precision: Some(4),
                scale: Some(2),
            };
            assert!(decimal.to_value().is_err());
        }
        let too_large = TypedParameter {
            data_type: "decimal128".into(),
            value: json!("100.00"),
            precision: Some(4),
            scale: Some(2),
        };
        assert!(too_large.to_value().is_err());
    }
}

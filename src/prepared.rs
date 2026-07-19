use std::{collections::BTreeSet, ops::ControlFlow};

use arrow::datatypes::{DataType, TimeUnit};
use sqlparser::ast::{
    Expr, SelectItem, SetExpr, Statement, Value, Visit, VisitMut, Visitor, VisitorMut,
};
use sqlparser::tokenizer::Span;

use crate::{Error, QueryResult, Result, Session};

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ParameterValue {
    Null(DataType),
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Utf8(String),
    Binary(Vec<u8>),
    Date32(i32),
    TimestampMicrosecond(i64),
    Time {
        value: i64,
        unit: TimeUnit,
    },
    Timestamp {
        value: i64,
        unit: TimeUnit,
        timezone: Option<String>,
    },
    Uuid([u8; 16]),
    MonthInterval(i32),
    DayInterval(i32),
    MonthDayNanoInterval {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
}

#[derive(Clone)]
pub struct PreparedStatement {
    session: Session,
    template: Statement,
    layout: ParameterLayout,
}

impl PreparedStatement {
    pub(crate) fn new(session: Session, sql: &str) -> Result<Self> {
        let mut statements = crate::sql::parse_statements(sql)?;
        if statements.len() != 1 {
            return Err(Error::InvalidArgument(
                "exactly one SQL statement is required".into(),
            ));
        }
        let template = statements.remove(0);
        if !matches!(&template, Statement::Query(_) | Statement::Explain { .. }) {
            return Err(Error::Unsupported(
                "prepared statements currently support queries and EXPLAIN only".into(),
            ));
        }
        crate::table_function::reject_parameterized_file_functions(&template)?;
        let layout = ParameterLayout::analyze(&template)?;
        Ok(Self {
            session,
            template,
            layout,
        })
    }

    pub fn parameter_count(&self) -> usize {
        self.layout.count
    }

    pub async fn execute(&self, parameters: &[ParameterValue]) -> Result<QueryResult> {
        let statement = self.instantiate(parameters)?;
        self.session.execute_prepared(statement).await
    }

    pub(crate) async fn execute_http_read_only(
        &self,
        parameters: &[ParameterValue],
    ) -> Result<QueryResult> {
        let statement = self.instantiate(parameters)?;
        self.session
            .execute_prepared_http_read_only(statement)
            .await
    }

    pub(crate) fn instantiate(&self, parameters: &[ParameterValue]) -> Result<Statement> {
        if parameters.len() != self.layout.count {
            return Err(Error::InvalidArgument(format!(
                "prepared statement expects {} parameters, got {}",
                self.layout.count,
                parameters.len()
            )));
        }
        let replacements = parameters
            .iter()
            .map(ParameterValue::expression)
            .collect::<Result<Vec<_>>>()?;
        let mut statement = self.template.clone();
        self.layout.substitute(&mut statement, &replacements)?;
        Ok(statement)
    }
}

#[derive(Clone, Copy)]
struct ParameterLayout {
    style: ParameterStyle,
    count: usize,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ParameterStyle {
    None,
    Question,
    Numbered,
}

impl ParameterLayout {
    fn analyze(statement: &Statement) -> Result<Self> {
        struct Analyzer {
            style: ParameterStyle,
            questions: usize,
            numbered: BTreeSet<usize>,
        }
        impl Visitor for Analyzer {
            type Break = Error;

            fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
                let Expr::Value(value) = expression else {
                    return ControlFlow::Continue(());
                };
                let Value::Placeholder(placeholder) = &value.value else {
                    return ControlFlow::Continue(());
                };
                match parameter_index(placeholder) {
                    Ok(None) => {
                        if self.style == ParameterStyle::Numbered {
                            return ControlFlow::Break(mixed_parameters());
                        }
                        self.style = ParameterStyle::Question;
                        self.questions += 1;
                    }
                    Ok(Some(index)) => {
                        if self.style == ParameterStyle::Question {
                            return ControlFlow::Break(mixed_parameters());
                        }
                        self.style = ParameterStyle::Numbered;
                        self.numbered.insert(index);
                    }
                    Err(error) => return ControlFlow::Break(error),
                }
                ControlFlow::Continue(())
            }
        }
        let mut analyzer = Analyzer {
            style: ParameterStyle::None,
            questions: 0,
            numbered: BTreeSet::new(),
        };
        if let ControlFlow::Break(error) = statement.visit(&mut analyzer) {
            return Err(error);
        }
        let count = match analyzer.style {
            ParameterStyle::None => 0,
            ParameterStyle::Question => analyzer.questions,
            ParameterStyle::Numbered => {
                let count = analyzer.numbered.last().copied().unwrap_or(0);
                if analyzer.numbered.iter().copied().ne(1..=count) {
                    return Err(Error::InvalidArgument(
                        "numbered parameters must be contiguous from $1".into(),
                    ));
                }
                count
            }
        };
        Ok(Self {
            style: analyzer.style,
            count,
        })
    }

    fn substitute(&self, statement: &mut Statement, replacements: &[Expr]) -> Result<()> {
        struct Replacer<'a> {
            style: ParameterStyle,
            replacements: &'a [Expr],
            question: usize,
        }
        impl VisitorMut for Replacer<'_> {
            type Break = Error;

            fn pre_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
                let Expr::Value(value) = expression else {
                    return ControlFlow::Continue(());
                };
                let Value::Placeholder(placeholder) = &value.value else {
                    return ControlFlow::Continue(());
                };
                let index = match self.style {
                    ParameterStyle::Question => {
                        let index = self.question;
                        self.question += 1;
                        index
                    }
                    ParameterStyle::Numbered => match parameter_index(placeholder) {
                        Ok(Some(index)) => index - 1,
                        Ok(None) => return ControlFlow::Break(mixed_parameters()),
                        Err(error) => return ControlFlow::Break(error),
                    },
                    ParameterStyle::None => {
                        return ControlFlow::Break(Error::Internal(
                            "unregistered prepared parameter reached substitution".into(),
                        ));
                    }
                };
                let Some(replacement) = self.replacements.get(index) else {
                    return ControlFlow::Break(Error::Internal(
                        "prepared parameter index is out of bounds".into(),
                    ));
                };
                *expression = expression_at_span(replacement.clone(), value.span);
                ControlFlow::Continue(())
            }
        }
        let mut replacer = Replacer {
            style: self.style,
            replacements,
            question: 0,
        };
        match statement.visit(&mut replacer) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }
}

fn expression_at_span(mut expression: Expr, span: Span) -> Expr {
    struct Reposition {
        span: Span,
    }
    impl VisitorMut for Reposition {
        type Break = std::convert::Infallible;

        fn pre_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
            if let Expr::Value(value) = expression {
                value.span = self.span;
            }
            ControlFlow::Continue(())
        }
    }
    let _ = VisitMut::visit(&mut expression, &mut Reposition { span });
    expression
}

impl ParameterValue {
    fn expression(&self) -> Result<Expr> {
        parse_expression(&self.sql_literal()?)
    }

    fn sql_literal(&self) -> Result<String> {
        Ok(match self {
            Self::Null(DataType::Timestamp(unit, Some(timezone))) => {
                internal_timestamp_sql(None, *unit, timezone)?
            }
            Self::Null(data_type) => format!("CAST(NULL AS {})", sql_type(data_type)?),
            Self::Boolean(value) => value.to_string(),
            Self::Int64(value) => format!("CAST('{}' AS BIGINT)", value),
            Self::UInt64(value) => format!("CAST('{}' AS UBIGINT)", value),
            Self::Float64(value) => format!("CAST('{}' AS DOUBLE)", value),
            Self::Decimal128 {
                value,
                precision,
                scale,
            } => {
                validate_decimal(*precision, *scale)?;
                format!(
                    "CAST('{}' AS DECIMAL({}, {}))",
                    format_decimal(*value, *scale),
                    precision,
                    scale
                )
            }
            Self::Utf8(value) => format!("'{}'", value.replace('\'', "''")),
            Self::Binary(value) => {
                let mut literal = String::with_capacity(value.len().saturating_mul(2) + 3);
                literal.push_str("X'");
                for byte in value {
                    use std::fmt::Write;
                    write!(literal, "{byte:02X}").expect("writing to String cannot fail");
                }
                literal.push('\'');
                literal
            }
            Self::Date32(value) => {
                format!("DATE '{}'", crate::sql::temporal::format_date32(*value))
            }
            Self::TimestampMicrosecond(value) => format!(
                "TIMESTAMP '{}'",
                crate::sql::temporal::format_timestamp_microsecond(*value)
            ),
            Self::Time { value, unit } => format!(
                "TIME({}) '{}'",
                temporal_precision(*unit),
                crate::sql::temporal::format_time(*value, *unit)?
            ),
            Self::Timestamp {
                value,
                unit,
                timezone,
            } => {
                if let Some(timezone) = timezone {
                    return internal_timestamp_sql(Some(*value), *unit, timezone);
                }
                let kind = if timezone.is_some() {
                    "TIMESTAMPTZ"
                } else {
                    "TIMESTAMP"
                };
                let suffix = if timezone.is_some() { "+00:00" } else { "" };
                format!(
                    "{kind}({}) '{}{suffix}'",
                    temporal_precision(*unit),
                    crate::sql::temporal::format_timestamp(*value, *unit),
                )
            }
            Self::Uuid(value) => format!("UUID '{}'", uuid::Uuid::from_bytes(*value)),
            Self::MonthInterval(months) => format!("INTERVAL '{months}' MONTH"),
            Self::DayInterval(days) => format!("INTERVAL '{days}' DAY"),
            Self::MonthDayNanoInterval {
                months,
                days,
                nanoseconds,
            } => format!(
                "INTERVAL '{months} months {days} days {} seconds'",
                format_interval_seconds(*nanoseconds)
            ),
        })
    }
}

fn parse_expression(sql: &str) -> Result<Expr> {
    let mut statements = crate::sql::parse_statements(&format!("SELECT {sql}"))?;
    let Statement::Query(query) = statements.remove(0) else {
        return Err(Error::Internal(
            "parameter literal did not parse as a query".into(),
        ));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(Error::Internal(
            "parameter literal query has no SELECT".into(),
        ));
    };
    match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(expression)] => Ok(expression.clone()),
        _ => Err(Error::Internal(
            "parameter literal produced an invalid projection".into(),
        )),
    }
}

fn parameter_index(value: &str) -> Result<Option<usize>> {
    if value == "?" {
        return Ok(None);
    }
    let number = value.strip_prefix('$').ok_or_else(|| {
        Error::InvalidArgument(format!("unsupported parameter placeholder '{value}'"))
    })?;
    let index = number
        .parse::<usize>()
        .map_err(|_| Error::InvalidArgument(format!("invalid numbered parameter '{value}'")))?;
    if index == 0 {
        return Err(Error::InvalidArgument(
            "numbered parameters start at $1".into(),
        ));
    }
    Ok(Some(index))
}

fn mixed_parameters() -> Error {
    Error::InvalidArgument("cannot mix '?' and '$n' parameters".into())
}

fn sql_type(data_type: &DataType) -> Result<String> {
    Ok(match data_type {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt64 => "UBIGINT".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal128(precision, scale) => {
            validate_decimal(*precision, *scale)?;
            format!("DECIMAL({precision}, {scale})")
        }
        DataType::Utf8 => "VARCHAR".into(),
        DataType::Binary => "BLOB".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP".into(),
        DataType::Time32(unit) | DataType::Time64(unit) => {
            format!("TIME({})", temporal_precision(*unit))
        }
        DataType::Timestamp(unit, timezone) => {
            let kind = if timezone.is_some() {
                "TIMESTAMPTZ"
            } else {
                "TIMESTAMP"
            };
            format!("{kind}({})", temporal_precision(*unit))
        }
        DataType::FixedSizeBinary(16) => "UUID".into(),
        DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
            "INTERVAL YEAR TO MONTH".into()
        }
        DataType::Interval(arrow::datatypes::IntervalUnit::DayTime) => "INTERVAL DAY".into(),
        DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => {
            "INTERVAL DAY TO SECOND".into()
        }
        other => {
            return Err(Error::Unsupported(format!(
                "typed NULL parameter of type {other} is not supported"
            )));
        }
    })
}

fn temporal_precision(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

fn internal_timestamp_sql(value: Option<i64>, unit: TimeUnit, timezone: &str) -> Result<String> {
    let timezone = timezone.parse::<chrono_tz::Tz>().map_err(|_| {
        Error::InvalidArgument(format!(
            "timestamp parameter has unknown IANA timezone '{timezone}'"
        ))
    })?;
    let function = if value.is_some() {
        "__rustdb_internal_parameter_timestamp"
    } else {
        "__rustdb_internal_parameter_timestamp_null"
    };
    let value = value
        .map(|value| format!("'{value}', "))
        .unwrap_or_default();
    Ok(format!(
        "{function}({value}'{}', '{timezone}')",
        temporal_unit_name(unit)
    ))
}

fn temporal_unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "second",
        TimeUnit::Millisecond => "millisecond",
        TimeUnit::Microsecond => "microsecond",
        TimeUnit::Nanosecond => "nanosecond",
    }
}

fn format_interval_seconds(nanoseconds: i64) -> String {
    let negative = nanoseconds.is_negative();
    let absolute = nanoseconds.unsigned_abs();
    let seconds = absolute / 1_000_000_000;
    let fraction = absolute % 1_000_000_000;
    let sign = if negative { "-" } else { "" };
    if fraction == 0 {
        format!("{sign}{seconds}")
    } else {
        let fraction = format!("{fraction:09}");
        format!("{sign}{seconds}.{}", fraction.trim_end_matches('0'))
    }
}

fn validate_decimal(precision: u8, scale: i8) -> Result<()> {
    if precision == 0 || precision > 38 || scale < 0 || scale as u8 > precision {
        return Err(Error::InvalidArgument(format!(
            "parameter Decimal128 precision/scale ({precision}, {scale}) is invalid"
        )));
    }
    Ok(())
}

fn format_decimal(value: i128, scale: i8) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let negative = value.is_negative();
    let mut digits = value.unsigned_abs().to_string();
    let scale = scale as usize;
    if digits.len() <= scale {
        digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
    }
    digits.insert(digits.len() - scale, '.');
    if negative {
        digits.insert(0, '-');
    }
    digits
}

#[cfg(test)]
#[path = "prepared/tests.rs"]
mod tests;

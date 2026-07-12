use std::fmt;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Date32(i32),
    TimestampMicrosecond(i64),
    DayInterval(i32),
    MonthInterval(i32),
    Utf8(String),
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::Int64(_) => DataType::Int64,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float64(_) => DataType::Float64,
            Self::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128(*precision, *scale),
            Self::Date32(_) => DataType::Date32,
            Self::TimestampMicrosecond(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::DayInterval(_) => DataType::Interval(IntervalUnit::DayTime),
            Self::MonthInterval(_) => DataType::Interval(IntervalUnit::YearMonth),
            Self::Utf8(_) => DataType::Utf8,
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("NULL"),
            Self::Boolean(value) => write!(formatter, "{value}"),
            Self::Int64(value) => write!(formatter, "{value}"),
            Self::UInt64(value) => write!(formatter, "{value}"),
            Self::Float64(value) => write!(formatter, "{value}"),
            Self::Decimal128 { value, scale, .. } => {
                formatter.write_str(&format_decimal(*value, *scale))
            }
            Self::Date32(value) => write!(formatter, "DATE_DAY({value})"),
            Self::TimestampMicrosecond(value) => {
                write!(formatter, "TIMESTAMP_MICROSECOND({value})")
            }
            Self::DayInterval(value) => write!(formatter, "INTERVAL '{value}' DAY"),
            Self::MonthInterval(value) => write!(formatter, "INTERVAL '{value}' MONTH"),
            Self::Utf8(value) => write!(formatter, "'{value}'"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOp {
    Not,
    Negate,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundExpr {
    pub kind: ExprKind,
    pub data_type: DataType,
    pub display_name: String,
}

impl BoundExpr {
    pub fn column(index: usize, data_type: DataType, name: impl Into<String>) -> Self {
        let display_name = name.into();
        Self {
            kind: ExprKind::Column(index),
            data_type,
            display_name,
        }
    }

    pub fn literal(value: ScalarValue) -> Self {
        Self {
            data_type: value.data_type(),
            display_name: value.to_string(),
            kind: ExprKind::Literal(value),
        }
    }

    pub(crate) fn outer_ref(
        depth: u8,
        index: usize,
        data_type: DataType,
        name: impl Into<String>,
    ) -> Self {
        let display_name = name.into();
        Self {
            kind: ExprKind::OuterRef { depth, index },
            data_type,
            display_name,
        }
    }

    pub fn referenced_columns(&self, output: &mut Vec<usize>) {
        match &self.kind {
            ExprKind::Column(index) => output.push(*index),
            ExprKind::OuterRef { .. }
            | ExprKind::DeferredGroup(_)
            | ExprKind::DeferredAggregate(_)
            | ExprKind::Literal(_) => {}
            ExprKind::Binary { left, right, .. } => {
                left.referenced_columns(output);
                right.referenced_columns(output);
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr.referenced_columns(output);
                pattern.referenced_columns(output);
            }
            ExprKind::Case {
                when_then,
                else_expr,
            } => {
                for (when, then) in when_then {
                    when.referenced_columns(output);
                    then.referenced_columns(output);
                }
                else_expr.referenced_columns(output);
            }
            ExprKind::Unary { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::Cast { expr } => expr.referenced_columns(output),
            ExprKind::ScalarFunction { args, .. } => {
                for arg in args {
                    arg.referenced_columns(output);
                }
            }
        }
    }

    pub(crate) fn outer_references(&self, output: &mut Vec<(u8, usize)>) {
        match &self.kind {
            ExprKind::OuterRef { depth, index } => output.push((*depth, *index)),
            ExprKind::Column(_)
            | ExprKind::DeferredGroup(_)
            | ExprKind::DeferredAggregate(_)
            | ExprKind::Literal(_) => {}
            ExprKind::Binary { left, right, .. } => {
                left.outer_references(output);
                right.outer_references(output);
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr.outer_references(output);
                pattern.outer_references(output);
            }
            ExprKind::Case {
                when_then,
                else_expr,
            } => {
                for (when, then) in when_then {
                    when.outer_references(output);
                    then.outer_references(output);
                }
                else_expr.outer_references(output);
            }
            ExprKind::Unary { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::Cast { expr } => expr.outer_references(output),
            ExprKind::ScalarFunction { args, .. } => {
                for arg in args {
                    arg.outer_references(output);
                }
            }
        }
    }

    pub(crate) fn contains_outer_ref(&self) -> bool {
        let mut references = Vec::new();
        self.outer_references(&mut references);
        !references.is_empty()
    }

    /// Returns true when evaluating this expression cannot raise a structured
    /// SQL error. Optimizer relocation and boolean execution use the same
    /// deliberately conservative classification so eager evaluation never
    /// exposes an error from an otherwise inactive branch.
    pub(crate) fn is_structurally_infallible(&self) -> bool {
        match &self.kind {
            ExprKind::Column(_) | ExprKind::Literal(_) => true,
            ExprKind::Binary {
                left,
                op:
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::And
                    | BinaryOp::Or,
                right,
            } => left.is_structurally_infallible() && right.is_structurally_infallible(),
            ExprKind::Unary {
                op: UnaryOp::Not,
                expr,
            }
            | ExprKind::IsNull { expr, .. } => expr.is_structurally_infallible(),
            ExprKind::OuterRef { .. }
            | ExprKind::DeferredGroup(_)
            | ExprKind::DeferredAggregate(_)
            | ExprKind::Binary { .. }
            | ExprKind::Unary { .. }
            | ExprKind::Like { .. }
            | ExprKind::Case { .. }
            | ExprKind::Cast { .. }
            | ExprKind::ScalarFunction { .. } => false,
        }
    }

    pub(crate) fn contains_deferred_aggregate(&self) -> bool {
        match &self.kind {
            ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => true,
            ExprKind::Column(_) | ExprKind::OuterRef { .. } | ExprKind::Literal(_) => false,
            ExprKind::Binary { left, right, .. }
            | ExprKind::Like {
                expr: left,
                pattern: right,
                ..
            } => left.contains_deferred_aggregate() || right.contains_deferred_aggregate(),
            ExprKind::Unary { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::Cast { expr } => expr.contains_deferred_aggregate(),
            ExprKind::Case {
                when_then,
                else_expr,
            } => {
                when_then.iter().any(|(when, then)| {
                    when.contains_deferred_aggregate() || then.contains_deferred_aggregate()
                }) || else_expr.contains_deferred_aggregate()
            }
            ExprKind::ScalarFunction { args, .. } => {
                args.iter().any(BoundExpr::contains_deferred_aggregate)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExprKind {
    Column(usize),
    OuterRef {
        depth: u8,
        index: usize,
    },
    /// Planning-only reference to a GROUP BY output used by an IN attachment
    /// that must run after aggregation.
    DeferredGroup(usize),
    /// Planning-only aggregate state used by an IN attachment that must run
    /// after aggregation. Optimizer verification rejects any leaked value.
    DeferredAggregate(Box<AggregateExpr>),
    Literal(ScalarValue),
    Binary {
        left: Box<BoundExpr>,
        op: BinaryOp,
        right: Box<BoundExpr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<BoundExpr>,
    },
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
    Like {
        expr: Box<BoundExpr>,
        pattern: Box<BoundExpr>,
        negated: bool,
        escape: Option<char>,
    },
    Case {
        when_then: Vec<(BoundExpr, BoundExpr)>,
        else_expr: Box<BoundExpr>,
    },
    Cast {
        expr: Box<BoundExpr>,
    },
    ScalarFunction {
        function: ScalarFunction,
        args: Vec<BoundExpr>,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DateTimePart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl fmt::Display for DateTimePart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Year => "year",
            Self::Month => "month",
            Self::Day => "day",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScalarFunction {
    Substring,
    Length,
    Lower,
    Upper,
    Trim,
    LTrim,
    RTrim,
    Concat,
    Replace,
    StartsWith,
    EndsWith,
    Contains,
    Coalesce,
    NullIf,
    Abs,
    Ceil,
    Floor,
    Round,
    DatePart(DateTimePart),
    DateTrunc(DateTimePart),
}

impl fmt::Display for ScalarFunction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DatePart(part) => write!(formatter, "date_part[{part}]"),
            Self::DateTrunc(part) => write!(formatter, "date_trunc[{part}]"),
            other => formatter.write_str(match other {
                Self::Substring => "substring",
                Self::Length => "length",
                Self::Lower => "lower",
                Self::Upper => "upper",
                Self::Trim => "trim",
                Self::LTrim => "ltrim",
                Self::RTrim => "rtrim",
                Self::Concat => "concat",
                Self::Replace => "replace",
                Self::StartsWith => "starts_with",
                Self::EndsWith => "ends_with",
                Self::Contains => "contains",
                Self::Coalesce => "coalesce",
                Self::NullIf => "nullif",
                Self::Abs => "abs",
                Self::Ceil => "ceil",
                Self::Floor => "floor",
                Self::Round => "round",
                Self::DatePart(_) | Self::DateTrunc(_) => unreachable!(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub expr: Option<BoundExpr>,
    /// Whether the aggregate consumes one value per distinct, non-NULL input.
    /// MIN/MAX DISTINCT are normalized to ordinary aggregates by the binder.
    pub distinct: bool,
    pub data_type: DataType,
    pub display_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortExpr {
    pub expr: BoundExpr,
    pub descending: bool,
    pub nulls_first: bool,
}

fn format_decimal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let negative = value.is_negative();
    let mut digits = value.unsigned_abs().to_string();
    let scale = scale as usize;
    if digits.len() <= scale {
        digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
    }
    let split = digits.len() - scale;
    digits.insert(split, '.');
    if negative {
        digits.insert(0, '-');
    }
    digits
}

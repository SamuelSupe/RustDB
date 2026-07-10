use std::fmt;

use arrow::datatypes::{DataType, IntervalUnit};

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

    pub fn referenced_columns(&self, output: &mut Vec<usize>) {
        match &self.kind {
            ExprKind::Column(index) => output.push(*index),
            ExprKind::Literal(_) => {}
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
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExprKind {
    Column(usize),
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

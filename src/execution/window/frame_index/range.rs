use std::{cmp::Ordering, sync::Arc};

use arrow::datatypes::DataType;

use crate::runtime::{QueryContext, SpillFile};
use crate::sql::{SortExpr, WindowExpr, WindowFrameBound};
use crate::{Error, Result};

use super::super::navigation::{
    cursor::{PeerCursor, ValueCursor},
    frame::PeerRange,
};
use super::super::sidecar::RangeSidecar;
use super::normalize;
use crate::execution::value::CellValue;

pub(super) struct RangeFrames {
    rows: u64,
    frame: crate::sql::WindowFrame,
    order: SortExpr,
    current: ValueCursor,
    starts: SearchCursor,
    ends: SearchCursor,
    peers: PeerCursor,
}

impl RangeFrames {
    pub(super) fn new(
        partition: &SpillFile,
        expression: &WindowExpr,
        peers: &RangeSidecar,
        context: &Arc<QueryContext>,
        rows: u64,
    ) -> Result<Self> {
        let [order] = expression.order_by.as_slice() else {
            return Err(Error::Internal(
                "bounded RANGE does not have one ORDER BY expression".into(),
            ));
        };
        Ok(Self {
            rows,
            frame: expression.frame,
            order: order.clone(),
            current: ValueCursor::new(partition, order.expr.clone(), Arc::clone(context))?,
            starts: SearchCursor::new(partition, order, Arc::clone(context), rows)?,
            ends: SearchCursor::new(partition, order, Arc::clone(context), rows)?,
            peers: PeerCursor::new(&peers.file, Arc::clone(context))?,
        })
    }

    pub(super) fn frame_at(&mut self, row: u64) -> Result<(u64, u64)> {
        let current = self.current.value_at(row)?;
        let peer = self.peers.range_at(row)?;
        let start = start_bound(
            self.frame.start,
            &current,
            peer,
            &self.order,
            &mut self.starts,
            self.rows,
        )?;
        let end = end_bound(
            self.frame.end,
            &current,
            peer,
            &self.order,
            &mut self.ends,
            self.rows,
        )?;
        Ok(normalize(start, end))
    }
}

fn start_bound(
    bound: WindowFrameBound,
    current: &CellValue,
    peer: PeerRange,
    order: &SortExpr,
    search: &mut SearchCursor,
    rows: u64,
) -> Result<u64> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Ok(0),
        WindowFrameBound::CurrentRow => Ok(peer.start),
        WindowFrameBound::UnboundedFollowing => Ok(rows),
        WindowFrameBound::Preceding(offset) => {
            search.first(current, offset, true, order.descending, peer.start)
        }
        WindowFrameBound::Following(offset) => {
            search.first(current, offset, false, order.descending, peer.start)
        }
    }
}

fn end_bound(
    bound: WindowFrameBound,
    current: &CellValue,
    peer: PeerRange,
    order: &SortExpr,
    search: &mut SearchCursor,
    rows: u64,
) -> Result<u64> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Ok(0),
        WindowFrameBound::CurrentRow => Ok(peer.end),
        WindowFrameBound::UnboundedFollowing => Ok(rows),
        WindowFrameBound::Preceding(offset) => {
            search.after(current, offset, true, order.descending, peer.end)
        }
        WindowFrameBound::Following(offset) => {
            search.after(current, offset, false, order.descending, peer.end)
        }
    }
}

struct SearchCursor {
    cursor: ValueCursor,
    data_type: DataType,
    descending: bool,
    nulls_first: bool,
    position: u64,
    rows: u64,
}

impl SearchCursor {
    fn new(
        file: &SpillFile,
        order: &SortExpr,
        context: Arc<QueryContext>,
        rows: u64,
    ) -> Result<Self> {
        Ok(Self {
            cursor: ValueCursor::new(file, order.expr.clone(), context)?,
            data_type: order.expr.data_type.clone(),
            descending: order.descending,
            nulls_first: order.nulls_first,
            position: 0,
            rows,
        })
    }

    fn first(
        &mut self,
        current: &CellValue,
        offset: u64,
        preceding: bool,
        descending: bool,
        null_peer: u64,
    ) -> Result<u64> {
        let Some(threshold) = threshold(current, &self.data_type, offset, preceding, descending)?
        else {
            return Ok(null_peer);
        };
        while self.position < self.rows {
            let candidate = self.cursor.value_at(self.position)?;
            if ordered_cmp(
                &candidate,
                &threshold,
                &self.data_type,
                self.descending,
                self.nulls_first,
            )? != Ordering::Less
            {
                break;
            }
            self.position += 1;
        }
        Ok(self.position)
    }

    fn after(
        &mut self,
        current: &CellValue,
        offset: u64,
        preceding: bool,
        descending: bool,
        null_peer: u64,
    ) -> Result<u64> {
        let Some(threshold) = threshold(current, &self.data_type, offset, preceding, descending)?
        else {
            return Ok(null_peer);
        };
        while self.position < self.rows {
            let candidate = self.cursor.value_at(self.position)?;
            if ordered_cmp(
                &candidate,
                &threshold,
                &self.data_type,
                self.descending,
                self.nulls_first,
            )? == Ordering::Greater
            {
                break;
            }
            self.position += 1;
        }
        Ok(self.position)
    }
}

#[derive(Clone, Copy)]
enum RangeValue {
    NegativeInfinity,
    Integer(i128),
    Float(f64),
    PositiveInfinity,
}

fn threshold(
    value: &CellValue,
    data_type: &DataType,
    offset: u64,
    preceding: bool,
    descending: bool,
) -> Result<Option<RangeValue>> {
    if value.is_null() {
        return Ok(None);
    }
    let add = preceding == descending;
    let value = range_value(value, data_type)?;
    Ok(Some(match value {
        RangeValue::Integer(value) => {
            let offset = scaled_offset(offset, data_type)?;
            match if add {
                value.checked_add(offset)
            } else {
                value.checked_sub(offset)
            } {
                Some(value) => RangeValue::Integer(value),
                None if add => RangeValue::PositiveInfinity,
                None => RangeValue::NegativeInfinity,
            }
        }
        RangeValue::Float(value) => RangeValue::Float(if add {
            value + offset as f64
        } else {
            value - offset as f64
        }),
        infinite => infinite,
    }))
}

fn scaled_offset(offset: u64, data_type: &DataType) -> Result<i128> {
    let offset = i128::from(offset);
    let scale = match data_type {
        DataType::Decimal128(_, scale) => u32::try_from(*scale).map_err(|_| {
            Error::InvalidArgument("bounded RANGE DECIMAL scale must be non-negative".into())
        })?,
        _ => 0,
    };
    offset
        .checked_mul(10_i128.checked_pow(scale).ok_or_else(|| {
            Error::Execution("bounded RANGE offset scale overflowed Decimal128".into())
        })?)
        .ok_or_else(|| Error::Execution("bounded RANGE offset overflowed Decimal128".into()))
}

fn range_value(value: &CellValue, data_type: &DataType) -> Result<RangeValue> {
    match value {
        CellValue::Int64(value) => Ok(RangeValue::Integer(i128::from(*value))),
        CellValue::UInt64(value) => Ok(RangeValue::Integer(i128::from(*value))),
        CellValue::Decimal128(value) => Ok(RangeValue::Integer(*value)),
        CellValue::Float64(value) if value.is_nan() => Err(Error::InvalidArgument(
            "bounded RANGE does not support NaN ORDER BY values".into(),
        )),
        CellValue::Float64(value) => Ok(RangeValue::Float(*value)),
        other => Err(Error::Internal(format!(
            "bounded RANGE expected {data_type}, got {other:?}"
        ))),
    }
}

fn ordered_cmp(
    candidate: &CellValue,
    threshold: &RangeValue,
    data_type: &DataType,
    descending: bool,
    nulls_first: bool,
) -> Result<Ordering> {
    if candidate.is_null() {
        return Ok(if nulls_first {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    let natural = compare_value(range_value(candidate, data_type)?, *threshold);
    Ok(if descending {
        natural.reverse()
    } else {
        natural
    })
}

fn compare_value(left: RangeValue, right: RangeValue) -> Ordering {
    use RangeValue::{Float, Integer, NegativeInfinity, PositiveInfinity};
    match (left, right) {
        (NegativeInfinity, NegativeInfinity) | (PositiveInfinity, PositiveInfinity) => {
            Ordering::Equal
        }
        (NegativeInfinity, _) | (_, PositiveInfinity) => Ordering::Less,
        (PositiveInfinity, _) | (_, NegativeInfinity) => Ordering::Greater,
        (Integer(left), Integer(right)) => left.cmp(&right),
        (Float(left), Float(right)) => left.total_cmp(&right),
        _ => unreachable!("RANGE values have one physical numeric family"),
    }
}

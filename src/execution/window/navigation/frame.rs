use crate::sql::{WindowExpr, WindowFrameBound, WindowFrameUnits, WindowFunction};

#[derive(Clone, Copy, Debug)]
pub(in crate::execution::window) struct PeerRange {
    pub(in crate::execution::window) group: u64,
    pub(in crate::execution::window) start: u64,
    pub(in crate::execution::window) end: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Target {
    Value(u64),
    Default(u64),
    Null,
}

pub(super) fn target(
    expression: &WindowExpr,
    row: u64,
    rows: u64,
    peer: Option<PeerRange>,
    indexed: Option<(u64, u64)>,
) -> Target {
    match &expression.function {
        WindowFunction::Lead { offset, .. } => row
            .checked_add(*offset)
            .filter(|target| *target < rows)
            .map(Target::Value)
            .unwrap_or(Target::Default(row)),
        WindowFunction::Lag { offset, .. } => row
            .checked_sub(*offset)
            .map(Target::Value)
            .unwrap_or(Target::Default(row)),
        WindowFunction::FirstValue(_) => indexed
            .and_then(|(start, end)| (start < end).then_some(Target::Value(start)))
            .or_else(|| {
                indexed.is_none().then(|| {
                    frame(expression, row, rows, peer)
                        .map(|(start, _)| Target::Value(start))
                        .unwrap_or(Target::Null)
                })
            })
            .unwrap_or(Target::Null),
        WindowFunction::LastValue(_) => indexed
            .and_then(|(start, end)| (start < end).then_some(Target::Value(end.saturating_sub(1))))
            .or_else(|| {
                indexed.is_none().then(|| {
                    frame(expression, row, rows, peer)
                        .map(|(_, end)| Target::Value(end))
                        .unwrap_or(Target::Null)
                })
            })
            .unwrap_or(Target::Null),
        _ => Target::Null,
    }
}

fn frame(
    expression: &WindowExpr,
    row: u64,
    rows: u64,
    peer: Option<PeerRange>,
) -> Option<(u64, u64)> {
    if rows == 0 {
        return None;
    }
    let (start, end) = match expression.frame.units {
        WindowFrameUnits::Rows => (
            start_index(expression.frame.start, row, rows)?,
            end_index(expression.frame.end, row, rows)?,
        ),
        WindowFrameUnits::Range => {
            let peer = peer?;
            (
                range_start(expression.frame.start, peer, rows)?,
                range_end(expression.frame.end, peer, rows)?,
            )
        }
        WindowFrameUnits::Groups => return None,
    };
    (start <= end).then_some((start, end))
}

fn start_index(bound: WindowFrameBound, row: u64, rows: u64) -> Option<u64> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Some(0),
        WindowFrameBound::Preceding(offset) => Some(row.saturating_sub(offset)),
        WindowFrameBound::CurrentRow => Some(row),
        WindowFrameBound::Following(offset) => {
            row.checked_add(offset).filter(|target| *target < rows)
        }
        WindowFrameBound::UnboundedFollowing => None,
    }
}

fn end_index(bound: WindowFrameBound, row: u64, rows: u64) -> Option<u64> {
    match bound {
        WindowFrameBound::UnboundedPreceding => None,
        WindowFrameBound::Preceding(offset) => row.checked_sub(offset),
        WindowFrameBound::CurrentRow => Some(row),
        WindowFrameBound::Following(offset) => Some(row.saturating_add(offset).min(rows - 1)),
        WindowFrameBound::UnboundedFollowing => Some(rows - 1),
    }
}

fn range_start(bound: WindowFrameBound, peer: PeerRange, rows: u64) -> Option<u64> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Some(0),
        WindowFrameBound::CurrentRow => Some(peer.start),
        WindowFrameBound::UnboundedFollowing => Some(rows - 1),
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => None,
    }
}

fn range_end(bound: WindowFrameBound, peer: PeerRange, rows: u64) -> Option<u64> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Some(0),
        WindowFrameBound::CurrentRow => peer.end.checked_sub(1),
        WindowFrameBound::UnboundedFollowing => Some(rows - 1),
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => None,
    }
}

use std::{cell::Cell, path::Path};

use crate::{Error, Result};

use super::record::{Record, RecordKind};

thread_local! {
    static STATE: Cell<State> = const { Cell::new(State::Off) };
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum State {
    Off,
    Armed,
    Published,
}

pub(crate) fn arm_ambiguous_reconciliation() {
    STATE.with(|state| state.set(State::Armed));
}

pub(super) fn publish_before_append(path: &Path, record: &Record) -> Result<()> {
    if !matches!(record.kind(), RecordKind::CatalogCommit { .. })
        || STATE.with(Cell::get) != State::Armed
    {
        return Ok(());
    }
    if let Err(error) = super::record::write(path, record) {
        STATE.with(|state| state.set(State::Off));
        return Err(error);
    }
    STATE.with(|state| state.set(State::Published));
    Ok(())
}

pub(super) fn reconciliation_error(path: &Path) -> Option<Error> {
    STATE
        .with(|state| {
            if state.get() != State::Published {
                return false;
            }
            state.set(State::Off);
            true
        })
        .then(|| Error::native_storage(path, "injected WAL reconciliation read failure"))
}

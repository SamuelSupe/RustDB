use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
};

use parking_lot::Mutex;
use uuid::Uuid;

use crate::{Error, Result};

mod checkpoint;
mod record;
mod recovery;
#[cfg(test)]
pub(super) mod test_failpoint;

use record::{Record, RecordKind, TransactionMode};

pub(super) const COMMIT_RECORD_HEADROOM_BYTES: u64 = record::MAX_RECORD_BYTES as u64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::storage::native) struct CatalogCommit {
    pub(super) transaction_id: String,
    pub(super) expected_generation: u64,
    pub(super) generation: u64,
}

pub(in crate::storage::native) struct WalInspection {
    pub(super) checkpoint_catalog_generation: u64,
    pub(super) referenced_transactions: BTreeSet<String>,
    pub(super) committed_generations: BTreeMap<u64, String>,
    pub(super) files: Vec<PathBuf>,
}

pub(in crate::storage::native) fn inspect_read_only(
    database_root: &Path,
    database_id: &str,
) -> Result<WalInspection> {
    let directory = database_root.join("wal");
    super::io::require_directory(&directory)?;
    let (next_lsn, checkpoint_catalog_generation) = checkpoint::read(&directory, database_id)?;
    let mut paths = fs::read_dir(&directory)
        .map_err(|error| Error::io(Some(directory.clone()), error))?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|error| Error::io(Some(directory.clone()), error))
        })
        .collect::<Result<Vec<_>>>()?;
    paths.sort_unstable();

    let mut referenced_transactions = BTreeSet::new();
    let mut committed_generations = BTreeMap::new();
    let mut files = Vec::new();
    let mut state = State {
        next_lsn,
        transactions: HashMap::new(),
    };
    let mut expected = next_lsn;
    for path in paths {
        if path.file_name().and_then(|name| name.to_str()) == Some(checkpoint::FILE_NAME) {
            files.push(path);
            continue;
        }
        if is_atomic_temp(&path) {
            continue;
        }
        let Some(lsn) = record::lsn_from_path(&path) else {
            return Err(Error::native_storage(
                &path,
                "unrecognized entry in WAL directory",
            ));
        };
        let wal_record = record::read(&path, database_id, lsn)?;
        referenced_transactions.insert(wal_record.transaction_id().to_owned());
        if let RecordKind::CatalogCommit { generation, .. } = wal_record.kind() {
            committed_generations.insert(*generation, wal_record.transaction_id().to_owned());
        }
        files.push(path.clone());
        if lsn < next_lsn {
            continue;
        }
        if lsn != expected {
            return Err(Error::native_storage(
                &path,
                format!("WAL LSN sequence is not contiguous: expected {expected}, found {lsn}"),
            ));
        }
        apply_record(&mut state, &wal_record)?;
        expected = expected
            .checked_add(1)
            .ok_or_else(|| Error::ResourceExhausted("WAL LSN counter is exhausted".to_owned()))?;
    }
    Ok(WalInspection {
        checkpoint_catalog_generation,
        referenced_transactions,
        committed_generations,
        files,
    })
}

#[derive(Debug)]
pub(in crate::storage::native) struct Wal {
    directory: PathBuf,
    database_id: String,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    next_lsn: u64,
    transactions: HashMap<String, TransactionState>,
}

#[derive(Clone, Debug)]
enum TransactionState {
    Active,
    CatalogCommitted(CatalogCommit),
    Aborted,
}

impl Wal {
    pub(super) fn open(database_root: &Path, database_id: &str) -> Result<Self> {
        let directory = database_root.join("wal");
        super::io::require_directory(&directory)?;
        remove_atomic_temps(&directory)?;

        let mut paths = fs::read_dir(&directory)
            .map_err(|error| Error::io(Some(directory.clone()), error))?
            .map(|entry| {
                entry
                    .map(|entry| entry.path())
                    .map_err(|error| Error::io(Some(directory.clone()), error))
            })
            .collect::<Result<Vec<_>>>()?;
        paths.sort_unstable();

        let (next_lsn, _) = checkpoint::read(&directory, database_id)?;
        let mut state = State {
            next_lsn,
            transactions: HashMap::new(),
        };
        for path in paths {
            if path.file_name().and_then(|name| name.to_str()) == Some(checkpoint::FILE_NAME) {
                continue;
            }
            let Some(lsn) = record::lsn_from_path(&path) else {
                return Err(Error::native_storage(
                    &path,
                    "unrecognized entry in WAL directory",
                ));
            };
            if lsn < state.next_lsn {
                super::io::remove_file(&path)?;
                continue;
            }
            if lsn != state.next_lsn {
                return Err(Error::native_storage(
                    &path,
                    format!(
                        "WAL LSN sequence is not contiguous: expected {}, found {lsn}",
                        state.next_lsn
                    ),
                ));
            }
            let record = record::read(&path, database_id, lsn)?;
            apply_record(&mut state, &record)?;
            state.next_lsn = state.next_lsn.checked_add(1).ok_or_else(|| {
                Error::ResourceExhausted("WAL LSN counter is exhausted".to_owned())
            })?;
        }

        Ok(Self {
            directory,
            database_id: database_id.to_owned(),
            state: Mutex::new(state),
        })
    }

    pub(super) fn begin(&self, transaction_id: &str, snapshot_generation: u64) -> Result<()> {
        Uuid::parse_str(transaction_id).map_err(|error| {
            Error::InvalidArgument(format!(
                "invalid transaction id '{transaction_id}': {error}"
            ))
        })?;
        self.append(
            transaction_id,
            RecordKind::Begin {
                snapshot_generation,
                mode: TransactionMode::ReadWrite,
            },
        )
    }

    pub(super) fn commit_catalog(
        &self,
        transaction_id: &str,
        expected_generation: u64,
        generation: u64,
    ) -> Result<()> {
        self.append(
            transaction_id,
            RecordKind::CatalogCommit {
                expected_generation,
                generation,
            },
        )
    }

    pub(super) fn abort(&self, transaction_id: &str) -> Result<()> {
        self.append(transaction_id, RecordKind::Abort)
    }

    pub(super) fn transaction_is_active(&self, transaction_id: &str) -> bool {
        matches!(
            self.state.lock().transactions.get(transaction_id),
            Some(TransactionState::Active)
        )
    }

    pub(super) fn recover_catalog(&self, root: &Path, database_id: &str) -> Result<()> {
        recovery::replay_catalog(root, database_id, self)
    }

    pub(super) fn abort_recovered_transactions(&self) -> Result<()> {
        let mut transaction_ids = self
            .state
            .lock()
            .transactions
            .iter()
            .filter(|(_, state)| matches!(state, TransactionState::Active))
            .map(|(transaction_id, _)| transaction_id.clone())
            .collect::<Vec<_>>();
        transaction_ids.sort_unstable();
        for transaction_id in &transaction_ids {
            self.abort(transaction_id)?;
        }
        Ok(())
    }

    pub(super) fn checkpoint(&self, catalog_generation: u64) -> Result<u64> {
        let mut state = self.state.lock();
        if state
            .transactions
            .values()
            .any(|transaction| matches!(transaction, TransactionState::Active))
        {
            return Err(Error::InvalidArgument(
                "CHECKPOINT cannot run while a native write is active".to_owned(),
            ));
        }
        let next_lsn = state.next_lsn;
        checkpoint::write(
            &self.directory,
            &self.database_id,
            next_lsn,
            catalog_generation,
        )?;
        let mut removed = 0_u64;
        for entry in fs::read_dir(&self.directory)
            .map_err(|error| Error::io(Some(self.directory.clone()), error))?
        {
            let path = entry
                .map_err(|error| Error::io(Some(self.directory.clone()), error))?
                .path();
            let Some(lsn) = record::lsn_from_path(&path) else {
                continue;
            };
            if lsn < next_lsn {
                super::io::remove_file(&path)?;
                removed = removed.saturating_add(1);
            }
        }
        state.transactions.clear();
        Ok(removed)
    }

    pub(super) fn stats(&self) -> (u64, usize, usize) {
        let state = self.state.lock();
        let active = state
            .transactions
            .values()
            .filter(|transaction| matches!(transaction, TransactionState::Active))
            .count();
        (state.next_lsn, state.transactions.len(), active)
    }

    fn append(&self, transaction_id: &str, kind: RecordKind) -> Result<()> {
        let mut state = self.state.lock();
        validate_transition(&state, transaction_id, &kind)?;
        let lsn = state.next_lsn;
        let record = Record::new(&self.database_id, lsn, transaction_id, kind);
        let path = record::path(&self.directory, lsn);
        #[cfg(test)]
        test_failpoint::publish_before_append(&path, &record)?;
        if let Err(write_error) = record::write(&path, &record) {
            // Only absence of the final path proves that atomic publication did
            // not happen. An uninspectable or mismatched record is unsafe to retry.
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(write_error);
                }
                Err(error) => {
                    return Err(ambiguous_append_error(
                        &record,
                        &path,
                        transaction_id,
                        format!(
                            "WAL append {lsn} failed and its publication could not be inspected; reopen the engine: {write_error}; {error}"
                        ),
                    ));
                }
                Ok(_) => {}
            }
            let persisted = read_for_reconciliation(&path, &self.database_id, lsn).map_err(
                |error| {
                    ambiguous_append_error(
                        &record,
                        &path,
                        transaction_id,
                        format!(
                            "WAL append {lsn} failed after a record appeared, but publication could not be reconciled; reopen the engine: {write_error}; {error}"
                        ),
                    )
                },
            )?;
            if persisted != record {
                return Err(ambiguous_append_error(
                    &record,
                    &path,
                    transaction_id,
                    format!(
                        "WAL append {lsn} failed and the published record did not match the intended record; reopen the engine: {write_error}"
                    ),
                ));
            }
            super::io::sync_dir(&self.directory).map_err(|error| {
                ambiguous_append_error(
                    &record,
                    &path,
                    transaction_id,
                    format!(
                        "WAL record {lsn} was published but its directory durability remains unknown; reopen the engine: {write_error}; {error}"
                    ),
                )
            })?;
        }
        apply_record(&mut state, &record)?;
        state.next_lsn = state
            .next_lsn
            .checked_add(1)
            .ok_or_else(|| Error::ResourceExhausted("WAL LSN counter is exhausted".to_owned()))?;
        Ok(())
    }

    fn catalog_commits(&self) -> Vec<CatalogCommit> {
        self.state
            .lock()
            .transactions
            .values()
            .filter_map(|state| match state {
                TransactionState::CatalogCommitted(commit) => Some(commit.clone()),
                TransactionState::Active | TransactionState::Aborted => None,
            })
            .collect()
    }
}

fn read_for_reconciliation(path: &Path, database_id: &str, lsn: u64) -> Result<Record> {
    #[cfg(test)]
    if let Some(error) = test_failpoint::reconciliation_error(path) {
        return Err(error);
    }
    record::read(path, database_id, lsn)
}

fn ambiguous_append_error(
    record: &Record,
    path: &Path,
    transaction_id: &str,
    message: String,
) -> Error {
    match record.kind() {
        RecordKind::CatalogCommit { .. } => {
            Error::commit_outcome_unknown(path, transaction_id, message)
        }
        RecordKind::Begin { .. } | RecordKind::Abort => Error::native_storage(path, message),
    }
}

fn validate_transition(state: &State, transaction_id: &str, kind: &RecordKind) -> Result<()> {
    let current = state.transactions.get(transaction_id);
    let valid = matches!(
        (current, kind),
        (None, RecordKind::Begin { .. })
            | (
                Some(TransactionState::Active),
                RecordKind::CatalogCommit { .. }
            )
            | (Some(TransactionState::Active), RecordKind::Abort)
    );
    if valid {
        return Ok(());
    }
    Err(Error::native_storage(
        "wal",
        format!("invalid WAL transaction state transition for {transaction_id}"),
    ))
}

fn apply_record(state: &mut State, record: &Record) -> Result<()> {
    validate_transition(state, record.transaction_id(), record.kind())?;
    let next = match record.kind() {
        RecordKind::Begin { .. } => TransactionState::Active,
        RecordKind::CatalogCommit {
            expected_generation,
            generation,
        } => TransactionState::CatalogCommitted(CatalogCommit {
            transaction_id: record.transaction_id().to_owned(),
            expected_generation: *expected_generation,
            generation: *generation,
        }),
        RecordKind::Abort => TransactionState::Aborted,
    };
    state
        .transactions
        .insert(record.transaction_id().to_owned(), next);
    Ok(())
}

fn remove_atomic_temps(directory: &Path) -> Result<()> {
    for entry in
        fs::read_dir(directory).map_err(|error| Error::io(Some(directory.to_path_buf()), error))?
    {
        let path = entry
            .map_err(|error| Error::io(Some(directory.to_path_buf()), error))?
            .path();
        if is_atomic_temp(&path) {
            super::io::remove_file(&path)?;
        }
    }
    Ok(())
}

fn is_atomic_temp(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(inner) = name
        .strip_prefix('.')
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((target, uuid)) = inner.rsplit_once('.') else {
        return false;
    };
    Uuid::parse_str(uuid).is_ok()
        && (record::lsn_from_path(&PathBuf::from(target)).is_some()
            || target == checkpoint::FILE_NAME)
}

#[cfg(test)]
#[path = "wal/tests.rs"]
mod tests;

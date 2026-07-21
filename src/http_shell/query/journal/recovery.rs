use std::{collections::BTreeMap, path::Path};

use crate::{Error, Result};

use super::{
    PersistedQuery,
    codec::{self, EventKind},
    disk,
};
use crate::http_shell::QueryState;

pub(super) struct Recovered {
    pub(super) queries: BTreeMap<String, PersistedQuery>,
    pub(super) last_seq: u64,
    pub(super) events_since_snapshot: u64,
    pub(super) normalized: Vec<PersistedQuery>,
}

pub(super) fn load(root: &Path, _producer_version: &str) -> Result<Recovered> {
    load_from_records(root, disk::read_journal(root)?)
}

pub(super) fn check(root: &Path) -> Result<usize> {
    disk::check_root(root)?;
    let recovered = load_from_records(root, disk::read_journal_strict(root)?)?;
    Ok(recovered.queries.len())
}

fn load_from_records(root: &Path, records: Vec<Vec<u8>>) -> Result<Recovered> {
    let snapshot = disk::read_snapshot(root)?
        .map(|bytes| codec::decode_snapshot(&bytes))
        .transpose()?;
    let snapshot_seq = snapshot.as_ref().map_or(0, |value| value.last_seq);
    let mut queries = BTreeMap::new();
    if let Some(snapshot) = snapshot {
        for query in snapshot.queries {
            let id = query.query_id.clone();
            if queries
                .insert(id.clone(), (query, snapshot.producer_version.clone()))
                .is_some()
            {
                return Err(Error::InvalidArgument(format!(
                    "query journal snapshot contains duplicate query {id}"
                )));
            }
        }
    }

    let mut physical_previous: Option<u64> = None;
    let mut last_seq = snapshot_seq;
    let mut events_since_snapshot = 0_u64;
    for bytes in records {
        let event = codec::decode_event(&bytes)?;
        if let Some(previous) = physical_previous {
            if previous.checked_add(1) != Some(event.seq) {
                return Err(Error::InvalidArgument(format!(
                    "query journal sequence gap after {previous}"
                )));
            }
        } else if snapshot_seq == 0 && event.seq != 1 {
            return Err(Error::InvalidArgument(
                "query journal must begin at sequence 1 without a snapshot".into(),
            ));
        } else if snapshot_seq > 0 && event.seq > snapshot_seq.saturating_add(1) {
            return Err(Error::InvalidArgument(format!(
                "query journal begins after snapshot sequence {snapshot_seq}"
            )));
        }
        physical_previous = Some(event.seq);
        if event.seq <= snapshot_seq {
            continue;
        }
        if event.seq != last_seq.saturating_add(1) {
            return Err(Error::InvalidArgument(format!(
                "query journal cannot replay sequence {} after {last_seq}",
                event.seq
            )));
        }
        apply(&mut queries, event.kind, &event.producer_version)?;
        last_seq = event.seq;
        events_since_snapshot = events_since_snapshot.saturating_add(1);
    }

    let mut normalized = Vec::new();
    for (query, _producer) in queries.values() {
        if matches!(query.state, QueryState::Queued | QueryState::Running) {
            normalized.push(query.interrupt_for_restart());
        }
    }
    Ok(Recovered {
        queries: queries
            .into_iter()
            .map(|(id, (query, _))| (id, query))
            .collect(),
        last_seq,
        events_since_snapshot,
        normalized,
    })
}

fn apply(
    queries: &mut BTreeMap<String, (PersistedQuery, String)>,
    kind: EventKind,
    producer_version: &str,
) -> Result<()> {
    match kind {
        EventKind::Upsert { query } => {
            if queries.get(&query.query_id).is_some_and(|(current, _)| {
                super::terminal_state(current.state) && !super::terminal_state(query.state)
            }) {
                return Err(Error::InvalidArgument(format!(
                    "query journal contains terminal state regression for {}",
                    query.query_id
                )));
            }
            queries.insert(
                query.query_id.clone(),
                (*query, producer_version.to_owned()),
            );
        }
        EventKind::Delete {
            query_id,
            reason,
            deleted_at_ms,
        } => {
            let _ = (reason, deleted_at_ms);
            queries.remove(&query_id);
        }
    }
    Ok(())
}

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

use super::{DeleteReason, PersistedQuery};

pub(super) const FORMAT_EPOCH: u32 = 2;
pub(super) const MAX_EVENT_BYTES: usize = 512 * 1024;
pub(super) const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Event {
    pub(super) format_epoch: u32,
    pub(super) producer_version: String,
    pub(super) seq: u64,
    pub(super) kind: EventKind,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum EventKind {
    Upsert {
        query: Box<PersistedQuery>,
    },
    Delete {
        query_id: String,
        reason: DeleteReason,
        deleted_at_ms: u64,
    },
}

impl Event {
    pub(super) fn upsert(producer_version: &str, seq: u64, query: PersistedQuery) -> Self {
        Self {
            format_epoch: FORMAT_EPOCH,
            producer_version: producer_version.to_owned(),
            seq,
            kind: EventKind::Upsert {
                query: Box::new(query),
            },
        }
    }

    pub(super) fn delete(
        producer_version: &str,
        seq: u64,
        query_id: &str,
        reason: DeleteReason,
        deleted_at_ms: u64,
    ) -> Self {
        Self {
            format_epoch: FORMAT_EPOCH,
            producer_version: producer_version.to_owned(),
            seq,
            kind: EventKind::Delete {
                query_id: query_id.to_owned(),
                reason,
                deleted_at_ms,
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Snapshot {
    pub(super) format_epoch: u32,
    pub(super) producer_version: String,
    pub(super) last_seq: u64,
    pub(super) queries: Vec<PersistedQuery>,
}

impl Snapshot {
    pub(super) fn new(producer_version: &str, last_seq: u64, queries: Vec<PersistedQuery>) -> Self {
        Self {
            format_epoch: FORMAT_EPOCH,
            producer_version: producer_version.to_owned(),
            last_seq,
            queries,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    payload: T,
    sha256: String,
}

#[derive(Serialize)]
struct EnvelopeRef<'a, T> {
    payload: &'a T,
    sha256: &'a str,
}

pub(super) fn encode_event(event: &Event) -> Result<Vec<u8>> {
    validate_event(event)?;
    let mut bytes = encode_envelope(event, MAX_EVENT_BYTES, "query journal event")?;
    bytes.push(b'\n');
    if bytes.len() > MAX_EVENT_BYTES {
        return Err(Error::ResourceExhausted(
            "query journal event exceeds its size limit".into(),
        ));
    }
    Ok(bytes)
}

pub(super) fn decode_event(bytes: &[u8]) -> Result<Event> {
    let event = decode_envelope(bytes, MAX_EVENT_BYTES, "query journal event")?;
    validate_event(&event)?;
    Ok(event)
}

pub(super) fn encode_snapshot(snapshot: &Snapshot) -> Result<Vec<u8>> {
    validate_snapshot(snapshot)?;
    encode_envelope(snapshot, MAX_SNAPSHOT_BYTES, "query journal snapshot")
}

pub(super) fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot> {
    let snapshot = decode_envelope(bytes, MAX_SNAPSHOT_BYTES, "query journal snapshot")?;
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn encode_envelope<T: Serialize>(value: &T, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| Error::Internal(format!("failed to encode {label}: {error}")))?;
    let sha256 = format!("{:x}", Sha256::digest(&payload));
    let bytes = serde_json::to_vec(&EnvelopeRef {
        payload: value,
        sha256: &sha256,
    })
    .map_err(|error| Error::Internal(format!("failed to encode {label}: {error}")))?;
    if bytes.len() > maximum {
        return Err(Error::ResourceExhausted(format!(
            "{label} exceeds its size limit"
        )));
    }
    Ok(bytes)
}

fn decode_envelope<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    maximum: usize,
    label: &str,
) -> Result<T> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(Error::InvalidArgument(format!(
            "{label} has an invalid size"
        )));
    }
    let envelope: Envelope<T> = serde_json::from_slice(bytes)
        .map_err(|error| Error::InvalidArgument(format!("invalid {label}: {error}")))?;
    let payload = serde_json::to_vec(&envelope.payload)
        .map_err(|error| Error::Internal(format!("failed to verify {label}: {error}")))?;
    let actual = format!("{:x}", Sha256::digest(&payload));
    if envelope.sha256 != actual {
        return Err(Error::InvalidArgument(format!("{label} checksum mismatch")));
    }
    Ok(envelope.payload)
}

fn validate_event(event: &Event) -> Result<()> {
    if event.format_epoch != FORMAT_EPOCH {
        return Err(Error::InvalidArgument(format!(
            "unsupported query journal event epoch {}",
            event.format_epoch
        )));
    }
    if event.seq == 0 || event.producer_version.is_empty() || event.producer_version.len() > 128 {
        return Err(Error::InvalidArgument(
            "query journal event metadata is invalid".into(),
        ));
    }
    match &event.kind {
        EventKind::Upsert { query } => query.validate(),
        EventKind::Delete {
            query_id,
            deleted_at_ms,
            ..
        } if super::valid_query_id(query_id) && *deleted_at_ms > 0 => Ok(()),
        EventKind::Delete { .. } => Err(Error::InvalidArgument(
            "query journal delete event is invalid".into(),
        )),
    }
}

fn validate_snapshot(snapshot: &Snapshot) -> Result<()> {
    if snapshot.format_epoch != FORMAT_EPOCH {
        return Err(Error::InvalidArgument(format!(
            "unsupported query journal snapshot epoch {}",
            snapshot.format_epoch
        )));
    }
    if snapshot.producer_version.is_empty() || snapshot.producer_version.len() > 128 {
        return Err(Error::InvalidArgument(
            "query journal snapshot producer is invalid".into(),
        ));
    }
    for query in &snapshot.queries {
        query.validate()?;
    }
    Ok(())
}

pub use rustdb::{Error, Result, RetryClass};

#[path = "../src/http_shell/service_io.rs"]
pub(crate) mod service_io;

mod http_shell {
    pub(crate) use crate::service_io;
    pub use rustdb::http_shell::{ErrorBody, HttpQueryMetrics, QueryState};

    pub mod security {
        pub use rustdb::http_shell::security::QueryOwner;
    }
}

#[allow(dead_code)]
#[path = "../src/http_shell/query/journal.rs"]
mod journal;

use std::fs;

use http_shell::{HttpQueryMetrics, QueryState, security::QueryOwner};
use journal::{
    DeleteReason, PersistedQuery, QueryJournal, QueryJournalConfig, scoped_idempotency_digest,
};
use rustdb::http_shell::security::PrincipalId;

fn owner() -> QueryOwner {
    QueryOwner::Principal(PrincipalId::new("analyst@example.test").unwrap())
}

fn query(id: &str, state: QueryState, digest: String) -> PersistedQuery {
    let (started_at_ms, finished_at_ms, result_available) = match state {
        QueryState::Queued => (None, None, false),
        QueryState::Running => (Some(2), None, false),
        QueryState::Succeeded => (Some(2), Some(3), true),
        QueryState::Failed | QueryState::Cancelled => unreachable!(),
        _ => unreachable!("test fixture only covers the current v1 query states"),
    };
    PersistedQuery {
        query_id: id.to_owned(),
        owner: owner(),
        request_hash: "a".repeat(64),
        scoped_idempotency_digest: digest,
        state,
        created_at_ms: 1,
        started_at_ms,
        finished_at_ms,
        error: None,
        metrics: (state == QueryState::Succeeded).then(HttpQueryMetrics::default),
        result_available,
    }
}

#[test]
fn completed_query_survives_compaction_without_persisting_secrets() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("journal");
    let secret_key = "private-idempotency-key-123";
    let digest = scoped_idempotency_digest(&owner(), secret_key).unwrap();
    let mut config = QueryJournalConfig::new(&root);
    config.compact_after_events = 2;
    {
        let journal = QueryJournal::open(config.clone()).unwrap();
        journal
            .upsert(query("completed", QueryState::Succeeded, digest))
            .unwrap();
        journal.compact().unwrap();
    }

    let persisted = ["snapshot.json", "journal.jsonl"]
        .into_iter()
        .flat_map(|name| fs::read(root.join(name)).unwrap())
        .collect::<Vec<_>>();
    let text = String::from_utf8_lossy(&persisted);
    assert!(!text.contains(secret_key));
    assert!(!text.contains("select secret_column"));
    assert!(!text.contains("Bearer "));

    let journal = QueryJournal::open(config).unwrap();
    let loaded = journal.load();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].state, QueryState::Succeeded);
    assert!(loaded[0].result_available);
    assert_eq!(loaded[0].metrics.as_ref().unwrap().rows_returned, 0);

    #[cfg(unix)]
    for name in ["OWNER", ".lock", "snapshot.json", "journal.jsonl"] {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(root.join(name)).unwrap().permissions().mode() & 0o077,
            0
        );
    }
}

#[test]
fn restart_interrupts_active_queries_and_persists_the_transition_once() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("journal");
    let config = QueryJournalConfig::new(&root);
    let digest = scoped_idempotency_digest(&owner(), "active-query-key").unwrap();
    {
        let journal = QueryJournal::open(config.clone()).unwrap();
        journal
            .upsert(query("queued", QueryState::Queued, digest.clone()))
            .unwrap();
        journal
            .upsert(query("running", QueryState::Running, digest))
            .unwrap();
    }
    let first_length;
    {
        let journal = QueryJournal::open(config.clone()).unwrap();
        let loaded = journal.load();
        assert!(
            loaded
                .iter()
                .all(|query| query.state == QueryState::Interrupted)
        );
        assert!(loaded.iter().all(|query| {
            query.error.as_ref().unwrap().error == "query.interrupted"
                && query.error.as_ref().unwrap().request_id.is_none()
        }));
        first_length = fs::metadata(root.join("journal.jsonl")).unwrap().len();
    }
    let journal = QueryJournal::open(config).unwrap();
    assert_eq!(
        fs::metadata(root.join("journal.jsonl")).unwrap().len(),
        first_length
    );
    assert!(
        journal
            .load()
            .iter()
            .all(|query| query.state == QueryState::Interrupted)
    );
}

#[test]
fn producer_version_is_diagnostic_within_the_journal_epoch() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("journal");
    let config = QueryJournalConfig::new(&root);
    let digest = scoped_idempotency_digest(&owner(), "versioned-query-key").unwrap();
    {
        let journal = QueryJournal::open_with_producer(config.clone(), "old-beta").unwrap();
        journal
            .upsert(query("old", QueryState::Succeeded, digest))
            .unwrap();
        journal.compact().unwrap();
    }
    {
        let journal = QueryJournal::open_with_producer(config.clone(), "new-beta").unwrap();
        let query = journal.load().pop().unwrap();
        assert_eq!(query.state, QueryState::Succeeded);
        assert!(query.result_available);
        assert!(query.error.is_none());
    }
    let journal = QueryJournal::open_with_producer(config, "new-beta").unwrap();
    assert_eq!(journal.load()[0].state, QueryState::Succeeded);
}

#[test]
fn terminal_query_cannot_regress_to_running() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("journal");
    let journal = QueryJournal::open(QueryJournalConfig::new(&root)).unwrap();
    let digest = scoped_idempotency_digest(&owner(), "terminal-regression-key").unwrap();
    journal
        .upsert(query("stable", QueryState::Succeeded, digest.clone()))
        .unwrap();
    let error = journal
        .upsert(query("stable", QueryState::Running, digest))
        .unwrap_err();
    assert!(error.to_string().contains("terminal state regression"));
    assert_eq!(journal.load()[0].state, QueryState::Succeeded);
}

#[test]
fn explicit_and_ttl_deletes_are_durable() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("journal");
    let config = QueryJournalConfig::new(&root);
    let digest = scoped_idempotency_digest(&owner(), "delete-query-key").unwrap();
    {
        let journal = QueryJournal::open(config.clone()).unwrap();
        journal
            .upsert(query("explicit", QueryState::Succeeded, digest.clone()))
            .unwrap();
        journal
            .upsert(query("ttl", QueryState::Succeeded, digest))
            .unwrap();
        assert!(journal.delete("explicit", DeleteReason::Explicit).unwrap());
        assert!(journal.delete("ttl", DeleteReason::Ttl).unwrap());
        assert!(!journal.delete("missing", DeleteReason::Explicit).unwrap());
        journal.compact().unwrap();
    }
    assert!(QueryJournal::open(config).unwrap().load().is_empty());
}

#[test]
fn truncated_tail_is_discarded_but_checksum_corruption_is_rejected() {
    use std::io::Write;

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("journal");
    let config = QueryJournalConfig::new(&root);
    let digest = scoped_idempotency_digest(&owner(), "crash-query-key").unwrap();
    {
        let journal = QueryJournal::open(config.clone()).unwrap();
        journal
            .upsert(query("safe", QueryState::Succeeded, digest))
            .unwrap();
    }
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(root.join("journal.jsonl"))
            .unwrap();
        file.write_all(br#"{"partial""#).unwrap();
        file.sync_all().unwrap();
    }
    {
        let journal = QueryJournal::open(config.clone()).unwrap();
        assert_eq!(journal.load().len(), 1);
    }

    let path = root.join("journal.jsonl");
    let mut line = fs::read(&path).unwrap();
    let checksum = line
        .windows(b"\"sha256\":\"".len())
        .position(|window| window == b"\"sha256\":\"")
        .unwrap()
        + b"\"sha256\":\"".len();
    line[checksum] = if line[checksum] == b'a' { b'b' } else { b'a' };
    fs::write(&path, line).unwrap();
    let error = match QueryJournal::open(config) {
        Ok(_) => panic!("checksum corruption must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("checksum mismatch"));
}

#[test]
fn new_snapshot_can_replay_the_old_journal_after_compaction_crash() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("journal");
    let config = QueryJournalConfig::new(&root);
    let digest = scoped_idempotency_digest(&owner(), "compact-query-key").unwrap();
    {
        let journal = QueryJournal::open(config.clone()).unwrap();
        journal
            .upsert(query("kept", QueryState::Succeeded, digest))
            .unwrap();
        let old_journal = fs::read(root.join("journal.jsonl")).unwrap();
        journal.compact().unwrap();
        fs::write(root.join("journal.jsonl"), old_journal).unwrap();
    }
    let journal = QueryJournal::open(config).unwrap();
    assert_eq!(journal.load().len(), 1);
    assert_eq!(journal.load()[0].query_id, "kept");
}

#[cfg(unix)]
#[test]
fn journal_refuses_dangling_symlink_files() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("journal");
    {
        let journal = QueryJournal::open(QueryJournalConfig::new(&root)).unwrap();
        drop(journal);
    }
    fs::remove_file(root.join("journal.jsonl")).unwrap();
    symlink(
        temporary.path().join("outside-journal"),
        root.join("journal.jsonl"),
    )
    .unwrap();

    let error = match QueryJournal::open(QueryJournalConfig::new(&root)) {
        Ok(_) => panic!("a symlinked query journal must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("not a regular file"));
    assert!(!temporary.path().join("outside-journal").exists());
}

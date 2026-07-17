use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlparser::{dialect::DuckDbDialect, parser::Parser};

use crate::{
    disk::{StorageSampler, common_storage_root},
    protocol::SetupResponse,
    rss::RssSampler,
    source::{SourceFile, validate_manifest, verify_files},
};
use rustdb::Session;

const MARKER_VERSION: u32 = 1;
const MARKER_SUFFIX: &str = ".rustdb-v07-setup.json";

pub(crate) struct SetupRequest {
    pub(crate) setup_id: String,
    pub(crate) storage_track: String,
    pub(crate) source_sha256: String,
    pub(crate) source_bytes: u64,
    pub(crate) source_files: Vec<SourceFile>,
    pub(crate) statements_sha256: String,
    pub(crate) statements: Vec<String>,
    pub(crate) max_storage_bytes: u64,
}

pub(crate) struct SetupState {
    database: Option<PathBuf>,
    marker_path: Option<PathBuf>,
    temporary_path: Option<PathBuf>,
    marker: Option<SetupMarker>,
    partial: bool,
    attempted: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SetupMarker {
    format_version: u32,
    setup_id: String,
    storage_track: String,
    source_sha256: String,
    source_bytes: u64,
    statements_sha256: String,
    table_count: usize,
    tables: Vec<String>,
}

#[derive(Serialize)]
struct SetupIdentity<'a> {
    source_sha256: &'a str,
    statements_sha256: &'a str,
}

impl SetupState {
    pub(crate) fn load(database: Option<&Path>, session: &Session) -> Result<Self, String> {
        let Some(database) = database else {
            return Ok(Self {
                database: None,
                marker_path: None,
                temporary_path: None,
                marker: None,
                partial: false,
                attempted: false,
            });
        };
        let database = fs::canonicalize(database)
            .map_err(|_| "cannot resolve native database path".to_owned())?;
        let marker_path = marker_path(&database)?;
        let temporary_path = append_suffix(&marker_path, ".tmp")?;
        let partial = temporary_path.exists();
        let marker = if marker_path.exists() {
            let bytes =
                fs::read(&marker_path).map_err(|_| "cannot read native setup marker".to_owned())?;
            let marker: SetupMarker = serde_json::from_slice(&bytes)
                .map_err(|_| "native setup marker is invalid".to_owned())?;
            marker.validate()?;
            if marker.tables != table_names(session) {
                return Err("native setup marker does not match the catalog".to_owned());
            }
            Some(marker)
        } else {
            None
        };
        Ok(Self {
            database: Some(database),
            marker_path: Some(marker_path),
            temporary_path: Some(temporary_path),
            marker,
            partial,
            attempted: false,
        })
    }

    pub(crate) fn run_setup_id(
        &self,
        session: &Session,
        storage_track: &str,
        expected_setup_id: Option<&str>,
    ) -> Result<Option<String>, String> {
        if storage_track != "native" {
            if expected_setup_id.is_some() {
                return Err("setup_id is valid only for the native storage track".to_owned());
            }
            return Ok(None);
        }
        if self.database.is_none() {
            return Err("native queries require --database".to_owned());
        }
        if self.partial {
            return Err("native database has an incomplete setup marker".to_owned());
        }
        let marker = self
            .marker
            .as_ref()
            .ok_or_else(|| "native database has no completed setup marker".to_owned())?;
        let expected =
            expected_setup_id.ok_or_else(|| "native query requires setup_id".to_owned())?;
        if expected != marker.setup_id {
            return Err("native query setup_id does not match the database".to_owned());
        }
        if marker.tables != table_names(session) {
            return Err("native setup marker does not match the catalog".to_owned());
        }
        Ok(Some(marker.setup_id.clone()))
    }

    pub(crate) async fn execute(
        &mut self,
        session: &Session,
        spill_directory: &Path,
        memory_limit: usize,
        request: SetupRequest,
    ) -> Result<SetupResponse, String> {
        if self.attempted {
            return Err("a setup command was already attempted in this worker".to_owned());
        }
        self.attempted = true;
        let database = self
            .database
            .as_ref()
            .ok_or_else(|| "setup requires --database".to_owned())?;
        let marker_path = self.marker_path.as_ref().expect("database marker path");
        let temporary_path = self
            .temporary_path
            .as_ref()
            .expect("database temporary marker path");
        if self.marker.is_some() || self.partial || marker_path.exists() || temporary_path.exists()
        {
            return Err("native database setup cannot be reused".to_owned());
        }
        if !session.table_names().is_empty() {
            return Err("native database catalog is not empty".to_owned());
        }
        request.validate()?;
        for statement in &request.statements {
            validate_ctas(statement)?;
        }
        validate_statement_sources(&request.statements, &request.source_files)?;

        let spill_directory = fs::canonicalize(spill_directory)
            .map_err(|_| "cannot resolve spill directory".to_owned())?;
        let storage_root = common_storage_root(&[database, &spill_directory, marker_path])?;
        let storage = StorageSampler::start(storage_root, request.max_storage_bytes)?;
        let rss = RssSampler::start();
        verify_files(&request.source_files)?;
        let started = Instant::now();
        let mut load_elapsed_ms = None;

        for (index, statement) in request.statements.iter().enumerate() {
            storage.check()?;
            consume_ctas(session, statement, index, &storage).await?;
            if index + 1 == request.statements.len() {
                load_elapsed_ms =
                    Some((started.elapsed().as_secs_f64() * 1_000.0).max(f64::EPSILON));
            }
            let expected_count = index + 1;
            if session.table_names().len() != expected_count {
                return Err(format!(
                    "native setup statement {} did not commit exactly one table",
                    expected_count
                ));
            }
        }
        let load_elapsed_ms = load_elapsed_ms.expect("setup has at least one statement");
        verify_files(&request.source_files)?;
        let tables = table_names(session);
        if tables.len() != request.statements.len() {
            return Err("native setup table count is incomplete".to_owned());
        }
        let marker = SetupMarker::new(&request, tables);
        if let Err(error) = persist_marker(marker_path, temporary_path, &marker) {
            self.partial = true;
            return fail_after_marker(marker_path, &error);
        }

        let storage_report = match storage.stop() {
            Ok(report)
                if report.baseline_bytes <= report.final_bytes
                    && report.final_bytes <= report.peak_bytes =>
            {
                report
            }
            Ok(_) => {
                return fail_after_marker(
                    marker_path,
                    "native setup storage accounting is invalid",
                );
            }
            Err(error) => return fail_after_marker(marker_path, &error),
        };
        let (rss_baseline_bytes, peak_rss_bytes) = rss.stop();
        if rss_baseline_bytes == 0 || peak_rss_bytes < rss_baseline_bytes {
            return fail_after_marker(marker_path, "native setup RSS accounting is invalid");
        }
        if peak_rss_bytes > memory_limit as u64 {
            return fail_after_marker(
                marker_path,
                "native setup RSS exceeded the configured limit",
            );
        }
        self.marker = Some(marker.clone());
        Ok(SetupResponse {
            kind: "setup",
            engine: "rustdb",
            setup_id: marker.setup_id,
            complete: true,
            load_elapsed_ms,
            rss_baseline_bytes,
            peak_rss_bytes,
            storage_baseline_bytes: storage_report.baseline_bytes,
            storage_peak_bytes: storage_report.peak_bytes,
            storage_final_bytes: storage_report.final_bytes,
            table_count: marker.table_count,
        })
    }
}

impl SetupMarker {
    fn new(request: &SetupRequest, tables: Vec<String>) -> Self {
        Self {
            format_version: MARKER_VERSION,
            setup_id: request.setup_id.clone(),
            storage_track: request.storage_track.clone(),
            source_sha256: request.source_sha256.clone(),
            source_bytes: request.source_bytes,
            statements_sha256: request.statements_sha256.clone(),
            table_count: tables.len(),
            tables,
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.format_version != MARKER_VERSION
            || self.storage_track != "native"
            || self.source_bytes == 0
            || self.table_count == 0
            || self.table_count != self.tables.len()
            || !is_sha256(&self.setup_id)
            || !is_sha256(&self.source_sha256)
            || !is_sha256(&self.statements_sha256)
        {
            return Err("native setup marker is invalid".to_owned());
        }
        let mut tables = self.tables.clone();
        tables.sort();
        tables.dedup();
        if tables != self.tables {
            return Err("native setup marker table list is invalid".to_owned());
        }
        let identity = setup_identity(&self.source_sha256, &self.statements_sha256)?;
        if identity != self.setup_id {
            return Err("native setup marker identity is invalid".to_owned());
        }
        Ok(())
    }
}

impl SetupRequest {
    fn validate(&self) -> Result<(), String> {
        if self.storage_track != "native" {
            return Err("setup supports only the native storage track".to_owned());
        }
        if !is_sha256(&self.setup_id)
            || !is_sha256(&self.source_sha256)
            || !is_sha256(&self.statements_sha256)
        {
            return Err("setup hashes must be lowercase SHA-256 values".to_owned());
        }
        if self.source_bytes == 0 || self.max_storage_bytes == 0 || self.statements.is_empty() {
            return Err("setup sizes and statements must be non-empty".to_owned());
        }
        validate_manifest(&self.source_files, self.source_bytes, &self.source_sha256)?;
        let encoded = serde_json::to_vec(&self.statements)
            .map_err(|_| "cannot encode native setup statements".to_owned())?;
        if digest(&encoded) != self.statements_sha256 {
            return Err("native setup statement hash does not match".to_owned());
        }
        if setup_identity(&self.source_sha256, &self.statements_sha256)? != self.setup_id {
            return Err("native setup identity does not match".to_owned());
        }
        Ok(())
    }
}

async fn consume_ctas(
    session: &Session,
    statement: &str,
    index: usize,
    storage: &StorageSampler,
) -> Result<(), String> {
    let mut result = session
        .execute(statement)
        .await
        .map_err(|_| format!("native setup statement {} failed", index + 1))?;
    let cancellation = result.cancellation_handle();
    loop {
        tokio::select! {
            batch = result.stream().next() => match batch {
                Some(Ok(_)) => {}
                Some(Err(_)) => return Err(format!("native setup statement {} failed", index + 1)),
                None => return Ok(()),
            },
            () = tokio::time::sleep(Duration::from_millis(2)) => {
                if let Err(error) = storage.check() {
                    cancellation.cancel();
                    return Err(error);
                }
            }
        }
    }
}

fn validate_ctas(sql: &str) -> Result<(), String> {
    let statements = Parser::parse_sql(&DuckDbDialect {}, sql)
        .map_err(|_| "native setup contains an invalid SQL statement".to_owned())?;
    if !matches!(statements.as_slice(), [sqlparser::ast::Statement::CreateTable(create)] if create.query.is_some())
    {
        return Err("native setup statements must be one CREATE TABLE AS SELECT".to_owned());
    }
    if has_glob_literal(sql) {
        return Err("native setup CTAS must use explicit source files".to_owned());
    }
    Ok(())
}

fn has_glob_literal(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut quoted = false;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            if quoted && bytes.get(index + 1) == Some(&b'\'') {
                index += 2;
                continue;
            }
            quoted = !quoted;
        } else if quoted && matches!(bytes[index], b'*' | b'?' | b'[' | b']') {
            return true;
        }
        index += 1;
    }
    false
}

fn validate_statement_sources(statements: &[String], sources: &[SourceFile]) -> Result<(), String> {
    let mut actual = Vec::new();
    for statement in statements {
        actual.extend(parquet_locations(statement)?);
    }
    let mut expected = sources
        .iter()
        .map(|source| source.location.clone())
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    if actual != expected {
        return Err("native setup CTAS sources do not match the source manifest".to_owned());
    }
    Ok(())
}

fn parquet_locations(sql: &str) -> Result<Vec<String>, String> {
    const FUNCTION: &[u8] = b"read_parquet";
    let lower = sql.to_ascii_lowercase();
    let lower = lower.as_bytes();
    let original = sql.as_bytes();
    let mut locations = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = lower[cursor..]
        .windows(FUNCTION.len())
        .position(|window| window == FUNCTION)
    {
        let mut index = cursor + offset + FUNCTION.len();
        if original.get(index..index + 2) != Some(b"('") {
            return Err("native setup read_parquet source must be one literal".to_owned());
        }
        index += 2;
        let mut location = Vec::new();
        loop {
            let byte = *original
                .get(index)
                .ok_or_else(|| "native setup read_parquet source is invalid".to_owned())?;
            if byte == b'\'' {
                if original.get(index + 1) == Some(&b'\'') {
                    location.push(b'\'');
                    index += 2;
                    continue;
                }
                index += 1;
                break;
            }
            location.push(byte);
            index += 1;
        }
        if original.get(index) != Some(&b')') {
            return Err("native setup read_parquet source must be one literal".to_owned());
        }
        locations.push(
            String::from_utf8(location)
                .map_err(|_| "native setup read_parquet source is invalid".to_owned())?,
        );
        cursor = index + 1;
    }
    Ok(locations)
}

fn setup_identity(source_sha256: &str, statements_sha256: &str) -> Result<String, String> {
    let encoded = serde_json::to_vec(&SetupIdentity {
        source_sha256,
        statements_sha256,
    })
    .map_err(|_| "cannot encode native setup identity".to_owned())?;
    Ok(digest(&encoded))
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn table_names(session: &Session) -> Vec<String> {
    let mut tables = session.table_names();
    tables.sort();
    tables
}

fn marker_path(database: &Path) -> Result<PathBuf, String> {
    append_suffix(database, MARKER_SUFFIX)
}

fn append_suffix(path: &Path, suffix: &str) -> Result<PathBuf, String> {
    let name = path
        .file_name()
        .ok_or_else(|| "native database path has no file name".to_owned())?;
    let mut name = name.to_os_string();
    name.push(suffix);
    Ok(path.with_file_name(name))
}

fn persist_marker(path: &Path, temporary: &Path, marker: &SetupMarker) -> Result<(), String> {
    if path.exists() || temporary.exists() {
        return Err("native setup marker already exists".to_owned());
    }
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(temporary)
        .map_err(|_| "cannot create native setup marker".to_owned())?;
    serde_json::to_writer(&mut file, marker)
        .map_err(|_| "cannot encode native setup marker".to_owned())?;
    file.write_all(b"\n")
        .and_then(|_| file.sync_all())
        .map_err(|_| "cannot persist native setup marker".to_owned())?;
    fs::rename(temporary, path).map_err(|_| "cannot commit native setup marker".to_owned())?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| "cannot sync native setup marker directory".to_owned())?;
    }
    Ok(())
}

fn fail_after_marker<T>(path: &Path, message: &str) -> Result<T, String> {
    let temporary = append_suffix(path, ".tmp");
    let poisoned = temporary
        .as_ref()
        .ok()
        .and_then(|temporary| {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(temporary)
                .ok()
        })
        .and_then(|mut file| {
            file.write_all(b"incomplete\n")
                .and_then(|_| file.sync_all())
                .ok()
        })
        .is_some();
    match fs::remove_file(path) {
        Ok(()) => Err(message.to_owned()),
        Err(error) if error.kind() == ErrorKind::NotFound => Err(message.to_owned()),
        Err(_) if poisoned => Err(message.to_owned()),
        Err(_) => Err(format!("{message}; cannot invalidate failed setup marker")),
    }
}

#[cfg(test)]
mod tests {
    use super::{SetupRequest, digest, setup_identity, validate_ctas};
    use crate::source::SourceFile;

    #[test]
    fn hashes_compact_json_like_the_coordinator() {
        let statements = vec!["CREATE TABLE t AS SELECT '数据' AS value".to_owned()];
        let statements_sha256 = digest(&serde_json::to_vec(&statements).unwrap());
        assert_eq!(
            statements_sha256,
            "7ad793c3fbe79e40d7365c6e53830930b684a4b5afb0f9197dcf56b9682a3a27"
        );
        let source_sha256 =
            "8a4f62878ec3b580c780d8709b855b320720a8c3bea1175aae44f6977b405567".to_owned();
        let request = SetupRequest {
            setup_id: setup_identity(&source_sha256, &statements_sha256).unwrap(),
            storage_track: "native".to_owned(),
            source_sha256,
            source_bytes: 3,
            source_files: vec![SourceFile {
                relative_path: "data/t.parquet".to_owned(),
                location: "/bench-data/data/t.parquet".to_owned(),
                bytes: 3,
                sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                    .to_owned(),
            }],
            statements_sha256,
            statements,
            max_storage_bytes: 1,
        };
        assert_eq!(
            request.setup_id,
            "4456ef5ae3bbc9d1e06d4d1cde2db71da05a166d7f1cbc780bd9ae5eb773fda0"
        );
        request.validate().unwrap();
    }

    #[test]
    fn ctas_allows_select_star_but_rejects_glob_literals() {
        assert!(
            validate_ctas("CREATE TABLE t AS SELECT * FROM read_parquet('/data/t.parquet')")
                .is_ok()
        );
        assert!(
            validate_ctas("CREATE TABLE t AS SELECT * FROM read_parquet('/data/*.parquet')")
                .is_err()
        );
    }
}

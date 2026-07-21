use std::io::{self, Write};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use rustdb::{
    Error, Result,
    http_shell::{
        ArrowResultPoll, ArrowResultState, QueryRequest, QueryState, RemoteClient,
        security::{default_profile_root, load_profile},
    },
};

use super::operations::ShellArgs;

pub(super) async fn run(args: &ShellArgs) -> Result<()> {
    let profile = load_profile(default_profile_root()?, &args.profile)?;
    let client = RemoteClient::from_profile(&profile)?;
    client.check_compatibility().await?;
    match (&args.command, &args.file) {
        (Some(sql), None) => execute_statements(&client, sql, args).await,
        (None, Some(path)) => {
            let sql = tokio::fs::read_to_string(path)
                .await
                .map_err(|error| Error::io(path.clone(), error))?;
            execute_statements(&client, &sql, args).await
        }
        (None, None) => repl(&client, args).await,
        (Some(_), Some(_)) => unreachable!("clap rejects conflicting arguments"),
    }
}

async fn execute_statements(client: &RemoteClient, sql: &str, args: &ShellArgs) -> Result<()> {
    let statements = rustdb::split_sql_statements(sql)?;
    if statements.is_empty() {
        return Err(Error::InvalidArgument("SQL input is empty".into()));
    }
    for statement in statements {
        execute(client, &statement, args).await?;
    }
    Ok(())
}

async fn execute(client: &RemoteClient, sql: &str, args: &ShellArgs) -> Result<()> {
    let accepted = client
        .submit(&QueryRequest {
            sql: sql.to_owned(),
            parameters: Vec::new(),
            timeout_ms: args.timeout_ms,
        })
        .await?;
    let query_id = accepted.query_id;
    if let Err(error) = fetch_arrow_batches(client, &query_id, args).await {
        if matches!(&error, Error::Cancelled) {
            return Err(error);
        }
        if let Ok(status) = client.status(&query_id).await
            && matches!(status.state, QueryState::Failed | QueryState::Cancelled)
        {
            return terminal_status(&status, &query_id);
        }
        return Err(Error::Execution(format!(
            "{error} [query {query_id}; the background query was not cancelled]"
        )));
    }
    let status = wait_or_cancel(client, &query_id).await?;
    terminal_status(&status, &query_id)?;
    if args.metrics
        && let Some(metrics) = status.metrics
    {
        eprintln!(
            "elapsed={}ms rows={} scanned_rows={} scanned_bytes={} peak_memory={} spill_read={} spill_write={}",
            metrics.elapsed_ms,
            metrics.rows_returned,
            metrics.rows_scanned,
            metrics.bytes_scanned,
            metrics.peak_memory_bytes,
            metrics.spill_read_bytes,
            metrics.spill_write_bytes,
        );
    }
    client.delete(&query_id).await.map_err(Into::into)
}

fn terminal_status(status: &rustdb::http_shell::QueryStatusResponse, query_id: &str) -> Result<()> {
    match status.state {
        QueryState::Succeeded => Ok(()),
        QueryState::Cancelled => Err(Error::Cancelled),
        QueryState::Interrupted => Err(Error::Execution(format!(
            "remote query was interrupted before completion [query {query_id}]"
        ))),
        QueryState::Failed => {
            let message = status
                .error
                .as_ref()
                .map(|error| format!("{}: {}", error.error, error.message))
                .unwrap_or_else(|| "remote query failed without an error detail".into());
            Err(Error::Execution(format!("{message} [query {query_id}]")))
        }
        QueryState::Queued | QueryState::Running => Err(Error::Internal(
            "remote wait returned a non-terminal state".into(),
        )),
        _ => Err(Error::Unsupported(
            "the remote server returned a query state unsupported by this CLI".into(),
        )),
    }
}

async fn wait_or_cancel(
    client: &RemoteClient,
    query_id: &str,
) -> Result<rustdb::http_shell::QueryStatusResponse> {
    loop {
        let status = tokio::select! {
            result = client.status(query_id) => result?,
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| Error::io(None, error))?;
                best_effort_cancel(client, query_id).await;
                return Err(Error::Cancelled);
            }
        };
        if matches!(
            status.state,
            QueryState::Succeeded
                | QueryState::Failed
                | QueryState::Cancelled
                | QueryState::Interrupted
        ) {
            return Ok(status);
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| Error::io(None, error))?;
                best_effort_cancel(client, query_id).await;
                return Err(Error::Cancelled);
            }
        }
    }
}

async fn fetch_arrow_batches(
    client: &RemoteClient,
    query_id: &str,
    args: &ShellArgs,
) -> Result<()> {
    let mut batch_seq = 0_u64;
    let mut first = true;
    loop {
        let poll = {
            let request = client.arrow_batch(query_id, batch_seq);
            tokio::pin!(request);
            tokio::select! {
                result = &mut request => result?,
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(|error| Error::io(None, error))?;
                    best_effort_cancel(client, query_id).await;
                    return Err(Error::Cancelled);
                }
            }
        };
        match poll {
            ArrowResultPoll::Batch(value) => {
                super::output::write_batch(
                    &value.batch,
                    args.format,
                    args.csv_null.as_deref(),
                    first,
                )?;
                first = false;
                batch_seq = value.next_batch_seq;
                if value.result_complete {
                    return Ok(());
                }
            }
            ArrowResultPoll::Pending { state, .. } => {
                if matches!(state, ArrowResultState::Queued | ArrowResultState::Running) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                } else {
                    return Err(Error::Execution(format!(
                        "remote result entered terminal state {state:?} before another batch"
                    )));
                }
            }
            ArrowResultPoll::Complete { schema, .. } => {
                if first {
                    super::output::write_batch(
                        &RecordBatch::new_empty(schema),
                        args.format,
                        args.csv_null.as_deref(),
                        true,
                    )?;
                }
                return Ok(());
            }
            _ => {
                return Err(Error::Unsupported(
                    "the remote server returned an Arrow result variant unsupported by this CLI"
                        .into(),
                ));
            }
        }
    }
}

async fn best_effort_cancel(client: &RemoteClient, query_id: &str) {
    let _ = tokio::time::timeout(Duration::from_secs(2), client.cancel(query_id)).await;
}

async fn repl(client: &RemoteClient, args: &ShellArgs) -> Result<()> {
    eprintln!(
        "RustDB remote shell v{}; end SQL with ';'; use .help",
        env!("CARGO_PKG_VERSION")
    );
    let mut pending = String::new();
    loop {
        print!(
            "{}",
            if pending.is_empty() {
                "rustdb> "
            } else {
                "     -> "
            }
        );
        io::stdout()
            .flush()
            .map_err(|error| Error::io(None, error))?;
        let mut line = String::new();
        if io::stdin()
            .read_line(&mut line)
            .map_err(|error| Error::io(None, error))?
            == 0
        {
            return Ok(());
        }
        let trimmed = line.trim();
        if pending.is_empty() && matches!(trimmed, ".quit" | ".exit") {
            return Ok(());
        }
        if pending.is_empty() && trimmed == ".help" {
            eprintln!(".quit    exit the remote shell");
            eprintln!("Queries run synchronously; Ctrl-C cancels the active remote query.");
            continue;
        }
        if pending.is_empty() && trimmed.starts_with('.') {
            eprintln!("unknown command: {trimmed}");
            continue;
        }
        pending.push_str(&line);
        if !trimmed.ends_with(';') {
            continue;
        }
        let sql = std::mem::take(&mut pending);
        if let Err(error) = execute_statements(client, &sql, args).await {
            eprintln!("error: {error}");
        }
    }
}

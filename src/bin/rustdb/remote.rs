use std::io::{self, Write};
use std::time::Duration;

use rustdb::{
    Error, Result,
    http_shell::{
        JsonResultPage, QueryRequest, QueryState, RemoteClient, SchemaColumn,
        security::{default_profile_root, load_profile},
    },
};
use serde_json::Value;

use super::{args::OutputFormat, operations::ShellArgs};

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
    let status = wait_or_cancel(client, &query_id).await?;
    match status.state {
        QueryState::Succeeded => {}
        QueryState::Cancelled => return Err(Error::Cancelled),
        QueryState::Failed => {
            let message = status
                .error
                .map(|error| format!("{}: {}", error.error, error.message))
                .unwrap_or_else(|| "remote query failed without an error detail".into());
            return Err(Error::Execution(format!("{message} [query {query_id}]")));
        }
        QueryState::Queued | QueryState::Running => {
            return Err(Error::Internal(
                "remote wait returned a non-terminal state".into(),
            ));
        }
    }
    let mut renderer = Renderer::new(args.format, args.csv_null.as_deref());
    let render = fetch_pages(client, &query_id, &mut renderer).await;
    render?;
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
    client.delete(&query_id).await
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
            QueryState::Succeeded | QueryState::Failed | QueryState::Cancelled
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

async fn fetch_pages(
    client: &RemoteClient,
    query_id: &str,
    renderer: &mut Renderer<'_>,
) -> Result<()> {
    let mut cursor = None;
    loop {
        let page = {
            let request = client.page(query_id, cursor.as_deref(), 1_000);
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
        renderer.write(&page)?;
        if page.page.complete {
            break;
        }
        cursor = page.page.next_cursor;
        if cursor.is_none() {
            return Err(Error::Internal(
                "remote result page omitted its continuation cursor".into(),
            ));
        }
    }
    Ok(())
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

struct Renderer<'a> {
    format: OutputFormat,
    csv_null: Option<&'a str>,
    schema: Option<Vec<SchemaColumn>>,
}

impl<'a> Renderer<'a> {
    fn new(format: OutputFormat, csv_null: Option<&'a str>) -> Self {
        Self {
            format,
            csv_null,
            schema: None,
        }
    }

    fn write(&mut self, page: &JsonResultPage) -> Result<()> {
        if self.schema.is_none() {
            self.write_header(&page.schema)?;
            self.schema = Some(page.schema.clone());
        } else if self.schema.as_deref() != Some(page.schema.as_slice()) {
            return Err(Error::Execution(
                "remote result schema changed between pages".into(),
            ));
        }
        let schema = self.schema.as_ref().expect("initialized above");
        for row in &page.rows {
            if row.len() != schema.len() {
                return Err(Error::Execution(
                    "remote result row width does not match its schema".into(),
                ));
            }
            match self.format {
                OutputFormat::Table => println!(
                    "{}",
                    row.iter()
                        .map(display_value)
                        .collect::<Vec<_>>()
                        .join(" | ")
                ),
                OutputFormat::Csv => println!(
                    "{}",
                    row.iter()
                        .map(|value| csv_value(value, self.csv_null))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                OutputFormat::Jsonl => println!("{}", json_object(schema, row)?),
            }
        }
        Ok(())
    }

    fn write_header(&self, schema: &[SchemaColumn]) -> Result<()> {
        match self.format {
            OutputFormat::Table => {
                let names = schema
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>();
                println!("{}", names.join(" | "));
                println!(
                    "{}",
                    names
                        .iter()
                        .map(|name| "-".repeat(name.chars().count().max(1)))
                        .collect::<Vec<_>>()
                        .join("-+-")
                );
            }
            OutputFormat::Csv => println!(
                "{}",
                schema
                    .iter()
                    .map(|column| csv_text(&column.name))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            OutputFormat::Jsonl => {}
        }
        Ok(())
    }
}

fn display_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::String(value) => value.clone(),
        value => value.to_string(),
    }
}

fn csv_value(value: &Value, null: Option<&str>) -> String {
    match value {
        Value::Null => csv_text(null.unwrap_or("")),
        Value::String(value) => csv_text(value),
        value => csv_text(&value.to_string()),
    }
}

fn csv_text(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn json_object(schema: &[SchemaColumn], row: &[Value]) -> Result<String> {
    let mut output = String::from("{");
    for (index, (column, value)) in schema.iter().zip(row).enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str(
            &serde_json::to_string(&column.name)
                .map_err(|error| Error::Internal(format!("failed to encode JSON key: {error}")))?,
        );
        output.push(':');
        output.push_str(
            &serde_json::to_string(value).map_err(|error| {
                Error::Internal(format!("failed to encode JSON value: {error}"))
            })?,
        );
    }
    output.push('}');
    Ok(output)
}

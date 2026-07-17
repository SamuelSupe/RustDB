use std::io::{self, Write};

use rustdb::{Error, Result, Session};

use super::{args::OutputFormat, output};

pub async fn run(
    session: &Session,
    format: OutputFormat,
    csv_null: Option<&str>,
    metrics: bool,
) -> Result<()> {
    eprintln!(
        "RustDB v{}; end SQL with ';'; use .help or .help zh",
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
        let read = io::stdin()
            .read_line(&mut line)
            .map_err(|error| Error::io(None, error))?;
        if read == 0 {
            break;
        }
        let trimmed = line.trim();
        if pending.is_empty() && trimmed.starts_with('.') {
            if handle_meta(session, trimmed) {
                break;
            }
            continue;
        }

        pending.push_str(&line);
        if !trimmed.ends_with(';') {
            continue;
        }
        let sql = std::mem::take(&mut pending);
        if let Err(error) =
            super::execute_statements(session, &sql, format, csv_null, metrics).await
        {
            eprintln!("error: {error}");
        }
    }
    Ok(())
}

pub async fn execute(
    session: &Session,
    sql: &str,
    format: OutputFormat,
    csv_null: Option<&str>,
    metrics: bool,
) -> Result<()> {
    let execute = session.execute(sql);
    tokio::pin!(execute);
    let mut result = tokio::select! {
        result = &mut execute => result?,
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|error| Error::io(None, error))?;
            return Err(Error::Cancelled);
        }
    };
    {
        let cancellation = result.cancellation_handle();
        let write = output::write_result(&mut result, format, csv_null);
        tokio::pin!(write);
        tokio::select! {
            result = &mut write => result?,
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| Error::io(None, error))?;
                cancellation.cancel();
                (&mut write).await?;
            }
        }
    }
    if metrics {
        output::print_metrics(&result);
    }
    Ok(())
}

fn handle_meta(session: &Session, command: &str) -> bool {
    match command {
        ".quit" | ".exit" => true,
        ".tables" => {
            for name in session.table_names() {
                println!("{name}");
            }
            false
        }
        ".help" | ".help en" => {
            eprintln!(".tables  list registered external tables");
            eprintln!(".quit    exit the shell");
            eprintln!(".help zh show Chinese help");
            false
        }
        ".help zh" | ".help zh-cn" => {
            eprintln!(".tables  列出已注册的外部表");
            eprintln!(".quit    退出交互终端");
            eprintln!(".help    显示英文帮助");
            false
        }
        _ => {
            eprintln!("unknown command: {command}");
            false
        }
    }
}

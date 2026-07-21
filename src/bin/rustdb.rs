#[path = "rustdb/admin.rs"]
mod admin;
#[path = "rustdb/args.rs"]
mod args;
#[path = "rustdb/config.rs"]
mod config;
#[path = "rustdb/diagnostics.rs"]
mod diagnostics;
#[path = "rustdb/operations.rs"]
mod operations;
#[path = "rustdb/output.rs"]
mod output;
#[path = "rustdb/remote.rs"]
mod remote;
#[path = "rustdb/repl.rs"]
mod repl;
#[path = "rustdb/server_config.rs"]
mod server_config;
#[path = "rustdb/service_admin.rs"]
mod service_admin;
#[path = "rustdb/sql_input.rs"]
mod sql_input;

use clap::Parser;

use args::{Args, LogFormatArg, Operation};
use rustdb::{Engine, EngineConfig, Error, Result};

fn main() {
    let args = Args::parse();
    init_logging(args.log_format);
    if args.help_zh {
        print!("{}", args::HELP_ZH);
        return;
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: failed to create runtime: {error}");
            std::process::exit(1);
        }
    };

    if let Err(error) = runtime.block_on(run(args)) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn init_logging(format: LogFormatArg) {
    let filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "warn,rustdb::http_shell=info".into())
    };
    match format {
        LogFormatArg::Text => tracing_subscriber::fmt()
            .with_env_filter(filter())
            .with_writer(std::io::stderr)
            .init(),
        LogFormatArg::Json => tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_env_filter(filter())
            .with_writer(std::io::stderr)
            .init(),
    }
}

async fn run(args: Args) -> Result<()> {
    if let Some(operation) = &args.operation {
        ensure_standalone_operation(&args)?;
        return run_operation(operation, &args, config::engine_config(&args)).await;
    }

    let config = config::engine_config(&args);

    let engine = match &args.database {
        Some(path) => Engine::open(path, config)?,
        None => Engine::new(config)?,
    };
    let session = engine.session();
    let csv_null = args.csv_null.as_deref();
    match (args.command, args.file) {
        (Some(sql), None) => {
            execute_statements(&session, &sql, args.format, csv_null, args.metrics).await
        }
        (None, Some(path)) => {
            let sql = tokio::fs::read_to_string(&path)
                .await
                .map_err(|error| Error::io(Some(path), error))?;
            execute_statements(&session, &sql, args.format, csv_null, args.metrics).await
        }
        (None, None) => repl::run(&session, args.format, csv_null, args.metrics).await,
        (Some(_), Some(_)) => unreachable!("clap rejects conflicting arguments"),
    }
}

fn ensure_standalone_operation(args: &Args) -> Result<()> {
    if args.command.is_some() || args.file.is_some() || args.database.is_some() {
        return Err(Error::InvalidArgument(
            "database operations cannot be combined with query input or --database".to_owned(),
        ));
    }
    Ok(())
}

async fn run_operation(operation: &Operation, global: &Args, config: EngineConfig) -> Result<()> {
    match operation {
        Operation::Import(args) => {
            let format = match args.format {
                operations::NativeImportFormatArg::Csv => rustdb::NativeImportFormat::Csv,
                operations::NativeImportFormatArg::Parquet => rustdb::NativeImportFormat::Parquet,
            };
            let header = match args.header {
                operations::CsvHeaderArg::Auto => rustdb::CsvHeader::Auto,
                operations::CsvHeaderArg::Present => rustdb::CsvHeader::Present,
                operations::CsvHeaderArg::Absent => rustdb::CsvHeader::Absent,
            };
            let compression = match args.compression {
                operations::CsvCompressionArg::Auto => rustdb::CsvCompression::Auto,
                operations::CsvCompressionArg::None => rustdb::CsvCompression::None,
                operations::CsvCompressionArg::Gzip => rustdb::CsvCompression::Gzip,
                operations::CsvCompressionArg::Zstd => rustdb::CsvCompression::Zstd,
            };
            let csv = rustdb::CsvOptions::builder()
                .header(header)
                .delimiter(args.delimiter)
                .compression(compression)
                .build();
            let options = rustdb::NativeImportOptions::new(
                &args.import_id,
                &args.table,
                &args.location,
                format,
            )
            .csv_options(csv);
            let engine = Engine::open(&args.database, config)?;
            let result = engine.session().import(options).await?;
            if args.json {
                print_json(&result, "Native import receipt")?;
            } else {
                let receipt = result.receipt();
                println!(
                    "Native import {}: table={} rows={} generation={} ({})",
                    receipt.import_id(),
                    receipt.table(),
                    receipt.rows(),
                    receipt.catalog_generation(),
                    if result.replayed() {
                        "replayed"
                    } else {
                        "committed"
                    }
                );
            }
        }
        Operation::Migrate { database } => {
            let migration = Engine::migrate(database)?;
            if migration.migrated() {
                println!(
                    "migrated Native format {} -> {}",
                    migration.from_version(),
                    migration.to_version()
                );
            } else {
                println!(
                    "Native database is already at format {}",
                    migration.to_version()
                );
            }
        }
        Operation::Native { command } => match command {
            operations::NativeOperation::Check { database, json } => {
                let report = Engine::check_native(database)?;
                if *json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&report).map_err(|error| {
                            Error::Internal(format!(
                                "failed to encode Native check report: {error}"
                            ))
                        })?
                    );
                } else {
                    println!(
                        "Native check {}: {}",
                        report.path().display(),
                        if report.is_ok() { "OK" } else { "FAILED" }
                    );
                    println!(
                        "format={:?} catalog_generation={:?} tables={} snapshots={} files={} bytes={}",
                        report.format_version(),
                        report.catalog_generation(),
                        report.checked_tables(),
                        report.checked_snapshots(),
                        report.checked_files(),
                        report.checked_bytes()
                    );
                    for issue in report.warnings() {
                        println!(
                            "warning [{}]{}: {}",
                            issue.code(),
                            issue_path(issue),
                            issue.message()
                        );
                    }
                    for issue in report.errors() {
                        println!(
                            "error [{}]{}: {}",
                            issue.code(),
                            issue_path(issue),
                            issue.message()
                        );
                    }
                }
                if !report.is_ok() {
                    return Err(Error::Execution(format!(
                        "Native check found {} integrity error(s)",
                        report.errors().len()
                    )));
                }
            }
            operations::NativeOperation::Repair {
                database,
                apply,
                json,
            } => {
                if *apply {
                    let report = Engine::apply_native_repair(database)?;
                    if *json {
                        print_json(&report, "Native repair report")?;
                    } else {
                        println!(
                            "Native repair {}: {} action(s), {}",
                            report.plan().path().display(),
                            report.applied_actions(),
                            if report.succeeded() { "OK" } else { "FAILED" }
                        );
                        if let Some(backup) = report.backup_path() {
                            println!("metadata backup: {}", backup.display());
                        }
                        print_check_issues(report.after());
                    }
                    if !report.succeeded() {
                        return Err(Error::NativeRepairRefused {
                            path: database.clone(),
                            message: "post-repair integrity check still reports errors; metadata backup was retained"
                                .to_owned(),
                        });
                    }
                } else {
                    let plan = Engine::plan_native_repair(database)?;
                    if *json {
                        print_json(&plan, "Native repair plan")?;
                    } else {
                        println!(
                            "Native repair plan {}: {} action(s), {} blocker(s)",
                            plan.path().display(),
                            plan.actions().len(),
                            plan.blockers().len()
                        );
                        for action in plan.actions() {
                            println!("action: {action:?}");
                        }
                        for blocker in plan.blockers() {
                            println!(
                                "blocker [{}]{}: {}",
                                blocker.code(),
                                issue_path(blocker),
                                blocker.message()
                            );
                        }
                        println!("dry run only; pass --apply to write changes");
                    }
                    if !plan.is_applicable() {
                        return Err(Error::NativeRepairRefused {
                            path: database.clone(),
                            message: format!(
                                "repair plan has {} blocker(s)",
                                plan.blockers().len()
                            ),
                        });
                    }
                }
            }
        },
        Operation::Backup {
            database,
            destination,
            state_root,
        } => {
            let state_root = state_root
                .clone()
                .map(Ok)
                .unwrap_or_else(rustdb::http_shell::security::default_state_root)?;
            Engine::open(database, config)?
                .backup_service_to_location(state_root, destination)
                .await?;
            println!("backup published: {destination}");
        }
        Operation::BackupCheck { backup, json } => {
            let report = Engine::check_service_backup_location(backup, config).await?;
            if *json {
                print_json(&report, "service backup check report")?;
            } else {
                println!(
                    "service backup {}: OK (database_id={} files={} bytes={} http_state={})",
                    report.location(),
                    report.database_id(),
                    report.files(),
                    report.bytes(),
                    if report.service_state_included() {
                        "included"
                    } else {
                        "absent"
                    }
                );
            }
        }
        Operation::Restore {
            backup,
            database,
            state_root,
        } => {
            let state_root = state_root
                .clone()
                .map(Ok)
                .unwrap_or_else(rustdb::http_shell::security::default_state_root)?;
            Engine::restore_service_from_location(backup, database, state_root, config).await?;
            println!("database restored: {}", database.display());
        }
        Operation::Diagnostics { database, output } => {
            return diagnostics::run(database, output.as_deref(), &config);
        }
        Operation::Config { command } => match command {
            operations::ConfigOperation::Validate { path } => {
                let path = server_config::validate_path(path.as_deref(), global)?;
                println!("configuration is valid: {}", path.display());
            }
        },
        Operation::Serve(args) => return admin::serve_database(args.as_ref(), global).await,
        Operation::Service { command } => return service_admin::run(command).await,
        Operation::Shell(args) => return remote::run(args).await,
        Operation::Profile { command } => return admin::profile(command),
        Operation::Principal { command } => return admin::principal(command),
        Operation::Token { command } => return admin::token(command),
        Operation::Datasource { command } => return admin::datasource(command, config).await,
    }
    Ok(())
}

fn issue_path(issue: &rustdb::NativeCheckIssue) -> String {
    issue
        .path()
        .map(|path| format!(" at {}", path.display()))
        .unwrap_or_default()
}

fn print_check_issues(report: &rustdb::NativeCheckReport) {
    for issue in report.warnings() {
        println!(
            "warning [{}]{}: {}",
            issue.code(),
            issue_path(issue),
            issue.message()
        );
    }
    for issue in report.errors() {
        println!(
            "error [{}]{}: {}",
            issue.code(),
            issue_path(issue),
            issue.message()
        );
    }
}

fn print_json(value: &impl serde::Serialize, kind: &str) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value)
            .map_err(|error| Error::Internal(format!("failed to encode {kind}: {error}")))?
    );
    Ok(())
}

pub(crate) async fn execute_statements(
    session: &rustdb::Session,
    sql: &str,
    format: args::OutputFormat,
    csv_null: Option<&str>,
    metrics: bool,
) -> Result<()> {
    let statements = sql_input::parse_statements(sql)?;
    if statements.is_empty() {
        return Err(Error::InvalidArgument("SQL input is empty".to_owned()));
    }
    for statement in statements {
        repl::execute(session, &statement, format, csv_null, metrics).await?;
    }
    Ok(())
}

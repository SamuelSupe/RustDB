#[path = "rustdb/args.rs"]
mod args;
#[path = "rustdb/config.rs"]
mod config;
#[path = "rustdb/output.rs"]
mod output;
#[path = "rustdb/repl.rs"]
mod repl;
#[path = "rustdb/sql_input.rs"]
mod sql_input;

use clap::Parser;

use args::{Args, Operation};
use rustdb::{Engine, EngineConfig, Error, Result};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
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

async fn run(args: Args) -> Result<()> {
    if let Some(operation) = &args.operation {
        ensure_standalone_operation(&args)?;
        return run_operation(operation, config::engine_config(&args)).await;
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

async fn run_operation(operation: &Operation, config: EngineConfig) -> Result<()> {
    match operation {
        Operation::Migrate { database } => {
            let migration = Engine::migrate(database)?;
            if migration.migrated() {
                println!(
                    "migrated Native format {} -> {}",
                    migration.from_version(),
                    migration.to_version()
                );
                if let Some(backup) = migration.backup_path() {
                    println!("v0.7 backup: {}", backup.display());
                }
            } else {
                println!(
                    "Native database is already at format {}",
                    migration.to_version()
                );
            }
        }
        Operation::Backup {
            database,
            destination,
        } => {
            Engine::open(database, config)?
                .backup_to_location(destination)
                .await?;
            println!("backup published: {destination}");
        }
        Operation::Restore { backup, database } => {
            Engine::restore_from_location(backup, database, config).await?;
            println!("database restored: {}", database.display());
        }
    }
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

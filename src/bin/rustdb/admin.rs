use std::{fs, path::Path};

use rustdb::{
    CsvCompression, CsvHeader, CsvOptions, Engine, EngineConfig, Error, ParquetOptions,
    ParquetSchemaMode, Result,
    http_shell::{
        security::{
            BearerToken, SecurityState, copy_profile_bundle, default_profile_root,
            default_state_root, import_profile_bundle,
        },
        serve,
    },
};

use super::{
    args::Args,
    operations::{
        CsvCompressionArg, CsvHeaderArg, DatasourceOperation, ParquetSchemaModeArg,
        ProfileOperation, ServeArgs,
    },
    server_config,
};

pub(super) async fn serve_database(args: &ServeArgs, global: &Args) -> Result<()> {
    let (engine_config, server_config) = server_config::load(args, global)?;
    let engine = Engine::open(&args.database, engine_config)?;
    serve(engine, server_config).await
}

pub(super) fn profile(command: &ProfileOperation) -> Result<()> {
    match command {
        ProfileOperation::Import {
            bundle,
            name,
            profile_root,
        } => {
            let root = profile_root
                .clone()
                .map(Ok)
                .unwrap_or_else(default_profile_root)?;
            let profile = import_profile_bundle(root, name, bundle)?;
            println!(
                "profile '{}' imported for {}",
                profile.name(),
                profile.server_url()
            );
        }
        ProfileOperation::Export {
            database,
            output,
            state_root,
        } => {
            let engine = Engine::open(database, EngineConfig::default())?;
            let root = state_root
                .clone()
                .map(Ok)
                .unwrap_or_else(default_state_root)?;
            let state = SecurityState::for_native_database(root, database)?;
            let _state_lock = state.acquire_server_lock()?;
            copy_profile_bundle(state.directory().join("connection.rustdb-profile"), output)?;
            drop(engine);
            println!("profile bundle exported: {}", output.display());
        }
        ProfileOperation::RotateToken {
            database,
            state_root,
        } => {
            let engine = Engine::open(database, EngineConfig::default())?;
            let root = state_root
                .clone()
                .map(Ok)
                .unwrap_or_else(default_state_root)?;
            let state = SecurityState::for_native_database(root, database)?;
            let _state_lock = state.acquire_server_lock()?;
            let _ = BearerToken::rotate(&state)?;
            remove_generated_bundle(&state.directory().join("connection.rustdb-profile"))?;
            drop(engine);
            println!("Bearer Token rotated; restart the server and export a new profile");
        }
    }
    Ok(())
}

pub(super) async fn datasource(
    command: &DatasourceOperation,
    engine_config: EngineConfig,
) -> Result<()> {
    match command {
        DatasourceOperation::AddCsv {
            database,
            name,
            locations,
            header,
            compression,
            delimiter,
        } => {
            let delimiter = one_byte_delimiter(*delimiter)?;
            let options = CsvOptions::builder()
                .header(csv_header(*header))
                .compression(csv_compression(*compression))
                .delimiter(delimiter)
                .build();
            let engine = Engine::open(database, engine_config)?;
            let schema = engine
                .add_external_csv(name, locations.clone(), options)
                .await?;
            println!(
                "registered CSV source '{name}' ({} columns)",
                schema.fields().len()
            );
        }
        DatasourceOperation::AddParquet {
            database,
            name,
            locations,
            schema_mode,
            hive_partitioning,
        } => {
            let options = ParquetOptions {
                schema: None,
                union_by_name: false,
                schema_mode: parquet_schema_mode(*schema_mode),
                hive_partitioning: *hive_partitioning,
            };
            let engine = Engine::open(database, engine_config)?;
            let schema = engine
                .add_external_parquet(name, locations.clone(), options)
                .await?;
            println!(
                "registered Parquet source '{name}' ({} columns)",
                schema.fields().len()
            );
        }
        DatasourceOperation::List { database } => {
            let engine = Engine::open(database, engine_config)?;
            for source in engine.list_external_sources() {
                println!(
                    "{}\t{}\t{}",
                    source.name(),
                    source.format(),
                    source.locations().join(",")
                );
            }
        }
        DatasourceOperation::Refresh { database, name } => {
            let engine = Engine::open(database, engine_config)?;
            let schema = engine.refresh_external_source(name).await?;
            println!(
                "refreshed source '{name}' ({} columns)",
                schema.fields().len()
            );
        }
        DatasourceOperation::Remove { database, name } => {
            let engine = Engine::open(database, engine_config)?;
            if !engine.remove_external_source(name)? {
                return Err(Error::Catalog(format!(
                    "external source '{name}' does not exist"
                )));
            }
            println!("removed source '{name}'");
        }
    }
    Ok(())
}

fn csv_header(value: CsvHeaderArg) -> CsvHeader {
    match value {
        CsvHeaderArg::Auto => CsvHeader::Auto,
        CsvHeaderArg::Present => CsvHeader::Present,
        CsvHeaderArg::Absent => CsvHeader::Absent,
    }
}

fn csv_compression(value: CsvCompressionArg) -> CsvCompression {
    match value {
        CsvCompressionArg::Auto => CsvCompression::Auto,
        CsvCompressionArg::None => CsvCompression::None,
        CsvCompressionArg::Gzip => CsvCompression::Gzip,
        CsvCompressionArg::Zstd => CsvCompression::Zstd,
    }
}

fn parquet_schema_mode(value: ParquetSchemaModeArg) -> ParquetSchemaMode {
    match value {
        ParquetSchemaModeArg::Strict => ParquetSchemaMode::Strict,
        ParquetSchemaModeArg::Union => ParquetSchemaMode::UnionByName,
        ParquetSchemaModeArg::SafeWidening => ParquetSchemaMode::SafeWidening,
    }
}

fn one_byte_delimiter(value: char) -> Result<u8> {
    if value.len_utf8() != 1 {
        return Err(Error::InvalidArgument(
            "CSV delimiter must be one ASCII byte".into(),
        ));
    }
    Ok(value as u8)
}

fn remove_generated_bundle(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path).map_err(|error| Error::io(path.to_owned(), error))
        }
        Ok(_) => Err(Error::InvalidArgument(format!(
            "refusing to remove unsafe generated profile path {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(path.to_owned(), error)),
    }
}

use std::{fs, path::Path};

use rustdb::{
    CsvCompression, CsvHeader, CsvOptions, Engine, EngineConfig, Error, ParquetOptions,
    ParquetSchemaMode, Result,
    http_shell::{
        security::{
            PrincipalId, PrincipalStore, Role, SecurityState, TokenId, copy_profile_bundle,
            default_profile_root, default_state_root, export_profile_bundle,
            export_profile_bundle_with_token, import_profile_bundle,
        },
        serve,
    },
};

use super::{
    args::Args,
    operations::{
        CsvCompressionArg, CsvHeaderArg, DatasourceOperation, ParquetSchemaModeArg,
        PrincipalOperation, ProfileOperation, RoleArg, ServeArgs, TokenOperation,
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
            token_id,
            server_url,
            state_root,
        } => {
            let engine = Engine::open(database, EngineConfig::default())?;
            let root = state_root
                .clone()
                .map(Ok)
                .unwrap_or_else(default_state_root)?;
            let state = SecurityState::for_native_database(root, database)?;
            let _state_lock = state.acquire_server_lock()?;
            let managed = state.directory().join("connection.rustdb-profile");
            if token_id.is_none() && server_url.is_none() {
                copy_profile_bundle(&managed, output)?;
            } else {
                let store = PrincipalStore::new(state.clone());
                let token_path = match token_id {
                    Some(token_id) => store.profile_token_path(&TokenId::new(token_id.clone())?)?,
                    None => store.connection_token_path()?,
                };
                match server_url {
                    Some(server_url) => {
                        let server_url = url::Url::parse(server_url).map_err(|error| {
                            Error::InvalidArgument(format!("invalid profile server URL: {error}"))
                        })?;
                        export_profile_bundle(
                            output,
                            &server_url,
                            state.ca_certificate_path(),
                            token_path,
                        )?;
                    }
                    None => export_profile_bundle_with_token(&managed, output, token_path)?,
                }
            }
            drop(engine);
            println!("profile bundle exported: {}", output.display());
        }
    }
    Ok(())
}

pub(super) fn principal(command: &PrincipalOperation) -> Result<()> {
    match command {
        PrincipalOperation::List {
            database,
            state_root,
        } => with_principal_store(database, state_root, |store, _| {
            for principal in store.list()? {
                println!(
                    "{}\t{}\t{}\ttokens={}",
                    principal.id(),
                    role_name(principal.role()),
                    if principal.enabled() {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    principal.token_count()
                );
            }
            Ok(())
        }),
        PrincipalOperation::Create {
            database,
            id,
            role,
            state_root,
        } => with_principal_store(database, state_root, |store, _state| {
            let principal_id = PrincipalId::new(id.clone())?;
            let provision = store.create_principal(principal_id.clone(), role_value(*role))?;
            println!("principal created: {principal_id}");
            println!("token id: {}", provision.token_id().as_str());
            println!("profile token file: {}", provision.token_path().display());
            Ok(())
        }),
        PrincipalOperation::SetEnabled {
            database,
            id,
            state: enabled,
            state_root,
        } => with_principal_store(database, state_root, |store, state| {
            let principal = PrincipalId::new(id.clone())?;
            let enabled = matches!(enabled, super::operations::PrincipalStateArg::Enabled);
            store.set_enabled(&principal, enabled)?;
            remove_generated_bundle(&state.directory().join("connection.rustdb-profile"))?;
            println!(
                "principal {principal}: {}",
                if enabled { "enabled" } else { "disabled" }
            );
            Ok(())
        }),
        PrincipalOperation::SetRole {
            database,
            id,
            role,
            state_root,
        } => with_principal_store(database, state_root, |store, state| {
            let principal = PrincipalId::new(id.clone())?;
            let role = role_value(*role);
            store.set_role(&principal, role)?;
            remove_generated_bundle(&state.directory().join("connection.rustdb-profile"))?;
            println!("principal {principal}: role={}", role_name(role));
            Ok(())
        }),
    }
}

pub(super) fn token(command: &TokenOperation) -> Result<()> {
    match command {
        TokenOperation::List {
            database,
            principal,
            state_root,
        } => with_principal_store(database, state_root, |store, _state| {
            let principal = principal
                .as_ref()
                .map(|value| PrincipalId::new(value.clone()))
                .transpose()?;
            for token in store.list_tokens(principal.as_ref())? {
                let state = if token.active() {
                    "active"
                } else if token.revoked() {
                    "revoked"
                } else {
                    "inactive"
                };
                let valid_until = token
                    .valid_until()
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| "never".to_owned());
                println!(
                    "{}\t{}\t{}\tvalid-until={valid_until}",
                    token.token_id().as_str(),
                    token.principal_id(),
                    state,
                );
            }
            Ok(())
        }),
        TokenOperation::Rotate {
            database,
            principal,
            state_root,
        } => with_principal_store(database, state_root, |store, _state| {
            let principal = PrincipalId::new(principal.clone())?;
            let provision = store.rotate_token(&principal)?;
            println!("token added for principal: {principal}");
            println!("token id: {}", provision.token_id().as_str());
            println!("profile token file: {}", provision.token_path().display());
            Ok(())
        }),
        TokenOperation::Revoke {
            database,
            token_id,
            state_root,
        } => with_principal_store(database, state_root, |store, state| {
            let token_id = TokenId::new(token_id.clone())?;
            store.revoke_token(&token_id)?;
            remove_generated_bundle(&state.directory().join("connection.rustdb-profile"))?;
            println!("token revoked: {}", token_id.as_str());
            Ok(())
        }),
    }
}

fn with_principal_store<T>(
    database: &Path,
    state_root: &Option<std::path::PathBuf>,
    operation: impl FnOnce(&PrincipalStore, &SecurityState) -> Result<T>,
) -> Result<T> {
    let engine = Engine::open(database, EngineConfig::default())?;
    let root = state_root
        .clone()
        .map(Ok)
        .unwrap_or_else(default_state_root)?;
    let state = SecurityState::for_native_database(root, database)?;
    let _state_lock = state.acquire_server_lock()?;
    let store = PrincipalStore::new(state.clone());
    let result = operation(&store, &state);
    drop(engine);
    result
}

fn role_value(role: RoleArg) -> Role {
    match role {
        RoleArg::Query => Role::Query,
        RoleArg::Admin => Role::Admin,
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Query => "query",
        Role::Admin => "admin",
        _ => "unknown",
    }
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
            let options = ParquetOptions::default()
                .schema_mode(parquet_schema_mode(*schema_mode))
                .hive_partitioning(*hive_partitioning);
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

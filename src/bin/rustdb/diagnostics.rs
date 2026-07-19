use std::path::Path;

use rustdb::{Engine, EngineConfig, Error, Result};

#[path = "diagnostics/model.rs"]
mod model;
#[path = "diagnostics/output.rs"]
mod output;
#[path = "diagnostics/storage.rs"]
mod storage;

use model::DiagnosticsDocument;

pub(super) fn run(
    database: &Path,
    output_path: Option<&Path>,
    config: &EngineConfig,
) -> Result<()> {
    if let Some(output_path) = output_path {
        reject_database_output(database, output_path)?;
    }
    let check = Engine::check_native(database)?;
    let document = DiagnosticsDocument::new(
        storage::path_sha256(database),
        &check,
        storage::StorageSummary::collect(database),
        config,
    );
    let mut encoded = serde_json::to_vec_pretty(&document).map_err(|error| {
        Error::Internal(format!("failed to encode diagnostics report: {error}"))
    })?;
    encoded.push(b'\n');
    output::emit(&encoded, output_path)
}

fn reject_database_output(database: &Path, output: &Path) -> Result<()> {
    let database = database
        .canonicalize()
        .map_err(|error| Error::io(database.to_path_buf(), error))?;
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(|error| Error::io(None, error))?;
    if parent.starts_with(&database) {
        return Err(Error::InvalidArgument(
            "diagnostics output must be outside the Native database directory".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::reject_database_output;

    #[test]
    fn refuses_to_write_diagnostics_inside_the_database() {
        let temporary = tempfile::tempdir().unwrap();
        let database = temporary.path().join("database");
        std::fs::create_dir(&database).unwrap();
        assert!(reject_database_output(&database, &database.join("report.json")).is_err());
        assert!(reject_database_output(&database, &temporary.path().join("report.json")).is_ok());
    }
}

use std::{fs, path::Path};

use crate::{Error, Result};

use super::{
    generation_io::{generation_from_name, read_current, read_generation},
    validate_tables,
};
use crate::storage::native::io;

pub(in crate::storage::native) fn recover_future_generations(
    root: &Path,
    database_id: &str,
) -> Result<()> {
    let current = read_current(root)?;
    let directory = root.join("catalog").join("generations");
    for entry in
        fs::read_dir(&directory).map_err(|error| Error::io(Some(directory.clone()), error))?
    {
        let entry = entry.map_err(|error| Error::io(Some(directory.clone()), error))?;
        let path = entry.path();
        let Some(generation) = generation_from_name(&path) else {
            continue;
        };
        if generation <= current {
            continue;
        }
        let state = match read_generation(root, generation) {
            Ok(state) => state,
            Err(_) => continue,
        };
        if state.database_id == database_id
            && state.generation == generation
            && validate_tables(&path, &state.tables).is_ok()
        {
            io::remove_file(&path)?;
        }
    }
    Ok(())
}

pub(in crate::storage::native) fn prune_old_generations(
    root: &Path,
    database_id: &str,
    current: u64,
) -> Result<()> {
    let directory = root.join("catalog").join("generations");
    for entry in
        fs::read_dir(&directory).map_err(|error| Error::io(Some(directory.clone()), error))?
    {
        let entry = entry.map_err(|error| Error::io(Some(directory.clone()), error))?;
        let path = entry.path();
        let Some(generation) = generation_from_name(&path) else {
            continue;
        };
        if generation >= current {
            continue;
        }
        let state = match read_generation(root, generation) {
            Ok(state) => state,
            Err(_) => continue,
        };
        if state.database_id == database_id
            && state.generation == generation
            && validate_tables(&path, &state.tables).is_ok()
        {
            io::remove_file(&path)?;
        }
    }
    Ok(())
}

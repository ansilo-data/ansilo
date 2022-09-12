use std::{fs, path::PathBuf};

use ansilo_core::err::{Context, Result};

/// Gets the file contents from the supplied file url
pub(crate) fn get_file(path: PathBuf) -> Result<Vec<u8>> {
    fs::read(&path).with_context(|| format!("Failed to read file {}", path.display()))
}

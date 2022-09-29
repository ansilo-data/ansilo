use std::path::{Path, PathBuf};

use ansilo_core::{
    config,
    err::{Context, Result},
};
use serde::{Deserialize, Serialize};

pub trait FileConfig: Clone + Send + Sync {
    /// The path in which files are be stored
    fn get_path(&self) -> &Path;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileSourceConfig {
    /// The name of the file
    file_name: String,
}

impl FileSourceConfig {
    pub fn new(file_name: String) -> Self {
        Self { file_name }
    }

    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse file source configuration options")
    }

    pub fn file_name(&self) -> &str {
        self.file_name.as_ref()
    }

    pub fn path<C: FileConfig>(&self, conf: &C) -> PathBuf {
        conf.get_path().join(self.file_name())
    }
}

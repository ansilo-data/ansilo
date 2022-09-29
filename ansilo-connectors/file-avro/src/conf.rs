use std::path::{Path, PathBuf};

use ansilo_connectors_file_base::FileConfig;
use ansilo_core::{
    config,
    err::{Context, Result},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AvroConfig {
    /// The path in which avro files should be stored
    pub path: PathBuf,
}

impl AvroConfig {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse connection configuration options")
    }
}

impl FileConfig for AvroConfig {
    fn get_path(&self) -> &Path {
        self.path.as_path()
    }
}

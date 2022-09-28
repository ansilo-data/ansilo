use std::path::{Path, PathBuf};

use ansilo_core::{
    config,
    err::{Context, Result},
};
use apache_avro::Schema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ArvoConfig {
    /// The path in which arvo files should be stored
    pub path: PathBuf,
}

impl ArvoConfig {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse connection configuration options")
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}

#[derive(Clone, Default, PartialEq, Debug, Deserialize, Serialize)]
pub struct ArvoFile {
    path: PathBuf,
    #[serde(skip)]
    schema: Option<Schema>,
}

impl ArvoFile {
    pub fn new(path: PathBuf, schema: Option<Schema>) -> Self {
        Self { path, schema }
    }

    pub fn parse(options: config::Value) -> Result<ArvoFile> {
        serde_yaml::from_value(options).context("Failed to parse")
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }
}

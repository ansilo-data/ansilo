use std::path::PathBuf;

use serde::{Serialize, Deserialize};


/// Configuration used to initialise the postgres database
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct BuildConfig {
    /// Build stages
    pub stages: Vec<BuildStageConfig>,
}

/// A set of of sql scripts to run
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct BuildStageConfig {
    /// The name of the build stage
    pub name: Option<String>,
    /// The name of the service user to authenticate as
    pub service_user: Option<String>,
    /// The sql scripts to run. This can contain wildcards for globbing.
    pub sql: PathBuf
}
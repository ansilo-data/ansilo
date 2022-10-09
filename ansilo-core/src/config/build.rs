use std::{fmt::Display, path::PathBuf};

use serde::{Deserialize, Serialize};

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
    pub sql: PathBuf,
    /// The build stage mode
    #[serde(default)]
    pub mode: BuildStageMode,
}

/// The type of the build stage
#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize, Default)]
pub enum BuildStageMode {
    /// Runs on `ansilo build`
    #[serde(rename = "build")]
    #[default]
    Build,
    /// Runs on `ansilo run`
    #[serde(rename = "runtime")]
    Runtime,
}

impl Display for BuildStageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildStageMode::Build => write!(f, "build-time"),
            BuildStageMode::Runtime => write!(f, "runtime"),
        }
    }
}

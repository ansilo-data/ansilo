use serde::{Deserialize, Serialize};

pub use serde_yaml::{from_value, Mapping, Number, Sequence, Value};

mod bincode;
mod networking;
pub use networking::*;
mod auth;
pub use auth::*;
mod build;
pub use build::*;
mod sources;
pub use sources::*;
mod entities;
pub use entities::*;
mod jobs;
pub use jobs::*;
mod util;
pub use util::*;
mod postgres;
pub use postgres::*;
mod secrets;
pub use secrets::*;

// TODO: consider ansilo versioning

/// An entire configuration for an ansilo node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct NodeConfig {
    /// The human-readable name of the node
    pub name: String,
    /// The description of this node
    pub description: Option<String>,
    /// Networking options
    pub networking: NetworkingConfig,
    /// Auth options
    pub auth: AuthConfig,
    /// The build configuration
    pub build: BuildConfig,
    /// List of data source configurations for the node
    #[serde(default)]
    pub sources: Vec<DataSourceConfig>,
    /// List of entities exposed by the node
    #[serde(default)]
    pub entities: Vec<EntityConfig>,
    /// List of jobs run by the node
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
    /// Postgres configuration options
    pub postgres: Option<PostgresConfig>,
}

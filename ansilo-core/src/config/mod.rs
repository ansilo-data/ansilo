use serde::{Deserialize, Serialize};

pub use serde_yaml::{
    Value, Number, Mapping, Sequence
};

mod ari;
pub use ari::*;
mod networking;
pub use networking::*;
mod auth;
pub use auth::*;
mod sources;
pub use sources::*;
mod entities;
pub use entities::*;
mod jobs;
pub use jobs::*;

// TODO: consider ansilo versioning

/// An entire configuration for an ansilo node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct NodeConfig {
    /// The human-readable name of the node
    pub name: String,
    /// The description of this node
    pub description: String,
    /// Networking options
    pub networking: NetworkingConfig,
    /// Auth options
    pub auth: AuthConfig,
    /// List of data source configurations for the node
    pub sources: Vec<DataSourceConfig>,
    /// List of entities exposed by the node
    pub entities: Vec<EntityConfig>,
    /// List of jobs run by the node
    pub jobs: Vec<JobConfig>,
}

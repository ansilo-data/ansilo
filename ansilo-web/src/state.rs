use ansilo_core::{
    config::NodeConfig,
    data::chrono::{DateTime, Utc},
};
use ansilo_pg::{handler::PostgresConnectionHandler, PostgresConnectionPools};
use ansilo_util_health::Health;
use serde::{Deserialize, Serialize};

/// Required state and dependencies for the http api
#[derive(Clone)]
pub struct HttpApiState {
    /// Reference to the app config
    conf: &'static NodeConfig,
    /// Connection pools to postgres
    pools: PostgresConnectionPools,
    /// Handler for connections to postgres
    pg_handler: PostgresConnectionHandler,
    /// System health
    health: Health,
    /// Version info
    version_info: VersionInfo,
}

impl HttpApiState {
    pub fn new(
        conf: &'static NodeConfig,
        pools: PostgresConnectionPools,
        pg_handler: PostgresConnectionHandler,
        health: Health,
        version_info: VersionInfo,
    ) -> Self {
        Self {
            conf,
            pools,
            pg_handler,
            health,
            version_info,
        }
    }

    pub fn conf(&self) -> &NodeConfig {
        self.conf
    }

    pub fn pools(&self) -> &PostgresConnectionPools {
        &self.pools
    }

    pub fn pg_handler(&self) -> &PostgresConnectionHandler {
        &self.pg_handler
    }

    pub fn health(&self) -> &Health {
        &self.health
    }

    pub fn version_info(&self) -> &VersionInfo {
        &self.version_info
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    /// The version string of this running instance
    pub version: String,
    /// When the ansilo build occurred
    pub built_at: DateTime<Utc>,
}

impl VersionInfo {
    pub fn new(version: impl Into<String>, built_at: DateTime<Utc>) -> Self {
        Self {
            version: version.into(),
            built_at,
        }
    }
}

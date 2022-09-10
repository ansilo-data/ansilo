use ansilo_auth::Authenticator;
use ansilo_core::{
    config::NodeConfig,
    data::chrono::{DateTime, Utc},
};
use ansilo_pg::PostgresConnectionPools;
use serde::{Serialize, Deserialize};

/// Required state and dependencies for the http api
#[derive(Clone)]
pub struct HttpApiState {
    /// Reference to the app config
    conf: &'static NodeConfig,
    /// Connection pools to postgres
    pools: PostgresConnectionPools,
    /// The authentication system
    auth: Authenticator,
    /// Version info
    version_info: VersionInfo,
}

impl HttpApiState {
    pub fn new(
        conf: &'static NodeConfig,
        pools: PostgresConnectionPools,
        auth: Authenticator,
        version_info: VersionInfo,
    ) -> Self {
        Self {
            conf,
            pools,
            auth,
            version_info,
        }
    }

    pub fn conf(&self) -> &NodeConfig {
        self.conf
    }

    pub fn pools(&self) -> &PostgresConnectionPools {
        &self.pools
    }

    pub fn auth(&self) -> &Authenticator {
        &self.auth
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

use ansilo_auth::Authenticator;
use ansilo_core::config::NodeConfig;
use ansilo_pg::PostgresConnectionPools;

/// Required state and dependencies for the http api
#[derive(Clone)]
pub struct HttpApiState {
    /// Reference to the app config
    conf: &'static NodeConfig,
    /// Connection pools to postgres
    pools: PostgresConnectionPools,
    /// The authentication system
    auth: Authenticator,
}

impl HttpApiState {
    pub fn new(
        conf: &'static NodeConfig,
        pools: PostgresConnectionPools,
        auth: Authenticator,
    ) -> Self {
        Self { conf, pools, auth }
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
}

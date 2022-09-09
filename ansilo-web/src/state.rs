use ansilo_pg::PostgresConnectionPools;

/// Required state and dependencies for the http api
#[derive(Clone)]
pub struct HttpApiState {
    /// Connection pools to postgres
    pools: PostgresConnectionPools,
}

impl HttpApiState {
    pub fn new(pools: PostgresConnectionPools) -> Self {
        Self { pools }
    }

    pub fn pools(&self) -> &PostgresConnectionPools {
        &self.pools
    }
}

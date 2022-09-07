use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use ansilo_connectors_base::interface::ConnectionPool;
use ansilo_core::{auth::AuthContext, err::Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;

use crate::{runtime, PostgresConnection, PostgresConnectionConfig};

/// Postgres connection pool based on deadpool
#[derive(Clone)]
pub struct PostgresConnectionPool {
    pool: Pool,
}

impl PostgresConnectionPool {
    pub fn new(conf: PostgresConnectionConfig) -> Result<Self> {
        let pool_conf = conf.pool.clone().unwrap_or_default();

        let pool = Pool::builder(Manager::from_config(
            conf.try_into()?,
            MakeTlsConnector::new(TlsConnector::new()?),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        ))
        .runtime(deadpool_postgres::Runtime::Tokio1)
        .max_size(pool_conf.max_size.unwrap_or(20) as _)
        .wait_timeout(Some(
            pool_conf
                .connection_timeout
                .unwrap_or(Duration::from_secs(60)),
        ))
        .build()?;

        Ok(Self { pool })
    }
}

impl ConnectionPool for PostgresConnectionPool {
    type TConnection = PostgresConnection<PooledClient>;

    fn acquire(&mut self, _auth: Option<&AuthContext>) -> Result<Self::TConnection> {
        let con = runtime().block_on(self.pool.get())?;

        Ok(PostgresConnection::new(PooledClient(con)))
    }
}

/// Adaptor for the deadpool client wrapper type to
/// deref into the underlying tokio_postgres::Client
pub struct PooledClient(deadpool_postgres::Client);

impl Deref for PooledClient {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

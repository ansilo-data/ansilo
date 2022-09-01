use std::{path::PathBuf, time::Duration};

use ansilo_core::err::{bail, Context, Error, Result};
use ansilo_logging::info;
use deadpool::{
    async_trait,
    managed::{Object, Pool, PoolConfig, PoolError, RecycleError, RecycleResult},
};
use postgres::Config;

use crate::conf::PostgresConf;

use super::connection::LlPostgresConnection;

#[derive(Debug, Clone, PartialEq)]
pub struct LlPostgresConnectionPoolConfig {
    pg: &'static PostgresConf,
    user: String,
    database: String,
    max_size: usize,
    recycle_query: Option<String>,
    connect_timeout: Duration,
}

/// Postgres connection pool
#[derive(Clone)]
pub struct LlPostgresConnectionPool {
    /// The inner deadpool pool
    pool: Pool<LlPostgresConnectionManager>,
}

impl LlPostgresConnectionPool {
    /// Constructs a new connection pool
    pub fn new(conf: LlPostgresConnectionPoolConfig) -> Result<Self> {
        let mut pg_conf = postgres::Config::new();
        pg_conf.user(&conf.user);
        pg_conf.dbname(&conf.database);

        let socket_path = conf.pg.pg_socket_path();

        Ok(Self {
            pool: Pool::builder(LlPostgresConnectionManager::new(conf.clone(), pg_conf))
                .max_size(conf.max_size)
                .create_timeout(Some(conf.connect_timeout))
                .runtime(deadpool::Runtime::Tokio1)
                .build()
                .map_err(|e| {
                    Error::msg(format!(
                        "Failed to create postgres connection pool: {:?}",
                        e
                    ))
                })?,
        })
    }

    /// Aquires a connection from the pool
    pub async fn acquire(
        &mut self,
    ) -> Result<Object<LlPostgresConnectionManager>, PoolError<Error>> {
        self.pool.get().await
    }
}

#[derive(Debug)]
pub struct LlPostgresConnectionManager {
    conf: LlPostgresConnectionPoolConfig,
    pg_conf: Config,
}

impl LlPostgresConnectionManager {
    pub fn new(conf: LlPostgresConnectionPoolConfig, pg_conf: Config) -> Self {
        Self { conf, pg_conf }
    }
}

#[async_trait]
impl deadpool::managed::Manager for LlPostgresConnectionManager {
    type Type = LlPostgresConnection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type> {
        Ok(
            LlPostgresConnection::connect(self.conf.pg.pg_socket_path(), self.pg_conf.clone())
                .await?,
        )
    }

    async fn recycle(&self, con: &mut Self::Type) -> RecycleResult<Self::Error> {
        if con.broken() {
            info!("Postgres connection is broken, cannot recycle");
            return Err(RecycleError::StaticMessage("Connection is broken"));
        }

        // Clean the connection
        if let Some(query) = self.conf.recycle_query.as_ref() {
            con.execute(query).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, thread};

    use deadpool::Status;

    use crate::{initdb::PostgresInitDb, server::PostgresServer, PG_SUPER_USER};

    use super::*;

    fn test_pg_config(test_name: &'static str) -> &'static PostgresConf {
        let conf = PostgresConf {
            install_dir: PathBuf::from("/home/vscode/.pgx/14.5/pgx-install/"),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-ll-connection-pool/{}",
                test_name
            )),
            socket_dir_path: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-ll-connection-pool/{}",
                test_name
            )),
            fdw_socket_path: PathBuf::from("not-used"),
        };
        Box::leak(Box::new(conf))
    }

    #[test]
    fn test_postgres_connection_pool_new() {
        let conf = test_pg_config("new");
        let pool = LlPostgresConnectionPool::new(LlPostgresConnectionPoolConfig {
            pg: conf,
            user: PG_SUPER_USER.into(),
            database: "postgres".into(),
            max_size: 5,
            recycle_query: None,
            connect_timeout: Duration::from_secs(1),
        })
        .unwrap();

        assert_eq!(pool.pool.status().max_size, 5);
        assert_eq!(pool.pool.status().size, 0);
    }

    #[tokio::test]
    async fn test_postgres_connection_pool_get_without_server() {
        let conf = test_pg_config("down");
        let mut pool = LlPostgresConnectionPool::new(LlPostgresConnectionPoolConfig {
            pg: conf,
            user: PG_SUPER_USER.into(),
            database: "postgres".into(),
            max_size: 5,
            recycle_query: None,
            connect_timeout: Duration::from_secs(1),
        })
        .unwrap();

        assert!(pool.acquire().await.is_err());
    }

    #[tokio::test]
    async fn test_postgres_connection_pool_with_running_server() {
        ansilo_logging::init_for_tests();
        let conf = test_pg_config("up");
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();
        let mut _server = PostgresServer::boot(conf).unwrap();
        thread::spawn(move || _server.wait());
        thread::sleep(Duration::from_secs(2));

        let mut pool = LlPostgresConnectionPool::new(LlPostgresConnectionPoolConfig {
            pg: conf,
            user: PG_SUPER_USER.into(),
            database: "postgres".into(),
            max_size: 5,
            recycle_query: None,
            connect_timeout: Duration::from_secs(1),
        })
        .unwrap();

        let mut con = pool.acquire().await.unwrap();
        con.execute("SELECT 3 + 4").await.unwrap();
    }
}

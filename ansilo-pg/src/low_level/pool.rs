use std::time::Duration;

use ansilo_core::err::{Error, Result};
use ansilo_logging::{debug, info};
use deadpool::{
    async_trait,
    managed::{Manager, Object, Pool, RecycleError, RecycleResult},
};
use postgres::Config;
use tokio::{
    runtime::Handle,
    sync::broadcast::{self, Receiver, Sender},
};

use crate::conf::PostgresConf;

use super::connection::LlPostgresConnection;

pub type AppPostgresConnection = Object<LlPostgresConnectionManager>;

/// Postgres connection pool
#[derive(Clone)]
pub struct LlPostgresConnectionPool {
    /// The inner deadpool pool
    pool: Pool<LlPostgresConnectionManager>,
    /// Upon drop will shutdown background tasks
    _terminator: Sender<()>,
}

/// Configuration options for the pool
#[derive(Debug, Clone, PartialEq)]
pub struct LlPostgresConnectionPoolConfig {
    pub pg: &'static PostgresConf,
    pub user: String,
    pub database: String,
    pub max_size: usize,
    pub connect_timeout: Duration,
}

impl LlPostgresConnectionPool {
    /// Constructs a new connection pool
    pub fn new(handle: Handle, conf: LlPostgresConnectionPoolConfig) -> Result<Self> {
        let mut pg_conf = postgres::Config::new();
        pg_conf.user(&conf.user);
        pg_conf.dbname(&conf.database);

        let pool = Pool::builder(LlPostgresConnectionManager::new(conf.clone(), pg_conf))
            .max_size(conf.max_size)
            .create_timeout(Some(conf.connect_timeout))
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .map_err(|e| {
                Error::msg(format!(
                    "Failed to create postgres connection pool: {:?}",
                    e
                ))
            })?;

        let (terminator, receiver) = broadcast::channel(1);
        Self::drop_old_connections(handle, pool.clone(), receiver);

        Ok(Self {
            pool,
            _terminator: terminator,
        })
    }

    fn drop_old_connections(
        handle: Handle,
        pool: Pool<LlPostgresConnectionManager>,
        mut terminator: Receiver<()>,
    ) {
        handle.spawn(async move {
            // TODO[low]: Make max connection age configurable
            let max_age = Duration::from_secs(3600);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                    _ = terminator.recv() => return,
                }

                debug!("Dropping old postgres connections");
                pool.retain(|_, metrics| metrics.last_used() < max_age);
            }
        });
    }

    /// Aquires a connection from the pool
    pub async fn acquire(&self) -> Result<AppPostgresConnection> {
        self.pool
            .get()
            .await
            .map_err(|e| Error::msg(format!("Failed to acquire connection: {:?}", e)))
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
impl Manager for LlPostgresConnectionManager {
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
        for query in con.recycle_queries.drain(..).collect::<Vec<_>>() {
            con.execute(query).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, thread};

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
            app_users: vec![],
            init_db_sql: vec![],
        };
        Box::leak(Box::new(conf))
    }

    #[tokio::test]
    async fn test_postgres_connection_pool_new() {
        let handle = tokio::runtime::Handle::try_current().unwrap();
        let conf = test_pg_config("new");
        let pool = LlPostgresConnectionPool::new(
            handle,
            LlPostgresConnectionPoolConfig {
                pg: conf,
                user: PG_SUPER_USER.into(),
                database: "postgres".into(),
                max_size: 5,
                connect_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();

        assert_eq!(pool.pool.status().max_size, 5);
        assert_eq!(pool.pool.status().size, 0);
    }

    #[tokio::test]
    async fn test_postgres_connection_pool_get_without_server() {
        let handle = tokio::runtime::Handle::try_current().unwrap();
        let conf = test_pg_config("down");
        let pool = LlPostgresConnectionPool::new(
            handle,
            LlPostgresConnectionPoolConfig {
                pg: conf,
                user: PG_SUPER_USER.into(),
                database: "postgres".into(),
                max_size: 5,
                connect_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();

        assert!(pool.acquire().await.is_err());
    }

    #[tokio::test]
    async fn test_postgres_connection_pool_with_running_server() {
        let handle = tokio::runtime::Handle::try_current().unwrap();
        ansilo_logging::init_for_tests();
        let conf = test_pg_config("up");
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();
        let mut _server = PostgresServer::boot(conf).unwrap();
        thread::spawn(move || _server.wait());
        thread::sleep(Duration::from_secs(2));

        let pool = LlPostgresConnectionPool::new(
            handle,
            LlPostgresConnectionPoolConfig {
                pg: conf,
                user: PG_SUPER_USER.into(),
                database: "postgres".into(),
                max_size: 5,
                connect_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();

        let mut con = pool.acquire().await.unwrap();
        con.execute("SELECT 3 + 4").await.unwrap();
    }
}

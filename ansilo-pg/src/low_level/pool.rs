use std::{path::PathBuf, time::Duration};

use ansilo_core::err::{Context, Result};
use ansilo_util_r2d2::manager::{OurManageConnection, R2d2Adaptor};
use postgres::Config;
use r2d2_postgres::r2d2::{Pool, PooledConnection};

use crate::conf::PostgresConf;

use super::connection::LlPostgresConnection;

/// Postgres connection pool
#[derive(Debug, Clone)]
pub(crate) struct LlPostgresConnectionPool {
    /// The inner r2d2 pool
    pool: Pool<R2d2Adaptor<LlPostgresConnectionManager>>,
}

impl LlPostgresConnectionPool {
    /// Constructs a new connection pool
    pub fn new(
        conf: &PostgresConf,
        user: &str,
        database: &str,
        min_size: u32,
        max_size: u32,
        connect_timeout: Duration,
    ) -> Result<Self> {
        let mut pg_conf = postgres::Config::new();
        pg_conf.user(user);
        pg_conf.dbname(database);

        let socket_path = conf.pg_socket_path();

        Ok(Self {
            pool: Pool::builder()
                .min_idle(Some(min_size))
                .max_size(max_size)
                .max_lifetime(Some(Duration::from_secs(60 * 60)))
                .idle_timeout(Some(Duration::from_secs(30 * 60)))
                .connection_timeout(connect_timeout)
                .build(LlPostgresConnectionManager::new(socket_path, pg_conf).into())
                .context("Failed to create postgres connection pool")?,
        })
    }

    /// Aquires a connection from the pool
    pub fn acquire(
        &mut self,
    ) -> Result<PooledConnection<R2d2Adaptor<LlPostgresConnectionManager>>> {
        self.pool
            .get()
            .context("Failed to acquire a connection from the connection pool")
    }
}

#[derive(Debug)]
pub struct LlPostgresConnectionManager {
    socket_path: PathBuf,
    config: Config,
}

impl LlPostgresConnectionManager {
    pub fn new(socket_path: PathBuf, config: Config) -> Self {
        Self {
            socket_path,
            config,
        }
    }
}

impl OurManageConnection for LlPostgresConnectionManager {
    type Connection = LlPostgresConnection;

    fn connect(&self) -> Result<Self::Connection> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create runtime")?;

        Ok(LlPostgresConnection::connect(
            runtime,
            self.socket_path.clone(),
            self.config.clone(),
        )?)
    }

    fn is_valid(&self, con: &mut Self::Connection) -> Result<()> {
        con.execute_sync("SELECT 1")
    }

    fn has_broken(&self, con: &mut Self::Connection) -> bool {
        con.broken()
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
        };
        Box::leak(Box::new(conf))
    }

    #[test]
    fn test_postgres_connection_pool_new() {
        let conf = test_pg_config("new");
        let pool = LlPostgresConnectionPool::new(
            conf,
            PG_SUPER_USER,
            "postgres",
            0,
            5,
            Duration::from_secs(1),
        )
        .unwrap();

        assert_eq!(pool.pool.min_idle(), Some(0));
        assert_eq!(pool.pool.max_size(), 5);
        assert_eq!(pool.pool.state().connections, 0);
    }

    #[test]
    fn test_postgres_connection_pool_without_server() {
        let conf = test_pg_config("down");
        LlPostgresConnectionPool::new(
            conf,
            PG_SUPER_USER,
            "postgres",
            1,
            5,
            Duration::from_secs(1),
        )
        .unwrap_err();
    }

    #[test]
    fn test_postgres_connection_pool_with_running_server() {
        ansilo_logging::init_for_tests();
        let conf = test_pg_config("up");
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();
        let mut _server = PostgresServer::boot(conf).unwrap();
        thread::spawn(move || _server.wait());
        thread::sleep(Duration::from_secs(2));

        let mut pool = LlPostgresConnectionPool::new(
            conf,
            PG_SUPER_USER,
            "postgres",
            1,
            5,
            Duration::from_secs(5),
        )
        .unwrap();

        let mut con = pool.acquire().unwrap();
        con.execute_sync("SELECT 3 + 4").unwrap();
    }
}

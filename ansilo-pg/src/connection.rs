use std::time::Duration;

use ansilo_core::err::{Context, Result};
use r2d2_postgres::{
    postgres::NoTls,
    r2d2::{Pool, PooledConnection},
    PostgresConnectionManager,
};

use crate::conf::PostgresConf;

/// Postgres connection pool
#[derive(Debug, Clone)]
pub(crate) struct PostgresConnectionPool {
    /// The inner r2d2 pool
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresConnectionPool {
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
        pg_conf.host_path(conf.socket_dir_path.as_path());
        pg_conf.port(conf.port);
        pg_conf.user(user);
        pg_conf.ssl_mode(postgres::config::SslMode::Disable);
        pg_conf.dbname(database);
        pg_conf.connect_timeout(connect_timeout);

        Ok(Self {
            pool: Pool::builder()
                .min_idle(Some(min_size))
                .max_size(max_size)
                .max_lifetime(Some(Duration::from_secs(60 * 60)))
                .idle_timeout(Some(Duration::from_secs(30 * 60)))
                .connection_timeout(connect_timeout)
                .build(PostgresConnectionManager::new(pg_conf, NoTls))
                .context("Failed to create postgres connection pool")?,
        })
    }

    /// Aquires a connection from the pool
    pub fn acquire(&mut self) -> Result<PooledConnection<PostgresConnectionManager<NoTls>>> {
        self.pool
            .get()
            .context("Failed to acquire a connection from the connection pool")
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, thread};

    use crate::{initdb::PostgresInitDb, server::PostgresServer};

    use super::*;

    fn test_pg_config(test_name: &'static str) -> PostgresConf {
        PostgresConf {
            install_dir: PathBuf::from("/usr/lib/postgresql/14"),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-connection-pool/{}",
                test_name
            )),
            socket_dir_path: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-connection-pool/{}",
                test_name
            )),
            port: 65432,
            fdw_socket_path: PathBuf::from("not-used"),
            superuser: "pgsuper".to_string(),
        }
    }

    #[test]
    fn test_postgres_connection_pool_new() {
        let conf = test_pg_config("new");
        let pool = PostgresConnectionPool::new(
            &conf,
            &conf.superuser,
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
        PostgresConnectionPool::new(
            &conf,
            &conf.superuser,
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
        PostgresInitDb::reset(&conf).unwrap();
        PostgresInitDb::run(conf.clone())
            .unwrap()
            .complete()
            .unwrap();
        let mut _server = PostgresServer::boot(conf.clone()).unwrap();
        thread::spawn(move || _server.wait());

        let mut pool = PostgresConnectionPool::new(
            &conf,
            &conf.superuser,
            "postgres",
            1,
            5,
            Duration::from_secs(5),
        )
        .unwrap();

        let mut con = pool.acquire().unwrap();
        let res: i32 = con.query_one("SELECT 3 + 4", &[]).unwrap().get(0);
        assert_eq!(res, 7);
    }
}

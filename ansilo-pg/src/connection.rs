use std::time::Duration;

use ansilo_core::err::{Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

use crate::{conf::PostgresConf, PG_PORT};

/// A connection to the local postgres
pub type PostgresConnection = deadpool_postgres::Client;

/// Postgres connection pool
#[derive(Clone)]
pub struct PostgresConnectionPool {
    /// The inner connection pool
    pool: Pool,
}

impl PostgresConnectionPool {
    /// Constructs a new connection pool
    pub fn new(
        conf: &PostgresConf,
        user: &str,
        database: &str,
        max_size: u32,
        connect_timeout: Duration,
    ) -> Result<Self> {
        let mut pg_conf = tokio_postgres::Config::new();
        pg_conf.host_path(conf.socket_dir_path.as_path());
        pg_conf.port(PG_PORT);
        pg_conf.user(user);
        pg_conf.ssl_mode(tokio_postgres::config::SslMode::Disable);
        pg_conf.dbname(database);
        pg_conf.connect_timeout(connect_timeout);

        Ok(Self {
            pool: Pool::builder(Manager::from_config(
                pg_conf,
                NoTls,
                ManagerConfig {
                    // We only use this connection pool for trusted clients,
                    // eg our build scripts or ansilo-web, hence we can have
                    // fast connection refreshes
                    recycling_method: RecyclingMethod::Fast,
                },
            ))
            .max_size(max_size as _)
            .create_timeout(Some(connect_timeout))
            .wait_timeout(Some(Duration::from_secs(60)))
            .recycle_timeout(Some(Duration::from_secs(10)))
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .context("Failed to create postgres connection pool")?,
        })
    }

    /// Aquires a connection from the pool
    pub async fn acquire(&self) -> Result<PostgresConnection> {
        self.pool
            .get()
            .await
            .context("Failed to acquire a connection from the connection pool")
    }
}

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf, thread};

    use ansilo_core::config::ResourceConfig;

    use crate::{initdb::PostgresInitDb, server::PostgresServer, PG_SUPER_USER};

    use super::*;

    fn test_pg_config(test_name: &'static str) -> &'static PostgresConf {
        let conf = PostgresConf {
            resources: ResourceConfig::default(),
            install_dir: PathBuf::from(
                env::var("ANSILO_TEST_PG_DIR").unwrap_or("/usr/lib/postgresql/14".into()),
            ),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-connection-pool/{}",
                test_name
            )),
            socket_dir_path: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-connection-pool/{}",
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
        let conf = test_pg_config("new");
        let pool =
            PostgresConnectionPool::new(conf, PG_SUPER_USER, "postgres", 5, Duration::from_secs(1))
                .unwrap();

        assert_eq!(pool.pool.status().size, 0);
        assert_eq!(pool.pool.status().max_size, 5);
    }

    #[tokio::test]
    async fn test_postgres_connection_pool_without_server() {
        let conf = test_pg_config("down");
        let res =
            PostgresConnectionPool::new(conf, PG_SUPER_USER, "postgres", 5, Duration::from_secs(1))
                .unwrap()
                .acquire()
                .await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_postgres_connection_pool_with_running_server() {
        ansilo_logging::init_for_tests();
        let conf = test_pg_config("up");
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();
        let server = PostgresServer::boot(conf).unwrap();
        server.block_until_ready(Duration::from_secs(5)).unwrap();
        thread::spawn(move || server.wait());

        let pool = PostgresConnectionPool::new(
            conf,
            PG_SUPER_USER,
            "postgres",
            5,
            Duration::from_secs(10),
        )
        .unwrap();

        let con = pool.acquire().await.unwrap();
        let res: i32 = con.query_one("SELECT 3 + 4", &[]).await.unwrap().get(0);
        assert_eq!(res, 7);
    }
}

use std::time::Duration;

use ansilo_core::err::Result;
use ansilo_logging::info;
use conf::PostgresConf;
use configure::configure;
use connection::{PostgresConnection, PostgresConnectionPool};
use initdb::PostgresInitDb;
use low_level::{
    multi_pool::{MultiUserPostgresConnectionPool, MultiUserPostgresConnectionPoolConfig},
    pool::AppPostgresConnection,
};
use manager::PostgresServerManager;

/// This module orchestrates our postgres instance and provides an api
/// to execute queries against it. Postgres is run as a child process.
///
/// In order for postgres to retrieve data from our sources, the ansilo-pgx
/// extension is installed which creates a FDW which connects back to our
/// ansilo process over a unix socket.
pub mod conf;
pub mod connection;
pub mod fdw;
pub mod initdb;
pub mod low_level;
pub mod manager;
pub mod proc;
pub mod proto;
pub mod server;
pub mod handler;

mod configure;
#[cfg(test)]
mod test;

/// Use the default database created by initdb
pub const PG_DATABASE: &str = "postgres";

/// NOTE: we disable listening over TCP/IP and only connect via unix sockets
/// This is purely used to have a stable unix socket path for connecting
pub const PG_PORT: u16 = 5432;

/// The username of the super user used to bootstrap and configure postgres
pub const PG_SUPER_USER: &str = "ansilosuperuser";

/// The username of the admin user which executes the user-provided initialisation scripts
pub const PG_ADMIN_USER: &str = "ansiloadmin";

/// The entrypoint for managing our postgres instance
pub struct PostgresInstance {
    /// The postgres configuration
    conf: &'static PostgresConf,
    /// The server manager
    server: PostgresServerManager,
    /// Connection pools
    pools: PostgresConnectionPools,
}

/// Thread-safe connection pools to access postgres
#[derive(Clone)]
pub struct PostgresConnectionPools {
    /// The admin user connection pool
    admin: PostgresConnectionPool,
    /// The app user connection pool
    app: MultiUserPostgresConnectionPool,
}

impl PostgresInstance {
    /// Boots an already-initialised postgres instance based on the
    /// supplied configuration
    pub async fn start(conf: &'static PostgresConf) -> Result<Self> {
        let server = PostgresServerManager::new(conf);
        server.block_until_ready(Duration::from_secs(10))?;

        Self::connect(conf, server).await
    }

    /// Boots and initialises postgres instance based on the
    /// supplied configuration
    pub async fn configure(conf: &'static PostgresConf) -> Result<Self> {
        let connect_timeout = Duration::from_secs(10);

        info!("Running initdb...");
        PostgresInitDb::reset(conf)?;
        PostgresInitDb::run(conf)?.complete()?;
        let server = PostgresServerManager::new(conf);
        server.block_until_ready(connect_timeout)?;

        let superuser_con =
            PostgresConnectionPool::new(conf, PG_SUPER_USER, PG_DATABASE, 1, connect_timeout)?
                .acquire()
                .await?;

        info!("Configuring postgres...");
        configure(conf, superuser_con).await?;

        Self::connect(conf, server).await
    }

    async fn connect(conf: &'static PostgresConf, server: PostgresServerManager) -> Result<Self> {
        let connect_timeout = Duration::from_secs(10);

        // TODO: configurable pool sizes
        let admin_pool =
            PostgresConnectionPool::new(conf, PG_ADMIN_USER, PG_DATABASE, 5, connect_timeout)?;

        let app_pool =
            MultiUserPostgresConnectionPool::new(MultiUserPostgresConnectionPoolConfig {
                pg: conf,
                users: conf.app_users.clone(),
                database: PG_DATABASE.into(),
                max_cons_per_user: 50,
                connect_timeout,
            })?;

        // Ensure able to connect to postgres
        let _ = admin_pool.acquire().await?;

        Ok(Self {
            conf,
            server,
            pools: PostgresConnectionPools {
                admin: admin_pool,
                app: app_pool,
            },
        })
    }

    /// Gets the connection pools for this instance
    pub fn connections(&mut self) -> &mut PostgresConnectionPools {
        &mut self.pools
    }

    /// Gets the configuration for this instance
    pub fn conf(&self) -> &PostgresConf {
        &self.conf
    }

    /// Terminates the postgres instance, waiting for shutdown to complete
    pub fn terminate(self) -> Result<()> {
        self.server.terminate()
    }
}

impl PostgresConnectionPools {
    pub fn new(admin: PostgresConnectionPool, app: MultiUserPostgresConnectionPool) -> Self {
        Self { admin, app }
    }

    /// Gets a connection with admin privileges to the database
    /// IMPORTANT: Only use this connection for trusted queries
    /// and not queries supplied by the user
    pub async fn admin(&self) -> Result<PostgresConnection> {
        self.admin.acquire().await
    }

    /// Gets a connection authenticated as the supplied app user
    pub async fn app(&self, username: &str) -> Result<AppPostgresConnection> {
        self.app.acquire(username).await
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn test_pg_config(test_name: &'static str) -> &'static PostgresConf {
        let conf = PostgresConf {
            install_dir: PathBuf::from("/usr/lib/postgresql/14"),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!("/tmp/ansilo-tests/pg-instance/{}/data/", test_name)),
            socket_dir_path: PathBuf::from(format!("/tmp/ansilo-tests/pg-instance/{}", test_name)),
            fdw_socket_path: PathBuf::from("not-used"),
            app_users: vec![],
            init_db_sql: vec![],
        };
        Box::leak(Box::new(conf))
    }

    #[tokio::test]
    async fn test_postgres_instance_configure() {
        ansilo_logging::init_for_tests();

        let conf = test_pg_config("configure");
        let instance = PostgresInstance::configure(&conf).await.unwrap();

        assert!(instance.server.running());
    }

    #[tokio::test]
    async fn test_postgres_instance_start_without_configure() {
        ansilo_logging::init_for_tests();

        let conf = test_pg_config("start_without_configure");
        assert!(PostgresInstance::start(&conf).await.is_err());
    }

    #[tokio::test]
    async fn test_postgres_instance_configure_then_start() {
        ansilo_logging::init_for_tests();

        let conf = test_pg_config("configure_then_start");
        let instance = PostgresInstance::configure(conf).await.unwrap();
        assert!(instance.server.running());
        drop(instance);

        let instance = PostgresInstance::start(conf).await.unwrap();
        assert!(instance.server.running());
    }
}

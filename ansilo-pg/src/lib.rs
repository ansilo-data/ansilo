use std::time::Duration;

use ansilo_core::err::Result;
use conf::PostgresConf;
use configure::configure;
use connection::{PostgresConnection, PostgresConnectionPool};
use initdb::PostgresInitDb;
use manager::PostgresServerManager;

/// This module orchestrates our postgres instance and provides an api
/// to execute queries against it. Postgres is run as a child process.
///
/// In order for postgres to retrieve data from our sources, the ansilo-pgx
/// extension is installed which creates a FDW which connects back to our
/// ansilo process over a unix socket.
pub mod conf;
pub mod connection;
pub mod initdb;
pub mod manager;
pub mod proc;
pub mod server;

mod configure;
#[cfg(test)]
mod test;
pub mod fdw;

/// Use the default database created by initdb
pub(crate) const PG_DATABASE: &str = "postgres";

/// NOTE: we disable listening over TCP/IP and only connect via unix sockets
/// This is purely used to have a stable unix socket path for connecting
pub(crate) const PG_PORT: u16 = 5432;

/// The username of the super user
pub(crate) const PG_SUPER_USER: &str = "ansilosuperuser";

/// The username of the admin user which can create and modify the schema
pub(crate) const PG_ADMIN_USER: &str = "ansiloadmin";

/// The username of the app user which has DML access to all tables
pub(crate) const PG_APP_USER: &str = "ansiloapp";


/// The entrypoint for managing our postgres instance
#[derive(Debug)]
pub struct PostgresInstance {
    /// The postgres configuration
    conf: PostgresConf,
    /// The server manager
    server: PostgresServerManager,
    /// Connection pools
    pools: PostgresConnectionPools,
}

/// Thread-safe connection pools to access postgres
#[derive(Debug, Clone)]
pub struct PostgresConnectionPools {
    /// The admin user connection pool
    admin: PostgresConnectionPool,
    /// The app user connection pool
    app: PostgresConnectionPool,
}

impl PostgresInstance {
    /// Boots and initialises a new postgres instance based on the
    /// supplied configuration
    pub fn new(conf: PostgresConf) -> Result<Self> {
        let connect_timeout = Duration::from_secs(15);

        // TODO[maybe]: the initdb/configure steps should be handled by PostgresServerManager?
        // in case it needs to be rebuilt on crash?
        PostgresInitDb::reset(&conf)?;
        PostgresInitDb::run(conf.clone())?.complete()?;
        let server = PostgresServerManager::new(conf.clone());

        let superuser_con =
            PostgresConnectionPool::new(&conf, PG_SUPER_USER, PG_DATABASE, 1, 1, connect_timeout)?
                .acquire()?;
        configure(&conf, superuser_con)?;

        // TODO: configurable pool sizes
        Ok(Self {
            conf: conf.clone(),
            server,
            pools: PostgresConnectionPools {
                admin: PostgresConnectionPool::new(
                    &conf,
                    PG_ADMIN_USER,
                    PG_DATABASE,
                    5,
                    10,
                    connect_timeout,
                )?,
                app: PostgresConnectionPool::new(
                    &conf,
                    PG_APP_USER,
                    PG_DATABASE,
                    5,
                    50,
                    connect_timeout,
                )?,
            },
        })
    }

    /// Gets the connection pools for this instance
    pub fn connections(&self) -> &PostgresConnectionPools {
        &self.pools
    }

    /// Gets the configuration for this instance
    pub fn conf(&self) -> &PostgresConf {
        &self.conf
    }

    /// Terminates the postgres instance, waiting for shutdown to complete
    pub fn terminate(&mut self) -> Result<()> {
        self.server.terminate()
    }
}

impl PostgresConnectionPools {
    /// Gets a connection with admin privileges to the database
    /// IMPORTANT: Only use this connection for known queries
    /// and not queries supplied by the user
    pub fn admin(&mut self) -> Result<PostgresConnection> {
        self.admin.acquire()
    }

    /// Gets a connection with standard privileges to the database
    pub fn app(&mut self) -> Result<PostgresConnection> {
        self.app.acquire()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn test_pg_config(test_name: &'static str) -> PostgresConf {
        PostgresConf {
            install_dir: PathBuf::from("/usr/lib/postgresql/14"),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!("/tmp/ansilo-tests/pg-instance/{}", test_name)),
            socket_dir_path: PathBuf::from(format!("/tmp/ansilo-tests/pg-instance/{}", test_name)),
            fdw_socket_path: PathBuf::from("not-used"),
        }
    }

    #[test]
    fn test_postgres_instance_init() {
        ansilo_logging::init_for_tests();

        let conf = test_pg_config("init");
        let instance = PostgresInstance::new(conf).unwrap();

        assert!(instance.server.running());
    }
}

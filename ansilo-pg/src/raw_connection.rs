use std::{fmt::Display, path::PathBuf, sync::Arc, time::Duration};

use ansilo_core::err::{bail, Context, Result};
use postgres::Config;
use r2d2_postgres::r2d2::{ManageConnection, Pool, PooledConnection};
use tokio::{io::AsyncWriteExt, net::UnixStream, runtime::Runtime};

use crate::{
    conf::PostgresConf,
    proto::{
        be::PostgresBackendMessage,
        fe::{PostgresFrontendMessage, PostgresFrontendStartupMessage},
    },
};

/// Postgres connection pool
#[derive(Debug, Clone)]
pub(crate) struct RawPostgresConnectionPool {
    /// The inner r2d2 pool
    pool: Pool<RawPostgresConnectionManager>,
}

impl RawPostgresConnectionPool {
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
                .build(RawPostgresConnectionManager::new(socket_path, pg_conf))
                .context("Failed to create postgres connection pool")?,
        })
    }

    /// Aquires a connection from the pool
    pub fn acquire(&mut self) -> Result<PooledConnection<RawPostgresConnectionManager>> {
        self.pool
            .get()
            .context("Failed to acquire a connection from the connection pool")
    }
}

#[derive(Debug)]
pub struct RawPostgresConnection {
    runtime: Arc<Runtime>,
    stream: UnixStream,
    broken: bool,
}

/// @see https://www.postgresql.org/docs/current/protocol-flow.html
impl RawPostgresConnection {
    fn new(runtime: Runtime, stream: UnixStream) -> Self {
        Self {
            runtime: Arc::new(runtime),
            stream,
            broken: false,
        }
    }

    async fn connect(&mut self, config: Config) -> Result<()> {
        self.send(PostgresFrontendMessage::StartupMessage(
            PostgresFrontendStartupMessage::new(
                [
                    ("client_encoding".into(), "UTF8".into()),
                    ("user".into(), config.get_user().unwrap().into()),
                    ("database".into(), config.get_dbname().unwrap().into()),
                ]
                .into_iter()
                .collect(),
            ),
        ))
        .await?;

        match self.receive().await? {
            PostgresBackendMessage::AuthenticationOk => {}
            msg => bail!("Unexpected response from postgres: {:?}", msg),
        };

        // Now we have authenticated, wait for ReadyForQuery
        loop {
            match self.receive().await? {
                PostgresBackendMessage::ReadyForQuery(_) => break,
                PostgresBackendMessage::Other(msg) if [b'S', b'K', b'N'].contains(&msg.tag()) => {
                    continue;
                }
                msg => bail!("Unexpected response from postgres: {:?}", msg),
            }
        }

        Ok(())
    }

    fn connect_sync(&mut self, config: Config) -> Result<()> {
        self.runtime.clone().block_on(self.connect(config))
    }

    pub async fn send(&mut self, message: PostgresFrontendMessage) -> Result<()> {
        let res = self
            .stream
            .write_all(message.serialise()?.as_slice())
            .await
            .context("Failed to write to unix socket");

        if res.is_err() {
            self.broken = true;
            return res;
        }

        let res = self.stream.flush().await.context("Failed to flush");

        if res.is_err() {
            self.broken = true;
            return res;
        }

        Ok(())
    }

    pub fn send_sync(&mut self, message: PostgresFrontendMessage) -> Result<()> {
        self.runtime.clone().block_on(self.send(message))
    }

    pub async fn receive(&mut self) -> Result<PostgresBackendMessage> {
        let res = PostgresBackendMessage::read(&mut self.stream)
            .await
            .context("Failed to read message from unix socket");

        if res.is_err() {
            self.broken = true;
        }

        res
    }

    pub fn receive_sync(&mut self) -> Result<PostgresBackendMessage> {
        self.runtime.clone().block_on(self.receive())
    }

    pub async fn execute(&mut self, sql: impl Into<String>) -> Result<()> {
        self.send(PostgresFrontendMessage::Query(sql.into()))
            .await
            .context("Failed to execute query")?;

        loop {
            let res = self.receive().await.context("Failed to execute query")?;

            match res {
                // Query complete, we are good to go
                PostgresBackendMessage::ReadyForQuery(_) => break,
                // Query returning data, continue
                PostgresBackendMessage::Other(res)
                    if [b'C', b'T', b'D', b'N'].contains(&res.tag()) =>
                {
                    continue
                }
                // Otherwise...
                _ => bail!("Unexpected response while executing query: {:?}", res),
            }
        }

        Ok(())
    }

    pub fn execute_sync(&mut self, sql: impl Into<String>) -> Result<()> {
        self.runtime.clone().block_on(self.execute(sql))
    }
}

#[derive(Debug)]
pub struct RawPostgresConnectionManager {
    socket_path: PathBuf,
    config: Config,
}

impl RawPostgresConnectionManager {
    pub fn new(socket_path: PathBuf, config: Config) -> Self {
        Self {
            socket_path,
            config,
        }
    }
}

#[derive(Debug)]
pub struct PostgresConnectionPoolError {
    err: ansilo_core::err::Error,
}

impl Display for PostgresConnectionPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.err))
    }
}

impl std::error::Error for PostgresConnectionPoolError {}

impl From<ansilo_core::err::Error> for PostgresConnectionPoolError {
    fn from(err: ansilo_core::err::Error) -> Self {
        Self { err }
    }
}

impl ManageConnection for RawPostgresConnectionManager {
    type Connection = RawPostgresConnection;
    type Error = PostgresConnectionPoolError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create runtime")?;

        let stream = runtime.block_on(async {
            UnixStream::connect(&self.socket_path)
                .await
                .with_context(|| {
                    format!(
                        "Failed to connect to socket: {}",
                        self.socket_path.display()
                    )
                })
        })?;

        let mut con = RawPostgresConnection::new(runtime, stream);

        con.connect_sync(self.config.clone())
            .map_err(Self::Error::from)?;

        Ok(con)
    }

    fn is_valid(&self, con: &mut Self::Connection) -> Result<(), Self::Error> {
        con.execute_sync("SELECT 1").map_err(Self::Error::from)
    }

    fn has_broken(&self, con: &mut Self::Connection) -> bool {
        con.broken
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
                "/tmp/ansilo-tests/pg-raw-connection-pool/data/{}",
                test_name
            )),
            socket_dir_path: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-raw-connection-pool/{}",
                test_name
            )),
            fdw_socket_path: PathBuf::from("not-used"),
        };
        Box::leak(Box::new(conf))
    }

    #[test]
    fn test_postgres_connection_pool_new() {
        let conf = test_pg_config("new");
        let pool = RawPostgresConnectionPool::new(
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
        RawPostgresConnectionPool::new(
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
    fn test_raw_postgres_connection_connect() {
        ansilo_logging::init_for_tests();
        let conf = test_pg_config("raw-connect");
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();
        let mut _server = PostgresServer::boot(conf).unwrap();
        thread::spawn(move || _server.wait());
        thread::sleep(Duration::from_secs(2));

        let mut pg_conf = postgres::Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        let manager = RawPostgresConnectionManager::new(conf.pg_socket_path(), pg_conf);

        let mut con = manager.connect().unwrap();
        con.execute_sync("SELECT 3 + 4").unwrap();
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

        let mut pool = RawPostgresConnectionPool::new(
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

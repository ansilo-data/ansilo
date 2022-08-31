use std::{path::PathBuf, sync::Arc};

use ansilo_core::err::{bail, Context, Result};
use postgres::Config;
use tokio::{io::AsyncWriteExt, net::UnixStream, runtime::Runtime};

use crate::proto::{
    be::PostgresBackendMessage,
    fe::{PostgresFrontendMessage, PostgresFrontendStartupMessage},
};

#[derive(Debug)]
pub struct LlPostgresConnection {
    runtime: Arc<Runtime>,
    stream: UnixStream,
    broken: bool,
}

/// @see https://www.postgresql.org/docs/current/protocol-flow.html
impl LlPostgresConnection {
    pub(crate) fn new(runtime: Runtime, stream: UnixStream) -> Self {
        Self {
            runtime: Arc::new(runtime),
            stream,
            broken: false,
        }
    }

    pub(crate) fn connect(runtime: Runtime, socket_path: PathBuf, config: Config) -> Result<Self> {
        let stream = runtime.block_on(async {
            UnixStream::connect(&socket_path)
                .await
                .with_context(|| format!("Failed to connect to socket: {}", socket_path.display()))
        })?;

        let mut con = Self::new(runtime, stream);

        con.runtime.clone().block_on(con.authenticate(config))?;

        Ok(con)
    }

    async fn authenticate(&mut self, config: Config) -> Result<()> {
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

    pub fn broken(&self) -> bool {
        self.broken
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, thread, time::Duration};

    use crate::{
        conf::PostgresConf, initdb::PostgresInitDb, server::PostgresServer, PG_SUPER_USER,
    };

    use super::*;

    fn test_pg_config(test_name: &'static str) -> &'static PostgresConf {
        let conf = PostgresConf {
            install_dir: PathBuf::from("/home/vscode/.pgx/14.5/pgx-install/"),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!("/tmp/ansilo-tests/pg-ll-connection/{}", test_name)),
            socket_dir_path: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-ll-connection/{}",
                test_name
            )),
            fdw_socket_path: PathBuf::from("not-used"),
        };
        Box::leak(Box::new(conf))
    }

    fn startup_postgres(conf: &'static PostgresConf) {
        ansilo_logging::init_for_tests();
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();
        let mut _server = PostgresServer::boot(conf).unwrap();
        thread::spawn(move || _server.wait());
        let mut i = 0;

        while !conf.pg_socket_path().exists() {
            if i >= 10 {
                panic!("Failed to initialise postgres");
            }

            thread::sleep(Duration::from_secs(1));
            i += 1;
        }
    }

    fn create_connection(conf: &'static PostgresConf, pg_conf: Config) -> LlPostgresConnection {
        let socket_path = conf.pg_socket_path().clone();

        let runtime = tokio::runtime::Runtime::new().unwrap();

        LlPostgresConnection::connect(runtime, socket_path, pg_conf).unwrap()
    }

    #[test]
    fn test_low_level_postgres_connection_connect() {
        let conf = test_pg_config("connect");
        startup_postgres(conf);

        let mut pg_conf = Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        let mut con = create_connection(conf, pg_conf);
        con.execute_sync("SELECT 3 + 4").unwrap();
    }

    #[test]
    fn test_low_level_postgres_connection_invalid_query() {
        let conf = test_pg_config("invalid-query");
        startup_postgres(conf);

        let mut pg_conf = Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        let mut con = create_connection(conf, pg_conf);
        con.execute_sync("INVALID QUERY").unwrap_err();
    }
}

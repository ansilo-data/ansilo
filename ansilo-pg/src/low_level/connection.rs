use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use ansilo_core::err::{bail, Context, Result};
use ansilo_logging::trace;
use tokio::{
    io::AsyncWriteExt,
    net::{
        unix::{OwnedReadHalf, OwnedWriteHalf},
        UnixStream,
    },
};
use tokio_postgres::Config;

use crate::proto::{
    be::{PostgresBackendMessage, PostgresBackendMessageTag},
    common::CancelKey,
    fe::{PostgresFrontendMessage, PostgresFrontendStartupMessage},
};

/// A low-level connection to postgres that operates at the protocol level.
///
/// @see https://www.postgresql.org/docs/current/protocol-flow.html
pub struct LlPostgresConnection {
    shared: Arc<SharedState>,
    reader: PgReader,
    writer: PgWriter,
}

#[derive(Default)]
struct OwnedState {
    initial_parameters: Vec<(String, String)>,
    backend_key_data: Option<CancelKey>,
    recycle_queries: Vec<String>,
}

/// Shared connection state
#[derive(Default)]
struct SharedState {
    broken: AtomicBool,
}

impl SharedState {
    fn set_broken(&self) {
        self.broken.store(true, Ordering::Relaxed)
    }

    fn broken(&self) -> bool {
        self.broken.load(Ordering::Relaxed)
    }

    fn check_broken(&self) -> Result<()> {
        if self.broken() {
            bail!("Connection is broken")
        }

        Ok(())
    }
}

impl LlPostgresConnection {
    /// Creates a new connection over the supplied stream
    pub(crate) fn new(stream: UnixStream) -> Self {
        let owned = OwnedState::default();
        let shared = Arc::new(SharedState::default());
        let (read, write) = stream.into_split();

        Self {
            shared: Arc::clone(&shared),
            reader: PgReader(owned, Arc::clone(&shared), read),
            writer: PgWriter(Arc::clone(&shared), write),
        }
    }

    fn owned(&self) -> &OwnedState {
        &self.reader.0
    }

    fn owned_mut(&mut self) -> &mut OwnedState {
        &mut self.reader.0
    }

    /// Connects to a postgres instance at the supplied socket path
    pub(crate) async fn connect(socket_path: PathBuf, config: Config) -> Result<Self> {
        let stream = UnixStream::connect(&socket_path)
            .await
            .with_context(|| format!("Failed to connect to socket: {}", socket_path.display()))?;

        let mut con = Self::new(stream);

        con.authenticate(config).await?;

        Ok(con)
    }

    /// Sends a startup message to postgres and authenticates as the configured user.
    ///
    /// We only support trust authentication for the postgres backend.
    /// Actual user authentication is taken performed by the `ansilo-auth` crate.
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
            let msg = self.receive().await?;
            match msg {
                PostgresBackendMessage::ReadyForQuery(_) => break,
                PostgresBackendMessage::ParameterStatus(k, v) => {
                    self.owned_mut().initial_parameters.push((k, v));
                    continue;
                }
                PostgresBackendMessage::BackendKeyData(key) => {
                    let _ = self.owned_mut().backend_key_data.insert(key);
                    continue;
                }
                _ if [PostgresBackendMessageTag::NoticeResponse].contains(&msg.tag()?) => {
                    continue;
                }
                msg => bail!("Unexpected response from postgres: {:?}", msg),
            }
        }

        Ok(())
    }

    /// Sends the supplied message to postgres
    pub async fn send(&mut self, message: PostgresFrontendMessage) -> Result<()> {
        self.writer.send(message).await
    }

    /// Receivs a message from the postgres backend
    pub async fn receive(&mut self) -> Result<PostgresBackendMessage> {
        self.reader.receive().await
    }

    /// Executes the supplied query on the postgres connection.
    /// We dont support returning any results from the query, only reporting
    /// if it was successful or not.
    pub async fn execute(&mut self, sql: impl Into<String>) -> Result<()> {
        self.execute_with_responses(sql).await?;
        Ok(())
    }

    /// Executes the supplied query on the postgres connection.
    /// We dont support returning any results from the query, only reporting
    /// if it was successful or not.
    pub async fn execute_with_responses(
        &mut self,
        sql: impl Into<String>,
    ) -> Result<Vec<PostgresBackendMessage>> {
        self.shared.check_broken()?;

        let sql = sql.into();
        trace!("Executing SQL: {}", &sql);

        self.send(PostgresFrontendMessage::Query(sql))
            .await
            .context("Failed to execute query")?;

        let mut responses = vec![];

        loop {
            let msg = self.receive().await.context("Failed to execute query")?;
            responses.push(msg.clone());

            match msg {
                // Query complete, we are good to go
                PostgresBackendMessage::ReadyForQuery(_) => break,
                // Query returning data, continue
                _ if [
                    PostgresBackendMessageTag::CommandComplete,
                    PostgresBackendMessageTag::RowDescription,
                    PostgresBackendMessageTag::DataRow,
                    PostgresBackendMessageTag::ParameterStatus,
                    PostgresBackendMessageTag::NoticeResponse,
                ]
                .contains(&msg.tag()?) =>
                {
                    continue
                }
                // Otherwise...
                _ => bail!("Unexpected response while executing query: {:?}", msg),
            }
        }

        Ok(responses)
    }

    /// Returns whether the connection has been broken.
    pub fn broken(&self) -> bool {
        self.shared.broken()
    }

    /// Marks the connection as broken, disallowing it to be used in future sessions
    pub fn set_broken(&self) {
        self.shared.set_broken()
    }

    /// Gets the initial parameters from the postgres backend
    /// These can be relayed to any client connections as they come in
    pub fn initial_parameters(&self) -> &Vec<(String, String)> {
        &self.owned().initial_parameters
    }

    /// Gets the backend key data associated with this connection
    pub fn backend_key_data(&self) -> &Option<CancelKey> {
        &self.owned().backend_key_data
    }

    /// Splits the connection into a reader and writer which can be used concurrently
    pub fn split<'a>(&'a mut self) -> (&'a mut PgReader, &'a mut PgWriter) {
        (&mut self.reader, &mut self.writer)
    }

    /// Splits the connection into a reader and writer which can be used concurrently
    pub fn into_split(self) -> (PgReader, PgWriter) {
        (self.reader, self.writer)
    }

    /// Recombines a reader and writer into a full connection
    ///
    /// NOTE: it is not checked that these readers and writers
    /// reference the same underlying connection. It is up to the caller
    /// to ensure this.
    pub fn combine(reader: PgReader, writer: PgWriter) -> Self {
        Self {
            shared: Arc::clone(&reader.1),
            reader,
            writer,
        }
    }

    /// Appends the query to use upon recycling the connection
    pub fn add_recycle_queries(&mut self, mut queries: Vec<String>) {
        self.owned_mut().recycle_queries.append(&mut queries);
    }

    pub fn recycle_queries_mut(&mut self) -> &mut Vec<String> {
        &mut self.owned_mut().recycle_queries
    }
}

pub struct PgReader(OwnedState, Arc<SharedState>, OwnedReadHalf);
pub struct PgWriter(Arc<SharedState>, OwnedWriteHalf);

impl PgReader {
    /// Receivs a message from the postgres backend
    pub async fn receive(&mut self) -> Result<PostgresBackendMessage> {
        self.1.check_broken()?;

        let res = PostgresBackendMessage::read(&mut self.2)
            .await
            .context("Failed to read message from unix socket");

        if res.is_err() {
            self.1.set_broken();
        }

        res
    }
}

impl PgWriter {
    /// Sends the supplied message to postgres
    pub async fn send(&mut self, message: PostgresFrontendMessage) -> Result<()> {
        self.0.check_broken()?;

        let res = self
            .1
            .write_all(message.serialise()?.as_slice())
            .await
            .context("Failed to write to unix socket");

        if res.is_err() {
            self.0.set_broken();
            return res;
        }

        let res = self.1.flush().await.context("Failed to flush");

        if res.is_err() {
            self.0.set_broken();
            return res;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env, path::PathBuf, thread, time::Duration};

    use ansilo_core::config::ResourceConfig;
    use tokio_postgres::Config;

    use crate::{
        conf::PostgresConf, initdb::PostgresInitDb, proto::common::PostgresMessage,
        server::PostgresServer, PG_SUPER_USER,
    };

    use super::*;

    fn test_pg_config(test_name: &'static str) -> &'static PostgresConf {
        let conf = PostgresConf {
            resources: ResourceConfig::default(),
            install_dir: PathBuf::from(
                env::var("ANSILO_TEST_PG_DIR")
                    .unwrap_or("/home/vscode/.pgx/15.0/pgx-install/".into()),
            ),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!("/tmp/ansilo-tests/pg-ll-connection/{}", test_name)),
            socket_dir_path: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-ll-connection/{}",
                test_name
            )),
            fdw_socket_path: PathBuf::from("not-used"),
            app_users: vec![],
            init_db_sql: vec![],
        };
        Box::leak(Box::new(conf))
    }

    fn startup_postgres(conf: &'static PostgresConf) -> u32 {
        ansilo_logging::init_for_tests();
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();
        let server = PostgresServer::boot(conf).unwrap();
        let pid = server.pid;
        thread::spawn(move || server.wait());
        let mut i = 0;

        while !conf.pg_socket_path().exists() {
            if i >= 10 {
                panic!("Failed to initialise postgres");
            }

            thread::sleep(Duration::from_secs(1));
            i += 1;
        }

        pid
    }

    async fn create_connection(
        conf: &'static PostgresConf,
        pg_conf: Config,
    ) -> LlPostgresConnection {
        let socket_path = conf.pg_socket_path().clone();

        LlPostgresConnection::connect(socket_path, pg_conf)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_low_level_postgres_connection_auth() {
        let (client, mut server) = UnixStream::pair().unwrap();

        let mut con = LlPostgresConnection::new(client);

        let mut pg_conf = Config::new();
        pg_conf.user("username");
        pg_conf.dbname("db");

        tokio::try_join!(con.authenticate(pg_conf), async move {
            let msg = PostgresFrontendMessage::read_initial(&mut server)
                .await
                .unwrap();

            assert_eq!(
                msg,
                PostgresFrontendMessage::StartupMessage(PostgresFrontendStartupMessage::new(
                    [
                        ("client_encoding".into(), "UTF8".into()),
                        ("user".into(), "username".into()),
                        ("database".into(), "db".into())
                    ]
                    .into_iter()
                    .collect()
                ))
            );

            PostgresBackendMessage::AuthenticationOk
                .write(&mut server)
                .await
                .unwrap();

            PostgresBackendMessage::ReadyForQuery(b'I')
                .write(&mut server)
                .await
                .unwrap();

            server.flush().await.unwrap();
            Ok(())
        })
        .unwrap();
    }

    #[tokio::test]
    async fn test_low_level_postgres_connection_execute_query() {
        let (client, mut server) = UnixStream::pair().unwrap();

        let mut con = LlPostgresConnection::new(client);

        tokio::try_join!(con.execute("Example Query"), async move {
            let msg = PostgresFrontendMessage::read(&mut server).await.unwrap();

            assert_eq!(msg, PostgresFrontendMessage::Query("Example Query".into()));

            PostgresBackendMessage::Other(
                PostgresMessage::build(PostgresBackendMessageTag::CommandComplete as _, |_| Ok(()))
                    .unwrap(),
            )
            .write(&mut server)
            .await
            .unwrap();

            PostgresBackendMessage::ReadyForQuery(b'I')
                .write(&mut server)
                .await
                .unwrap();

            server.flush().await.unwrap();
            Ok(())
        })
        .unwrap();
    }

    // Integration tests against a real postgres...

    #[tokio::test]
    async fn test_low_level_postgres_connection_connect() {
        let conf = test_pg_config("connect");
        startup_postgres(conf);

        let mut pg_conf = Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        let mut con = create_connection(conf, pg_conf).await;
        con.execute("SELECT 3 + 4").await.unwrap();
    }

    #[tokio::test]
    async fn test_low_level_postgres_connection_invalid_query() {
        let conf = test_pg_config("invalid-query");
        startup_postgres(conf);

        let mut pg_conf = Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        let mut con = create_connection(conf, pg_conf).await;
        con.execute("INVALID QUERY").await.unwrap_err();
    }

    #[tokio::test]
    async fn test_low_level_postgres_connection_split() {
        let conf = test_pg_config("connection-split");
        startup_postgres(conf);

        let mut pg_conf = Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        // First split
        let mut con = create_connection(conf, pg_conf).await;
        let (reader, writer) = con.split();

        writer
            .send(PostgresFrontendMessage::Query("SELECT 1".into()))
            .await
            .unwrap();

        assert_eq!(
            reader.receive().await.unwrap().tag().unwrap(),
            PostgresBackendMessageTag::RowDescription
        );
        assert_eq!(
            reader.receive().await.unwrap().tag().unwrap(),
            PostgresBackendMessageTag::DataRow
        );
        assert_eq!(
            reader.receive().await.unwrap().tag().unwrap(),
            PostgresBackendMessageTag::CommandComplete
        );
        assert_eq!(
            reader.receive().await.unwrap().tag().unwrap(),
            PostgresBackendMessageTag::ReadyForQuery
        );

        // Now recombine
        con.execute("SELECT 2").await.unwrap();
    }

    #[tokio::test]
    async fn test_low_level_postgres_connection_into_split() {
        let conf = test_pg_config("connection-into-split");
        startup_postgres(conf);

        let mut pg_conf = Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        // First split
        let con = create_connection(conf, pg_conf).await;
        let (mut reader, mut writer) = con.into_split();

        writer
            .send(PostgresFrontendMessage::Query("SELECT 1".into()))
            .await
            .unwrap();

        assert_eq!(
            reader.receive().await.unwrap().tag().unwrap(),
            PostgresBackendMessageTag::RowDescription
        );
        assert_eq!(
            reader.receive().await.unwrap().tag().unwrap(),
            PostgresBackendMessageTag::DataRow
        );
        assert_eq!(
            reader.receive().await.unwrap().tag().unwrap(),
            PostgresBackendMessageTag::CommandComplete
        );
        assert_eq!(
            reader.receive().await.unwrap().tag().unwrap(),
            PostgresBackendMessageTag::ReadyForQuery
        );

        // Now recombine
        let mut con = LlPostgresConnection::combine(reader, writer);
        con.execute("SELECT 2").await.unwrap();
    }

    #[tokio::test]
    async fn test_low_level_postgres_connection_captures_initial_parameters() {
        let conf = test_pg_config("connect_capture_initial_parameters");
        startup_postgres(conf);

        let mut pg_conf = Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        let con = create_connection(conf, pg_conf).await;

        let params: HashMap<String, String> =
            con.initial_parameters().clone().into_iter().collect();
        assert_eq!(params.get("client_encoding"), Some(&"UTF8".into()));
        assert_eq!(params.get("server_encoding"), Some(&"UTF8".into()));
        assert_eq!(params.get("TimeZone"), Some(&"Etc/UTC".into()));
    }

    #[tokio::test]
    async fn test_low_level_postgres_connection_captures_backend_key_data() {
        let conf = test_pg_config("connect_capture_backend_key_data");
        startup_postgres(conf);

        let mut pg_conf = Config::new();
        pg_conf.user(PG_SUPER_USER);
        pg_conf.dbname("postgres");

        let con = create_connection(conf, pg_conf).await;

        assert_ne!(con.backend_key_data().clone().unwrap().pid, 0);
        assert_ne!(con.backend_key_data().clone().unwrap().key, 0);
    }
}

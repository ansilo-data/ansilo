mod auth;

use std::collections::{HashMap, HashSet};

use crate::{
    low_level::{
        connection::{PgReader, PgWriter},
        pool::AppPostgresConnection,
    },
    proto::{
        be::PostgresBackendMessage,
        common::CancelKey,
        fe::{PostgresFrontendMessage, PostgresFrontendStartupMessage},
    },
    PostgresConnectionPools,
};
use ansilo_auth::Authenticator;
use ansilo_core::err::{Context, Result};
use ansilo_logging::{debug, warn};
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};
use ansilo_util_pg::query::{pg_quote_identifier, pg_str_literal};
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::distributions::{Alphanumeric, DistString};
use tokio::{
    io::{AsyncWriteExt, ReadHalf, WriteHalf},
    net::UnixStream,
    sync::Mutex,
};

/// Request handler for postgres-wire-protocol connections
pub struct PostgresConnectionHandler {
    authenticator: Authenticator,
    pool: PostgresConnectionPools,
    cancel_keys: Mutex<HashMap<CancelKey, CancelKey>>,
}

impl PostgresConnectionHandler {
    pub fn new(authenticator: Authenticator, pool: PostgresConnectionPools) -> Self {
        Self {
            authenticator,
            pool,
            cancel_keys: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ConnectionHandler for PostgresConnectionHandler {
    async fn handle(&self, mut client: Box<dyn IOStream>) -> Result<()> {
        // Get the initial request
        let request = PostgresFrontendMessage::read_initial(&mut client)
            .await
            .context("Failed to read initial request")?;

        match request {
            PostgresFrontendMessage::StartupMessage(startup) => {
                self.handle_connection(client, startup).await
            }
            PostgresFrontendMessage::CancelRequest(cancel) => {
                self.handle_cancel(client, cancel).await
            }
            _ => unreachable!(),
        }
    }
}

impl PostgresConnectionHandler {
    /// Handles a connection from a client
    async fn handle_connection(
        &self,
        mut client: Box<dyn IOStream>,
        startup: PostgresFrontendStartupMessage,
    ) -> Result<()> {
        // Authenticate the client
        let auth = Self::authenticate_postgres(&self.authenticator, &mut client, &startup).await?;

        // Now that we have authenticated, we acquire a connection to to postgres
        let mut con = self.pool.app(&auth.username).await?;

        // Set the authentication context with a new reset token
        // TODO[SEC]: The reset token cannot be made available to the client,
        // otherwise they could potentially change their auth context and hence
        // escalate their privileges.
        // Ideally, in future we should set the auth context "out-of-band" of the main connection
        // through some form of IPC to eliminate this possibility categorically.
        let reset_token = pg_str_literal(&Alphanumeric.sample_string(&mut rand::thread_rng(), 32));
        let auth_context = pg_str_literal(
            &serde_json::to_string(&auth).context("Failed to serialise auth context")?,
        );
        con.execute(format!(
            "SELECT __ansilo_auth.ansilo_set_auth_context({auth_context}, {reset_token})"
        ))
        .await?;

        // We ensure the connection is clean when it is next recycled
        con.add_recycle_queries(vec![
            // Ensure the auth context is appropriately reset
            format!("SELECT __ansilo_auth.ansilo_reset_auth_context({reset_token})"),
            // Clean any other temporary state
            "DISCARD ALL".into(),
        ]);

        // Generate a new cancel key and send it to the client
        // Record it against the connection's key to support cancelling
        // requests
        let client_key = Self::random_cancel_key()?;
        if let Some(con_key) = con.backend_key_data().as_ref() {
            PostgresBackendMessage::BackendKeyData(client_key.clone())
                .write(&mut client)
                .await
                .context("Failed to send backend key data")?;
            let mut sessions = self.cancel_keys.lock().await;
            // TODO[low]: ensure this is removed on return
            sessions.insert(client_key.clone(), con_key.clone());
        }

        // Forward startup parameters from the client connection
        Self::set_client_parameters(&mut con, &mut client, startup)
            .await
            .context("Failed to set client connection parameters")?;

        // We now inform the client that we are ready to accept queries
        PostgresBackendMessage::ReadyForQuery(b'I')
            .write(&mut client)
            .await
            .context("Failed to send ready for query")?;
        client
            .flush()
            .await
            .context("Failed to send ready for query")?;

        // Start proxying messages between the client and the server
        let (mut client_reader, mut client_writer) = tokio::io::split(client);
        let (mut pg_reader, mut pg_writer) = con.split();

        match Self::proxy(
            &mut client_reader,
            &mut client_writer,
            &mut pg_reader,
            &mut pg_writer,
        )
        .await
        {
            Ok(_) => debug!("Postgres connection closed gracefully"),
            Err(err) => {
                warn!("Error during postgres connection: {:?}", err);
                let _ = PostgresBackendMessage::error_msg(format!("{}", err))
                    .write(&mut client_writer)
                    .await;
            }
        }

        // Remove the client key from the map
        {
            let mut sessions = self.cancel_keys.lock().await;
            sessions.remove(&client_key);
        }

        // Now that the session has finished, we attempt to clean the connection
        // to free up any temporary tables, transactions or other state.
        if let Err(err) = con.execute("DISCARD ALL").await {
            warn!("Error while cleaning connection: {:?}", err);
            con.set_broken();
        }

        // The session is complete, we drop the connection which should return it to the pool
        Ok(())
    }

    /// Handles a cancel request from a client
    async fn handle_cancel(&self, _client: Box<dyn IOStream>, client_key: CancelKey) -> Result<()> {
        // Remove the key from the sessions map
        // If it is not present abort early
        let con_key = {
            let mut sessions = self.cancel_keys.lock().await;
            match sessions.remove(&client_key) {
                Some(k) => k,
                None => return Ok(()),
            }
        };

        // The key is valid, try cancel the query
        let mut con = UnixStream::connect(self.pool.conf().pg_socket_path())
            .await
            .context("Failed to cancel request")?;

        PostgresFrontendMessage::CancelRequest(con_key)
            .write(&mut con)
            .await
            .context("Failed to cancel request")?;

        Ok(())
    }

    /// Forwards the session local connection parameters from the client to the server.
    ///
    /// The parameters are reset by "DISCARD ALL" when the connection is recycled.
    ///
    /// @see https://www.postgresql.org/docs/current/runtime-config-client.html
    /// @see https://www.postgresql.org/docs/current/config-setting.html
    async fn set_client_parameters(
        con: &mut AppPostgresConnection,
        client: &mut Box<dyn IOStream>,
        startup: PostgresFrontendStartupMessage,
    ) -> Result<()> {
        let params = startup
            .params
            .iter()
            .filter(|(k, _)| ALLOWED_CLIENT_PARAMS.contains(k.as_str()))
            .collect::<Vec<(&String, &String)>>();

        if params.is_empty() {
            return Ok(());
        }

        let query = params
            .into_iter()
            .map(|(k, v)| {
                format!(
                    "SET SESSION {} = {};",
                    pg_quote_identifier(k),
                    pg_str_literal(v)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let responses = con
            .execute_with_responses(query)
            .await
            .context("Failed to set connection parameters")?;

        // First send the initial parameter statuses back to the client
        for (key, value) in con.initial_parameters().iter().cloned() {
            if ALLOWED_SERVER_PARAMS.contains(key.as_str()) {
                PostgresBackendMessage::ParameterStatus(key, value)
                    .write(client)
                    .await
                    .context("Failed to send parameter status")?;
            }
        }

        // Send any parameter status messages back to the client
        // to acknowledge their request
        for res in responses {
            if matches!(res, PostgresBackendMessage::ParameterStatus(_, _)) {
                res.write(client)
                    .await
                    .context("Failed to send parameter status")?;
            }
        }

        Ok(())
    }

    /// Perfoms bi-directional proxying of messages between the client (frontend) and the server (backend)
    async fn proxy(
        client_reader: &mut ReadHalf<Box<dyn IOStream>>,
        client_writer: &mut WriteHalf<Box<dyn IOStream>>,
        pg_reader: &mut PgReader,
        pg_writer: &mut PgWriter,
    ) -> Result<()> {
        // Task for forwarding messages from the client to postgres
        let input = async move {
            loop {
                let msg = PostgresFrontendMessage::read(client_reader).await?;

                // If the client sends a terminate message we dont want
                // to actually close the connection since then it cannot be
                // recycled for future sessions.
                // Instead we use this as a signal to stop the proxying and
                // end the session.
                if msg == PostgresFrontendMessage::Terminate {
                    break;
                }

                pg_writer.send(msg).await?;
            }

            Result::<()>::Ok(())
        };

        // Reverse task for forwarding the messages from postgres to the client
        let output = async move {
            loop {
                let msg = pg_reader.receive().await?;
                msg.write(client_writer).await?;
                client_writer.flush().await?;
            }

            #[allow(unreachable_code)]
            Result::<()>::Ok(())
        };

        // Perform both tasks concurrently and, importantly,
        // finish both tasks as soon as either one ends.
        tokio::select! {
            res = input => res?,
            res = output => res?,
        };

        Ok(())
    }

    /// Generate a random cancel key for each client
    /// We dont want to expose the real cancel keys as these
    /// are reused across clients, and we dont want one client
    /// attempting to cancel a request of another.
    fn random_cancel_key() -> Result<CancelKey> {
        let pid = rand::random();
        let key = rand::random();

        Ok(CancelKey { pid, key })
    }
}

lazy_static! {
    /// @see https://www.postgresql.org/docs/current/libpq-status.html#LIBPQ-PQPARAMETERSTATUS
    static ref ALLOWED_SERVER_PARAMS: HashSet<&'static str> = HashSet::from([
        "client_encoding",
        "DateStyle",
        "default_transaction_read_only",
        "integer_datetimes",
        "IntervalStyle",
        "server_encoding",
        "server_version",
        "standard_conforming_strings",
        "TimeZone"
    ]);

    /// @see https://www.postgresql.org/docs/current/runtime-config-client.html
    static ref ALLOWED_CLIENT_PARAMS: HashSet<&'static str> = HashSet::from([
        "application_name",
        "client_min_messages",
        "search_path",
        "row_security",
        "default_table_access_method",
        "default_tablespace",
        "default_toast_compression",
        "temp_tablespaces",
        "check_function_bodies",
        "default_transaction_isolation",
        "default_transaction_read_only",
        "default_transaction_deferrable",
        "transaction_isolation",
        "transaction_read_only",
        "transaction_deferrable",
        "session_replication_role",
        "statement_timeout",
        "log_min_error_statement",
        "statement_timeout",
        "lock_timeout",
        "statement_timeout",
        "lock_timeout",
        "log_min_error_statement",
        "idle_in_transaction_session_timeout",
        "idle_session_timeout",
        "bytea_output",
        "xmlbinary",
        "xmloption",
        "gin_pending_list_limit",
        "DateStyle",
        "lc_time",
        "IntervalStyle",
        "DateStyle",
        "IntervalStyle",
        "TimeZone",
        "timezone_abbreviations",
        "extra_float_digits",
        "client_encoding",
        "lc_messages",
        "lc_monetary",
        "lc_numeric",
        "default_text_search_config",
        "lc_ctype",
    ]);
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{conf::PostgresConf, PostgresInstance};
    use ansilo_core::{
        auth::{AuthContext, PasswordAuthContext, ProviderAuthContext},
        config::{AuthConfig, PasswordUserConfig, UserConfig, UserTypeOptions},
        err::Error,
    };
    use ansilo_proxy::stream::Stream;
    use tokio::net::UnixStream;
    use tokio_postgres::NoTls;

    use super::*;

    fn mock_password_auth() -> Authenticator {
        let conf = Box::leak(Box::new(AuthConfig {
            providers: vec![],
            users: vec![
                UserConfig {
                    username: "test_user".into(),
                    description: None,
                    provider: None,
                    r#type: UserTypeOptions::Password(PasswordUserConfig {
                        password: "pass123".into(),
                    }),
                },
                UserConfig {
                    username: "another_user".into(),
                    description: None,
                    provider: None,
                    r#type: UserTypeOptions::Password(PasswordUserConfig {
                        password: "luna456".into(),
                    }),
                },
            ],
            service_users: vec![],
        }));

        Authenticator::init(conf).unwrap()
    }

    async fn init_pg(test_name: &'static str) -> PostgresInstance {
        // This runs blocking code and contains a runtime
        let conf = Box::leak(Box::new(PostgresConf {
            install_dir: PathBuf::from(
                std::env::var("ANSILO_TEST_PG_DIR").unwrap_or("/usr/lib/postgresql/14".into()),
            ),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!("/tmp/ansilo-tests/main-pg-handler/{}", test_name)),
            socket_dir_path: PathBuf::from(format!(
                "/tmp/ansilo-tests/main-pg-handler/{}",
                test_name
            )),
            fdw_socket_path: PathBuf::from("not-used"),
            app_users: vec!["test_user".into(), "another_user".into()],
            init_db_sql: vec![],
        }));

        PostgresInstance::configure(conf).await.unwrap()
    }

    fn init_client_stream() -> (UnixStream, Box<dyn IOStream>) {
        let (a, b) = UnixStream::pair().unwrap();

        (a, Box::new(Stream(b)))
    }

    async fn init_handler(
        test_name: &'static str,
        auth: Authenticator,
    ) -> (PostgresInstance, PostgresConnectionHandler) {
        let mut pg = init_pg(test_name).await;

        let handler = PostgresConnectionHandler::new(auth, pg.connections().clone());

        (pg, handler)
    }

    #[tokio::test]
    async fn test_basic_query() {
        ansilo_logging::init_for_tests();
        let auth = mock_password_auth();
        let (_pg, handler) = init_handler("basic-query", auth).await;

        let (client, stream) = init_client_stream();

        let fut_client = async move {
            let (client, con) = tokio_postgres::Config::new()
                .user("test_user")
                .password("pass123")
                .connect_raw(client, NoTls)
                .await?;
            tokio::spawn(con);

            let res: String = client.query_one("SELECT 'Hello pg'", &[]).await?.get(0);

            Result::<_, Error>::Ok(res)
        };
        let fut_handler = handler.handle(stream);

        let (res_client, res_handler) = tokio::join!(fut_client, fut_handler);

        res_handler.unwrap();
        let res_client = res_client.unwrap();
        assert_eq!(res_client, "Hello pg");
    }

    #[tokio::test]
    async fn test_auth_incorrect_password() {
        ansilo_logging::init_for_tests();
        let auth = mock_password_auth();
        let (_pg, handler) = init_handler("invalid-pass", auth).await;

        let (client, stream) = init_client_stream();

        let fut_client = async move {
            let res = tokio_postgres::Config::new()
                .user("test_user")
                .password("wrong")
                .connect_raw(client, NoTls)
                .await;

            res
        };
        let fut_handler = handler.handle(stream);

        let (res_client, res_handler) = tokio::join!(fut_client, fut_handler);

        assert_eq!(
            res_handler.err().unwrap().to_string(),
            "Incorrect password".to_string()
        );
        assert_eq!(
            res_client.err().unwrap().to_string(),
            "db error: ERROR: Incorrect password".to_string()
        );
    }

    #[tokio::test]
    async fn test_auth_context() {
        ansilo_logging::init_for_tests();
        let auth = mock_password_auth();
        let (_pg, handler) = init_handler("auth-context", auth).await;

        let (client, stream) = init_client_stream();

        let fut_client = async move {
            let (client, con) = tokio_postgres::Config::new()
                .user("test_user")
                .password("pass123")
                .connect_raw(client, NoTls)
                .await?;
            tokio::spawn(con);

            let json: serde_json::Value =
                client.query_one("SELECT auth_context()", &[]).await?.get(0);
            let ctx: AuthContext = serde_json::from_value(json)?;

            Result::<_, Error>::Ok(ctx)
        };
        let fut_handler = handler.handle(stream);

        let (res_client, res_handler) = tokio::join!(fut_client, fut_handler);

        res_handler.unwrap();
        let res_client = res_client.unwrap();
        assert_eq!(res_client.username, "test_user");
        assert_eq!(
            res_client.more,
            ProviderAuthContext::Password(PasswordAuthContext {})
        );

        // Test second connection to ensure, gets reset when using recycled connection
        let (client, stream) = init_client_stream();

        let fut_client = async move {
            let (client, con) = tokio_postgres::Config::new()
                .user("another_user")
                .password("luna456")
                .connect_raw(client, NoTls)
                .await?;
            tokio::spawn(con);

            let json: serde_json::Value =
                client.query_one("SELECT auth_context()", &[]).await?.get(0);
            let ctx: AuthContext = serde_json::from_value(json)?;

            Result::<_, Error>::Ok(ctx)
        };
        let fut_handler = handler.handle(stream);

        let (res_client, res_handler) = tokio::join!(fut_client, fut_handler);

        res_handler.unwrap();
        let res_client = res_client.unwrap();
        assert_eq!(res_client.username, "another_user");
        assert_eq!(
            res_client.more,
            ProviderAuthContext::Password(PasswordAuthContext {})
        );
    }

    #[tokio::test]
    async fn test_client_receives_initial_server_parameters() {
        ansilo_logging::init_for_tests();
        let auth = mock_password_auth();
        let (_pg, handler) = init_handler("server-params", auth).await;

        let (client, stream) = init_client_stream();

        tokio::spawn(async move { handler.handle(stream).await });

        let (_client, con) = tokio_postgres::Config::new()
            .user("test_user")
            .password("pass123")
            .connect_raw(client, NoTls)
            .await
            .unwrap();

        assert_eq!(con.parameter("client_encoding"), Some("UTF8"));
        assert_eq!(con.parameter("server_encoding"), Some("UTF8"));
        assert!(con.parameter("server_version").is_some());
    }

    #[tokio::test]
    async fn test_client_parameters_with_reset() {
        ansilo_logging::init_for_tests();
        let auth = mock_password_auth();
        let (_pg, handler) = init_handler("client-params", auth).await;

        let (client, stream) = init_client_stream();

        let fut_client = async move {
            let (client, con) = tokio_postgres::Config::new()
                .user("test_user")
                .password("pass123")
                .application_name("my_custom_app")
                .connect_raw(client, NoTls)
                .await?;
            tokio::spawn(con);

            Result::<_, Error>::Ok(
                client
                    .query_one("SHOW application_name", &[])
                    .await?
                    .get::<_, String>(0),
            )
        };
        let fut_handler = handler.handle(stream);

        let (res_client, res_handler) = tokio::join!(fut_client, fut_handler);

        res_handler.unwrap();
        assert_eq!(res_client.unwrap(), "my_custom_app");

        // Test second connection to ensure, can get updated recycled connection
        let (client, stream) = init_client_stream();

        let fut_client = async move {
            let (client, con) = tokio_postgres::Config::new()
                .user("test_user")
                .password("pass123")
                .connect_raw(client, NoTls)
                .await?;
            tokio::spawn(con);

            Result::<_, Error>::Ok(
                client
                    .query_one("SHOW application_name", &[])
                    .await?
                    .get::<_, String>(0),
            )
        };
        let fut_handler = handler.handle(stream);

        let (res_client, res_handler) = tokio::join!(fut_client, fut_handler);

        res_handler.unwrap();
        assert_eq!(res_client.unwrap(), "");
    }

    #[tokio::test]
    async fn test_cancel_query() {
        ansilo_logging::init_for_tests();
        let auth = mock_password_auth();
        let (_pg, handler) = init_handler("cancel-query", auth).await;

        let (client, stream) = init_client_stream();
        let (cancel_client, cancel_stream) = init_client_stream();

        let fut_client = async move {
            let (client, con) = tokio_postgres::Config::new()
                .user("test_user")
                .password("pass123")
                .application_name("my_custom_app")
                .connect_raw(client, NoTls)
                .await?;
            tokio::spawn(con);

            let cancel_token = client.cancel_token();

            tokio::join!(
                async move {
                    cancel_token
                        .cancel_query_raw(cancel_client, NoTls)
                        .await
                        .unwrap();
                },
                async move {
                    let err = client
                        .batch_execute("SELECT pg_sleep(10)")
                        .await
                        .unwrap_err();

                    dbg!(err.to_string());
                    assert!(err
                        .to_string()
                        .contains("canceling statement due to user request"));
                }
            );

            Result::<_, Error>::Ok(())
        };
        let fut_handler = handler.handle(stream);
        let fut_handler_cancel = handler.handle(cancel_stream);

        tokio::try_join!(fut_client, fut_handler, fut_handler_cancel).unwrap();
    }
}

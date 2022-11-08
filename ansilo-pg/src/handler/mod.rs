mod auth;
mod service_user;
#[cfg(any(test, feature = "test"))]
#[allow(unused)]
pub mod test;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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
#[derive(Clone)]
pub struct PostgresConnectionHandler {
    authenticator: Authenticator,
    pool: PostgresConnectionPools,
    cancel_keys: Arc<Mutex<HashMap<CancelKey, CancelKey>>>,
}

impl PostgresConnectionHandler {
    pub fn new(authenticator: Authenticator, pool: PostgresConnectionPools) -> Self {
        Self {
            authenticator,
            pool,
            cancel_keys: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn pool(&self) -> &PostgresConnectionPools {
        &self.pool
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
                self.handle_connection(client, startup, None).await
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
        client: Box<dyn IOStream>,
        startup: PostgresFrontendStartupMessage,
        service_user_id: Option<String>,
    ) -> Result<()> {
        let mut session = ProxySession::new(&self, client, startup, service_user_id);

        // Process the session, and regardless of the result
        // run the clean up procedures
        let sess_res = session.process().await;
        let term_res = session.terminate().await;

        sess_res.and(term_res)
    }

    /// Handles a cancel request from a client
    async fn handle_cancel(&self, _client: Box<dyn IOStream>, client_key: CancelKey) -> Result<()> {
        // Remove the key from the sessions map
        // If it is not present we dont need to do anything
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
}

/// A session where we proxy between the client and postgres
pub(crate) struct ProxySession<'a> {
    /// Reference to the main handler
    handler: &'a PostgresConnectionHandler,
    /// The connection to the client
    client: Option<Box<dyn IOStream>>,
    /// The initial startup message from the client
    startup: PostgresFrontendStartupMessage,
    /// The connection to postgres
    con: Option<AppPostgresConnection>,
    /// The auth context reset token
    auth_reset_token: Option<String>,
    /// The cancel key given to this client
    cancel_key: Option<CancelKey>,
    /// The authenticating service user id, if any
    service_user_id: Option<String>,
    /// Terminated
    terminated: bool,
}

impl<'a> ProxySession<'a> {
    fn new(
        handler: &'a PostgresConnectionHandler,
        client: Box<dyn IOStream>,
        startup: PostgresFrontendStartupMessage,
        service_user_id: Option<String>,
    ) -> Self {
        Self {
            handler,
            client: Some(client),
            startup,
            con: None,
            auth_reset_token: None,
            cancel_key: None,
            service_user_id,
            terminated: false,
        }
    }

    /// Runs the session
    async fn process(&mut self) -> Result<()> {
        let mut client = self.client.take().context("Session already processed")?;

        // Authenticate the client
        let auth = Self::authenticate_postgres(
            &self.handler.authenticator,
            &mut client,
            &self.startup,
            self.service_user_id.clone(),
        )
        .await?;

        // Generate reset tokens and cancel keys
        let reset_token = self.auth_reset_token()?.clone();
        let cancel_key = self.cancel_key()?.clone();
        let startup = self.startup.clone();

        // Now that we have authenticated, we acquire a connection to postgres
        self.con = Some(self.handler.pool.app(&auth.username).await?);
        let mut con = self.con.as_mut().unwrap();

        // Set the authentication context with a new reset token
        // TODO[SEC]: The reset token cannot be made available to the client,
        // otherwise they could potentially change their auth context and hence
        // escalate their privileges.
        // Ideally, in future we should set the auth context "out-of-band" of the main connection
        // through some form of IPC to eliminate this possibility categorically.
        let reset_token = pg_str_literal(&reset_token);
        let auth_context = pg_str_literal(
            &serde_json::to_string(&auth).context("Failed to serialise auth context")?,
        );
        con.execute(format!(
            "SELECT __ansilo_auth.ansilo_set_auth_context({auth_context}, {reset_token})"
        ))
        .await?;

        // Generate a new cancel key and send it to the client
        // Record it against the connection's key to support cancel requests
        if let Some(con_key) = con.backend_key_data().as_ref() {
            PostgresBackendMessage::BackendKeyData(cancel_key.clone())
                .write(&mut client)
                .await
                .context("Failed to send backend key data")?;
            client
                .flush()
                .await
                .context("Failed to send backend key data")?;

            let mut sessions = self.handler.cancel_keys.lock().await;
            sessions.insert(cancel_key.clone(), con_key.clone());
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

        // The session is complete
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

        let responses = if !params.is_empty() {
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

            con.execute_with_responses(query)
                .await
                .context("Failed to set connection parameters")?
        } else {
            vec![]
        };

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

    /// Generate a random auth reset token.
    /// The reset token cannot be made available to the client,
    /// otherwise they could potentially change their auth context and hence
    /// escalate their privileges.
    fn auth_reset_token(&mut self) -> Result<&String> {
        Ok(self
            .auth_reset_token
            .get_or_insert_with(|| Alphanumeric.sample_string(&mut rand::thread_rng(), 32)))
    }

    /// Generate a random cancel key for each client
    /// We dont want to expose the real cancel keys as these
    /// are reused across clients, and we dont want one client
    /// attempting to cancel a request of another.
    fn cancel_key(&mut self) -> Result<&CancelKey> {
        Ok(self.cancel_key.get_or_insert_with(|| {
            let pid = rand::random();
            let key = rand::random();

            CancelKey { pid, key }
        }))
    }

    /// Terminate the session and clean up.
    /// IMPORTANT: This must be run after the session completes
    /// as we perform clean up tasks that need to run for security.
    async fn terminate(&mut self) -> Result<()> {
        // Get the postgres connection, if any
        // If none we return as we were never able to acquire a connection.
        let con = match self.con.as_mut() {
            Some(c) => c,
            None => {
                self.terminated = true;
                return Ok(());
            }
        };

        // Remove the session's cancel key from the map
        // This must be done in order to prevent the cancel key
        // being misused against
        if let Some(cancel_key) = self.cancel_key.as_ref() {
            let mut sessions = self.handler.cancel_keys.lock().await;
            sessions.remove(cancel_key);
        }

        // Now that the session has finished, we attempt to clean the connection
        // to free up any temporary tables, transactions or other state.
        if !con.broken() {
            if let Err(err) = con.execute("ROLLBACK").await {
                warn!("Error while cleaning connection: {:?}", err);
                con.set_broken();
            }

            if let Err(err) = con.execute("DISCARD ALL").await {
                warn!("Error while cleaning connection: {:?}", err);
                con.set_broken();
            }
        }

        // Reset the auth context so the connection be recycled with a new client.
        if !con.broken() {
            if let Some(reset_token) = self.auth_reset_token.as_ref() {
                let reset_token = pg_str_literal(reset_token);
                if let Err(err) = con
                    .execute(format!(
                        "SELECT __ansilo_auth.ansilo_reset_auth_context({reset_token})"
                    ))
                    .await
                {
                    warn!("Error while resetting auth context: {:?}", err);
                    con.set_broken();
                }
            }
        }

        self.terminated = true;
        Ok(())
    }
}

impl<'a> Drop for ProxySession<'a> {
    fn drop(&mut self) {
        if !self.terminated {
            warn!("Session dropped without calling terminate");
            if let Some(con) = self.con.as_ref() {
                con.set_broken();
            }
        }
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
    use std::time::Duration;

    use ansilo_core::{
        auth::{AuthContext, PasswordAuthContext, ProviderAuthContext},
        err::Error,
    };
    use tokio_postgres::NoTls;

    use super::test::*;
    use super::*;

    #[tokio::test]
    async fn test_basic_query() {
        ansilo_logging::init_for_tests();
        let auth = mock_password_auth_default();
        let (_pg, handler) = init_pg_handler("basic-query", auth).await;

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
        let auth = mock_password_auth_default();
        let (_pg, handler) = init_pg_handler("invalid-pass", auth).await;

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
        let auth = mock_password_auth_default();
        let (_pg, handler) = init_pg_handler("auth-context", auth).await;

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
        let auth = mock_password_auth_default();
        let (_pg, handler) = init_pg_handler("server-params", auth).await;

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
        let auth = mock_password_auth_default();
        let (_pg, handler) = init_pg_handler("client-params", auth).await;

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
        let auth = mock_password_auth_default();
        let (_pg, handler) = init_pg_handler("cancel-query", auth).await;

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

        // Ensure cancel keys get cleaned up
        let cancel_keys = handler.cancel_keys.lock().await;
        assert_eq!(cancel_keys.len(), 0);
    }

    #[tokio::test]
    async fn test_connection_clean_up_after_session_error() {
        ansilo_logging::init_for_tests();
        let auth = mock_password_auth_default();
        let (_pg, handler) = init_pg_handler("clean-up-error", auth).await;

        let (client, stream) = init_client_stream();

        let fut_client = async move {
            let (_client, con) = tokio_postgres::Config::new()
                .user("test_user")
                .password("pass123")
                .connect_raw(client, NoTls)
                .await?;
            // Trigger error by dropping the connection after one second
            let _ = tokio::time::timeout(Duration::from_secs(1), con).await;

            Result::<_, Error>::Ok(())
        };
        let fut_handler = handler.handle(stream);

        tokio::try_join!(fut_client, fut_handler).unwrap();

        // Ensure auth context cleaned up
        handler
            .pool
            .app("test_user")
            .await
            .unwrap()
            .execute(
                r#"
                DO $$BEGIN
                    ASSERT auth_context() IS NULL;
                END$$;
                "#,
            )
            .await
            .unwrap();
        // Ensure cancel keys get cleaned up
        let cancel_keys = handler.cancel_keys.lock().await;
        assert_eq!(cancel_keys.len(), 0);
    }
}

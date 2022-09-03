use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
};

use ansilo_auth::{ctx::AuthContext, Authenticator};
use ansilo_core::err::{Context, Result};
use ansilo_logging::{debug, warn};
use ansilo_pg::{
    low_level::{
        connection::{PgReader, PgWriter},
        pool::AppPostgresConnection,
    },
    proto::{
        be::PostgresBackendMessage,
        fe::{PostgresFrontendMessage, PostgresFrontendStartupMessage},
    },
    query::{pg_quote_identifier, pg_str_literal},
    PostgresConnectionPools,
};
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::distributions::{Alphanumeric, DistString};
use tokio::io::{ReadHalf, WriteHalf};

/// Handler for postgres-wire-protocol connections
pub struct PostgresConnectionHandler {
    authenticator: Authenticator,
    pool: PostgresConnectionPools,
}

impl PostgresConnectionHandler {
    pub fn new(authenticator: Authenticator, pool: PostgresConnectionPools) -> Self {
        Self {
            authenticator,
            pool,
        }
    }
}

#[async_trait]
impl ConnectionHandler for PostgresConnectionHandler {
    async fn handle(&self, mut client: Box<dyn IOStream>) -> Result<()> {
        // Authenticate the client
        let (auth, startup) = self
            .authenticator
            .authenticate_postgres(&mut client)
            .await?;

        // Now that we have authenticated, we acquire a connection to to postgres
        let mut con = self.pool.app(&auth.username).await?;

        // Set the authentication context with a new reset token
        // SEC: The reset token cannot be made available to the client,
        // otherwise they could potentially change their auth context and hence
        // escalate their privilges.
        // Ideally, in future we should set the auth context "out-of-band" of the main connection
        // through some form of IPC to eliminate this possibility categorically.
        let reset_token = pg_str_literal(&Alphanumeric.sample_string(&mut rand::thread_rng(), 32));
        let auth_context = pg_str_literal(
            &serde_json::to_string(&auth).context("Failed to serialise auth context")?,
        );
        con.execute(format!(
            "SELECT ansilo_set_auth_context({auth_context}, {reset_token})"
        ))
        .await?;

        // We ensure the connection is clean when it is next recycled
        con.recycle_queries(vec![
            // Ensure the auth context is appropriately reset
            format!("SELECT ansilo_reset_auth_context({reset_token})"),
            // Clean any other temporary state
            "DISCARD ALL".into(),
        ]);

        // Forward startup parameters from the client connection
        Self::set_client_parameters(&mut con, startup)
            .await
            .context("Failed to set client connection parameters")?;

        // We now inform the client that we are ready to accept queries
        PostgresBackendMessage::ReadyForQuery(b'I')
            .write(&mut client)
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
                let _ = PostgresBackendMessage::ErrorResponse(format!("Error: {}", err))
                    .write(&mut client_writer)
                    .await;
            }
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
}

lazy_static! {
    /// @see https://www.postgresql.org/docs/current/runtime-config-client.html
    static ref ALLOWED_PARAMS: HashSet<&'static str> = HashSet::from([
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

impl PostgresConnectionHandler {
    /// Forwards the session local connection parameters from the client to the server.
    ///
    /// The parameters are reset by "DISCARD ALL" when the connection is recycled.
    ///
    /// @see https://www.postgresql.org/docs/current/runtime-config-client.html
    /// @see https://www.postgresql.org/docs/current/config-setting.html
    async fn set_client_parameters(
        con: &mut AppPostgresConnection,
        startup: PostgresFrontendStartupMessage,
    ) -> Result<()> {
        let params = startup
            .params
            .iter()
            .filter(|(k, _)| ALLOWED_PARAMS.contains(k.as_str()))
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

        con.execute(query)
            .await
            .context("Failed to set connection parameters")
    }

    // Perfoms bi-directional proxying of messages between the client (frontend) and the server (backend)
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
                // recycled for future connects.
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
}

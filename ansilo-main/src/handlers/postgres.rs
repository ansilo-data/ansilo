use ansilo_auth::Authenticator;
use ansilo_core::err::{Context, Result};
use ansilo_logging::{debug, warn};
use ansilo_pg::{
    low_level::connection::{PgReader, PgWriter},
    proto::{be::PostgresBackendMessage, fe::PostgresFrontendMessage},
    PostgresConnectionPools,
};
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};
use async_trait::async_trait;
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
        // process:
        // 1. get username from startup request
        // 2. get user from config
        // 3. get auth provider for user
        // 4. auth context = match provider and perform authentication (password = sasl, jwt|saml = cleartext token)
        // 5. acquire postgres connection matching user type
        // 6. nonce = generate secret
        // 7. set auth context: exec ansilo_set_auth_context(context, nonce)
        // 8. proxy queries and results
        // 9. reset user context: exec ansilo_clear_auth_context(nonce)
        // 10. clean connection: exec discard all
        // 11. release connection back to pool

        let (auth, _startup) = self
            .authenticator
            .authenticate_postgres(&mut client)
            .await?;

        // TODO: forward startup connection params
        let mut con = self.pool.app(&auth.username).await?;
        let reset_token = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);

        con.execute(format!(
            "SELECT ansilo_set_auth_context('{}', '{}')",
            serde_json::to_string(&auth).context("Failed to serialise auth context")?,
            reset_token
        ))
        .await?;

        con.recycle_queries(Some(vec![
            format!("SELECT ansilo_reset_auth_context('{}')", reset_token),
            "DISCARD ALL".into(),
        ]));

        let (mut client_reader, mut client_writer) = tokio::io::split(client);
        let (mut pg_reader, mut pg_writer) = con.split();

        PostgresBackendMessage::ReadyForQuery(b'I')
            .write(&mut client_writer)
            .await
            .context("Faild to send ready for query")?;

        match Self::proxy(
            &mut client_reader,
            &mut client_writer,
            &mut pg_reader,
            &mut pg_writer,
        )
        .await
        {
            Ok(_) => {
                debug!("Postgres connection closed gracefully");
            }
            Err(err) => {
                warn!("Error during postgres connection: {:?}", err);
                let _ = PostgresBackendMessage::ErrorResponse(format!("Error: {}", err))
                    .write(&mut client_writer)
                    .await;
            }
        }

        Ok(())
    }
}

impl PostgresConnectionHandler {
    async fn proxy(
        client_reader: &mut ReadHalf<Box<dyn IOStream>>,
        client_writer: &mut WriteHalf<Box<dyn IOStream>>,
        pg_reader: &mut PgReader,
        pg_writer: &mut PgWriter,
    ) -> Result<()> {
        let input = async move {
            loop {
                let msg = PostgresFrontendMessage::read(client_reader).await?;

                if msg == PostgresFrontendMessage::Terminate {
                    break;
                }

                pg_writer.send(msg).await?;
            }

            Result::<()>::Ok(())
        };

        let output = async move {
            loop {
                let msg = pg_reader.receive().await?;
                msg.write(client_writer).await?;
            }

            #[allow(unreachable_code)]
            Result::<()>::Ok(())
        };

        tokio::select! {
            res = input => res?,
            res = output => res?,
        };

        Ok(())
    }
}

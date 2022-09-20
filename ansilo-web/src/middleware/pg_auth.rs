use std::sync::Arc;

use ansilo_connectors_native_postgres::{PostgresConnection, UnpooledClient};
use ansilo_core::err::{Context, Result};
use ansilo_logging::{debug, warn};
use ansilo_pg::handler::PostgresConnectionHandler;
use ansilo_proxy::{handler::ConnectionHandler, stream::Stream};
use axum::{
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use hyper::header;
use tokio::{net::UnixStream, sync::Mutex};
use tokio_postgres::NoTls;

use crate::HttpApiState;

#[derive(Clone)]
pub struct ClientAuthenticatedPostgresConnection(
    pub Arc<Mutex<PostgresConnection<UnpooledClient>>>,
);

/// This middleware authenticates the client's credentials.
/// We extract the credentials from the request and attempt
/// to authenticate against postgres.
/// Authentication of the credentials occurs within
/// @see ansilo-pg/src/handler/auth.rs
pub(crate) async fn auth<B>(
    mut req: Request<B>,
    next: Next<B>,
    state: Arc<HttpApiState>,
) -> Result<Response, StatusCode> {
    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok());

    let auth = match auth_header {
        Some(auth) if auth.starts_with("Basic ") => auth.strip_prefix("Basic ").unwrap(),
        _ => {
            debug!("Invalid authorization header: no basic prefix");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    let auth = match base64::decode(auth) {
        Ok(auth) => auth,
        Err(e) => {
            debug!(
                "Invalid authorization header: base64 decoding failed, {:?}",
                e
            );
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    let auth = match String::from_utf8(auth) {
        Ok(auth) => auth,
        Err(e) => {
            debug!(
                "Invalid authorization header: failed to parse as utf8, {:?}",
                e
            );
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    let (user, pass) = match auth.split(':').collect::<Vec<_>>().as_slice() {
        [user, pass] => (user.clone(), pass.clone()),
        _ => {
            debug!("Invalid authorization header: invalid formatting",);
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    match connect_to_postgres(user, pass, state).await {
        Ok(pg_client) => {
            req.extensions_mut()
                .insert(ClientAuthenticatedPostgresConnection(Arc::new(Mutex::new(
                    pg_client,
                ))));
            Ok(next.run(req).await)
        }
        Err(err) => {
            debug!("Failed to authenticate with postgres: {:?}", err);
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
}

async fn connect_to_postgres(
    user: &str,
    pass: &str,
    state: Arc<HttpApiState>,
) -> Result<PostgresConnection<UnpooledClient>> {
    let (client, server) = UnixStream::pair().context("Failed to create unix sockets")?;
    let handler = PostgresConnectionHandler::new(state.auth().clone(), state.pools().clone());

    tokio::spawn(async move {
        if let Err(err) = handler.handle(Box::new(Stream(server))).await {
            warn!(
                "Error while authenticating web request for postgres connection: {:?}",
                err
            );
        }
    });

    let mut conf = tokio_postgres::Config::new();

    conf.user(user)
        .password(pass)
        .application_name("ansilo-web");

    let (client, con) = conf
        .connect_raw(client, NoTls)
        .await
        .context("Failed to authenticate")?;

    tokio::spawn(con);

    Ok(PostgresConnection::new(UnpooledClient(client)))
}

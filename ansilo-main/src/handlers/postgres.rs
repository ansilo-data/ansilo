use ansilo_core::err::Result;
use ansilo_pg::PostgresConnectionPools;
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};
use async_trait::async_trait;
use tokio::net::UnixStream;

use crate::conf::AppConf;

/// Handler for postgres-wire-protocol connections
pub struct PostgresConnectionHandler {
    conf: &'static AppConf,
    pool: PostgresConnectionPools,
}

impl PostgresConnectionHandler {
    pub fn new(conf: &'static AppConf, pool: PostgresConnectionPools) -> Self {
        Self { conf, pool }
    }
}

#[async_trait]
impl ConnectionHandler for PostgresConnectionHandler {
    async fn handle(&self, mut client: Box<dyn IOStream>) -> Result<()> {
        // TODO: We currently bypass the connection pool and proxy
        // data directly to a new connection socket.
        // We should either have pooling of these sockets in some fashion
        // or become fully aware of the protocol messages and channel it
        // through the postgres connection API
        

        // process:
        // 1. get username from startup request
        // 2. get user from config
        // 3. get auth provider for user
        // 4. auth context = match provider and perform authentication (password = sasl, jwt|saml = cleartext token)
        // 5. acquire postgres connection
        // 6. nonce = generate secret
        // 7. assume user: exec ansilo_set_user(username, nonce)
        // 8. set auth context: exec ansilo_set_auth_context(context)
        // 9. proxy queries and results
        // 10. clear session: pg discard all (possibly ansilo_clear_auth_context() if required)
        // 11. reset user to ansiloapp: exec ansilo_reset_user(nonce)
        // 12. release connection back to pool

        let sock_path = self.conf.pg.pg_socket_path();
        let mut con = UnixStream::connect(sock_path).await?;

        tokio::io::copy_bidirectional(&mut client, &mut con).await?;

        Ok(())
    }
}

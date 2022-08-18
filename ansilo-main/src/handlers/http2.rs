use ansilo_core::err::Result;
use ansilo_pg::PostgresConnectionPools;
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};
use async_trait::async_trait;

/// Handler for HTTP/2 connections
pub struct Http2ConnectionHandler {
    #[allow(unused)]
    pool: PostgresConnectionPools,
}

impl Http2ConnectionHandler {
    pub fn new(pool: PostgresConnectionPools) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConnectionHandler for Http2ConnectionHandler {
    async fn handle(&self, _con: Box<dyn IOStream>) -> Result<()> {
        todo!()
    }
}

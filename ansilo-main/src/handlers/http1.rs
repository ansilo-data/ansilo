use ansilo_core::err::Result;
use ansilo_pg::PostgresConnectionPools;
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};
use async_trait::async_trait;

/// Handler for HTTP/1 connections
pub struct Http1ConnectionHandler {
    #[allow(unused)]
    pool: PostgresConnectionPools,
}

impl Http1ConnectionHandler {
    pub fn new(pool: PostgresConnectionPools) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConnectionHandler for Http1ConnectionHandler {
    async fn handle(&self, _con: Box<dyn IOStream>) -> Result<()> {
        todo!()
    }
}

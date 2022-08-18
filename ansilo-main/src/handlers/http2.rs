use ansilo_core::err::Result;
use ansilo_pg::PostgresConnectionPools;
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};

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

impl ConnectionHandler for Http2ConnectionHandler {
    fn handle(&self, con: Box<dyn IOStream>) -> Result<()> {
        todo!()
    }
}

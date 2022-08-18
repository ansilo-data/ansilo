use ansilo_core::err::Result;
use ansilo_pg::PostgresConnectionPools;
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};

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

impl ConnectionHandler for Http1ConnectionHandler {
    fn handle(&self, con: Box<dyn IOStream>) -> Result<()> {
        todo!()
    }
}

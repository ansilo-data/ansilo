use ansilo_core::err::Result;
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};
use ansilo_web::{HttpApiHandler, HttpMode};
use async_trait::async_trait;

/// Handler for HTTP/1 connections
pub struct Http1ConnectionHandler {
    handler: HttpApiHandler,
}

impl Http1ConnectionHandler {
    pub fn new(handler: HttpApiHandler) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl ConnectionHandler for Http1ConnectionHandler {
    async fn handle(&self, con: Box<dyn IOStream>) -> Result<()> {
        self.handler.serve(HttpMode::Http1, con).await
    }
}

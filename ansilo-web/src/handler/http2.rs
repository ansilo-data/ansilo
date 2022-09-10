use crate::{HttpApiHandler, HttpMode};
use ansilo_core::err::Result;
use ansilo_proxy::{handler::ConnectionHandler, stream::IOStream};
use async_trait::async_trait;

/// Handler for HTTP/2 connections
pub struct Http2ConnectionHandler {
    handler: HttpApiHandler,
}

impl Http2ConnectionHandler {
    pub fn new(handler: HttpApiHandler) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl ConnectionHandler for Http2ConnectionHandler {
    async fn handle(&self, con: Box<dyn IOStream>) -> Result<()> {
        self.handler.serve(HttpMode::Http2, con).await
    }
}

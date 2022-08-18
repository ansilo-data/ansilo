use ansilo_core::err::Result;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::peekable::Peekable;

pub mod http1;
pub mod http2;
pub mod postgres;

/// A protocol handler
#[async_trait]
pub trait Protocol<S: AsyncRead + AsyncWrite + Unpin> {
    /// Checks if the connection is of this protocol
    async fn matches(&self, con: &mut Peekable<S>) -> Result<bool>;

    /// Handles the supplied protocol
    async fn handle(&mut self, con: Peekable<S>) -> Result<()>;
}

use ansilo_core::err::Result;
use async_trait::async_trait;

use crate::stream::IOStream;

/// A protocol-specific connection handler
#[async_trait]
pub trait ConnectionHandler: Send + Sync {
    /// Handle the supplied connection
    async fn handle(&self, con: Box<dyn IOStream>) -> Result<()>;

    /// Returns a downcastable Any of the handler
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;
}

use ansilo_core::err::Result;

use crate::stream::IOStream;

/// A protocol-specific connection handler
pub trait ConnectionHandler: Send + Sync {
    /// Handle the supplied connection
    fn handle(&self, con: Box<dyn IOStream>) -> Result<()>;

    /// Returns a downcastable Any of the handler
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;
}

use ansilo_core::err::Result;
use std::io::{Read, Write};

use crate::peekable::Peekable;

pub mod http1;
pub mod http2;
pub mod postgres;

/// A protocol handler
pub trait Protocol<S: Read + Write> {
    /// Checks if the connection is of this protocol
    fn matches(&self, con: &mut Peekable<S>) -> Result<bool>;

    /// Handles the supplied protocol
    fn handle(&mut self, con: Peekable<S>) -> Result<()>;
}

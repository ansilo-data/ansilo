use std::io::{Read, Write};

use crate::peekable::Peekable;

/// A connection made to the proxy server
///
/// We are generic over the inner stream so we can support TLS and non-TLS connections.
pub struct Connection<S: Read + Write> {
    inner: Peekable<S>,
}

impl<S: Read + Write> Connection<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: Peekable::new(inner),
        }
    }
}

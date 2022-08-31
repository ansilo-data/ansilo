//! r2d2 connection pools require errot types to implement
//! std::error::Error which our core `Error` does not.

use std::fmt::Display;

use ansilo_core::err::Error;

#[derive(Debug)]
pub struct ConnectionPoolError {
    err: Error,
}

impl Display for ConnectionPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.err.fmt(f)
    }
}

impl std::error::Error for ConnectionPoolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.err.source()
    }
}

impl From<Error> for ConnectionPoolError {
    fn from(err: Error) -> Self {
        Self { err }
    }
}

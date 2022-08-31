//! r2d2 connection pools require errot types to implement
//! std::error::Error which our core `Error` does not.
//! We provide a wrapper trait for r2d2's ManageConnection trait
//! the converts the error types.

use std::fmt::Debug;

use ansilo_core::err::Result;
use r2d2::ManageConnection;

use crate::err::ConnectionPoolError;

/// A trait which provides connection-specific functionality.
pub trait OurManageConnection: Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;

    /// Attempts to create a new connection.
    fn connect(&self) -> Result<Self::Connection>;

    /// Determines if the connection is still connected to the database.
    ///
    /// A standard implementation would check if a simple query like `SELECT 1`
    /// succeeds.
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<()>;

    /// *Quickly* determines if the connection is no longer usable.
    ///
    /// This will be called synchronously every time a connection is returned
    /// to the pool, so it should *not* block. If it returns `true`, the
    /// connection will be discarded.
    ///
    /// For example, an implementation might check if the underlying TCP socket
    /// has disconnected. Implementations that do not support this kind of
    /// fast health check may simply return `false`.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool;

    /// Returns the adaptor implementing r2d2's `ManageConnection` trai
    fn adaptor(self) -> R2d2Adaptor<Self>
    where
        Self: Sized,
    {
        R2d2Adaptor(self)
    }
}

pub struct R2d2Adaptor<T: OurManageConnection>(T);

impl<T: OurManageConnection> ManageConnection for R2d2Adaptor<T> {
    type Connection = T::Connection;
    type Error = ConnectionPoolError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.0.connect().map_err(Self::Error::from)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.0.is_valid(conn).map_err(Self::Error::from)
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.0.has_broken(conn)
    }
}

impl<T: OurManageConnection> From<T> for R2d2Adaptor<T> {
    fn from(m: T) -> Self {
        R2d2Adaptor(m)
    }
}

impl<T: OurManageConnection + Debug> Debug for R2d2Adaptor<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("R2d2Adaptor").field(&self.0).finish()
    }
}

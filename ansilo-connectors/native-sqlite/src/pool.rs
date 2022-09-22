use ansilo_connectors_base::interface::ConnectionPool;
use ansilo_core::{
    auth::AuthContext,
    err::{Context, Result},
};
use rusqlite::OpenFlags;

use crate::{conf::SqliteConnectionConfig, SqliteConnection};

/// We do not require currently pool connections for sqlite
/// It may be worthwhile at some point but not now.
#[derive(Clone)]
pub struct SqliteConnectionUnpool {
    pub(crate) conf: SqliteConnectionConfig,
}

impl SqliteConnectionUnpool {
    pub fn new(conf: SqliteConnectionConfig) -> Self {
        Self { conf }
    }
}

impl ConnectionPool for SqliteConnectionUnpool {
    type TConnection = SqliteConnection;

    fn acquire(&mut self, _auth: Option<&AuthContext>) -> Result<Self::TConnection> {
        let con =
            rusqlite::Connection::open_with_flags(self.conf.path.clone(), OpenFlags::default())
                .context("Failed to connect to sqlite")?;

        if !self.conf.extensions.is_empty() {
            unsafe {
                con.load_extension_enable()?;
                for ext in self.conf.extensions.iter() {
                    con.load_extension(ext, None)?;
                }
                con.load_extension_disable()?;
            }
        }

        Ok(SqliteConnection::new(con))
    }
}

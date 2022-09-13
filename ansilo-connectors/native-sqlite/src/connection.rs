use std::{pin::Pin, sync::Arc};

use ansilo_connectors_base::{
    common::query::QueryParam,
    interface::{Connection, QueryHandle, TransactionManager},
};
use ansilo_core::{
    data::DataValue,
    err::{ensure, Context, Result},
};

use crate::{OwnedSqliteStatment, SqlitePreparedQuery, SqliteQuery, SqliteResultSet};

/// Connection to a sqlite database
pub struct SqliteConnection {
    /// The inner connection
    con: Pin<Arc<rusqlite::Connection>>,
}

impl SqliteConnection {
    pub fn new(con: rusqlite::Connection) -> Self {
        Self { con: Arc::pin(con) }
    }

    pub(crate) fn con<'a>(&'a self) -> &'a rusqlite::Connection {
        &*self.con
    }
}

impl Connection for SqliteConnection {
    type TQuery = SqliteQuery;
    type TQueryHandle = SqlitePreparedQuery;
    type TTransactionManager = Self;

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        let stmt = OwnedSqliteStatment::prepare(Pin::clone(&self.con), &query.sql)?;

        ensure!(
            stmt.parameter_count() == query.params.len(),
            "Query parameter count mismatch"
        );

        // Sqlite queries are not really "typed", so we just use the data types
        // from the query planner
        Ok(SqlitePreparedQuery::new(stmt, query)?)
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        Some(self)
    }
}

impl SqliteConnection {
    /// Executes the supplied sql on the connection
    pub fn execute(
        &mut self,
        query: impl Into<String>,
        params: Vec<DataValue>,
    ) -> Result<SqliteResultSet> {
        let params = params
            .iter()
            .map(|p| QueryParam::constant(p.clone()))
            .collect::<Vec<_>>();

        let mut prepared = self.prepare(SqliteQuery::new(query, params))?;

        prepared.execute_query()
    }

    /// Executes the supplied sql on the connection
    pub fn execute_modify(
        &mut self,
        query: impl Into<String>,
        params: Vec<DataValue>,
    ) -> Result<Option<u64>> {
        let params = params
            .iter()
            .map(|p| QueryParam::constant(p.clone()))
            .collect::<Vec<_>>();

        let mut prepared = self.prepare(SqliteQuery::new(query, params))?;

        prepared.execute_modify()
    }
}

impl TransactionManager for SqliteConnection {
    fn is_in_transaction(&mut self) -> Result<bool> {
        Ok(!self.con.is_autocommit())
    }

    fn begin_transaction(&mut self) -> Result<()> {
        self.con
            .execute("BEGIN DEFERRED", [])
            .context("Failed to begin transaction")?;
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        self.con
            .execute("ROLLBACK", [])
            .context("Failed to rollback transaction")?;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        self.con
            .execute("COMMIT", [])
            .context("Failed to commit transaction")?;
        Ok(())
    }
}

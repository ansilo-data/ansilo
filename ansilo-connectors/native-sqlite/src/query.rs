use std::{
    io::Write,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
};

use ansilo_connectors_base::{
    common::{data::QueryParamSink, query::QueryParam},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};
use ansilo_core::{
    data::DataValue,
    err::{Context, Result},
};
use rusqlite::{ParamsFromIter, ToSql};
use serde::Serialize;

use crate::{result_set::SqliteResultSet, to_sqlite, OwnedSqliteRows};

/// Sqlite query
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SqliteQuery {
    /// The sqlite SQL query
    pub sql: String,
    /// List of parameters expected by the query
    pub params: Vec<QueryParam>,
}

impl SqliteQuery {
    pub fn new(sql: impl Into<String>, params: Vec<QueryParam>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

/// Sqlite prepared query
pub struct SqlitePreparedQuery {
    /// The sqlite stmt
    stmt: OwnedSqliteStatment,
    /// The query details
    inner: SqliteQuery,
    /// Logged params
    logged_params: Vec<DataValue>,
    /// Buffer for storing query params
    sink: QueryParamSink,
}

impl SqlitePreparedQuery {
    pub(crate) fn new(stmt: OwnedSqliteStatment, inner: SqliteQuery) -> Result<Self> {
        let sink = QueryParamSink::new(inner.params.clone());

        Ok(Self {
            stmt,
            inner,
            sink,
            logged_params: vec![],
        })
    }

    fn get_params(&mut self) -> Result<ParamsFromIter<impl Iterator<Item = Box<dyn ToSql>>>> {
        let vals = self.sink.get_all()?;
        let mut params = vec![];

        for val in vals.into_iter() {
            params.push(to_sqlite(val.clone())?);
            self.logged_params.push(val.clone());
        }

        Ok(rusqlite::params_from_iter(params.into_iter()))
    }
}

impl QueryHandle for SqlitePreparedQuery {
    type TResultSet = SqliteResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(self.sink.get_input_structure().clone())
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        Ok(self.sink.write(buff)?)
    }

    fn restart(&mut self) -> Result<()> {
        self.sink.clear();
        self.logged_params.clear();
        Ok(())
    }

    fn execute_query(&mut self) -> Result<Self::TResultSet> {
        let stmt = self.stmt.try_clone()?;

        let rows = OwnedSqliteRows::query(stmt, self.get_params()?)?;

        Ok(SqliteResultSet::new(rows)?)
    }

    fn execute_modify(&mut self) -> Result<Option<u64>> {
        let params = self.get_params()?;

        let affected = self
            .stmt
            .execute(params)
            .context("Failed to execute query")?;

        Ok(Some(affected as u64))
    }

    fn logged(&self) -> Result<LoggedQuery> {
        Ok(LoggedQuery::new(
            &self.inner.sql,
            self.logged_params
                .iter()
                .map(|val| format!("value={:?}", val))
                .collect(),
            None,
        ))
    }
}

/// To get around restrictions in the rusqlite api design
/// we have a touch of unsafety here, similar to owning_ref
/// but for our usecase
pub(crate) struct OwnedSqliteStatment {
    /// The owned reference to the connection which prepared the statement
    /// This should be safe since the connection address is stable while
    /// we hold onto the Arc
    con: Pin<Arc<rusqlite::Connection>>,
    /// The statement itself
    stmt: rusqlite::Statement<'static>,
    /// The SQL of the statement
    sql: String,
}

impl OwnedSqliteStatment {
    pub fn prepare(con: Pin<Arc<rusqlite::Connection>>, sql: &str) -> Result<Self> {
        let stmt = con.prepare(sql).context("Failed to prepare query")?;

        // SAFETY: We maintain a stable reference to the connection
        // through pinning it in this struct
        let stmt = unsafe { mem::transmute::<_, rusqlite::Statement<'static>>(stmt) };

        Ok(Self {
            con,
            stmt,
            sql: sql.to_string(),
        })
    }

    /// To support multiple executions of the query we create new prepared statements
    /// of the same query.
    pub fn try_clone(&self) -> Result<Self> {
        Self::prepare(Pin::clone(&self.con), &self.sql)
    }
}

impl Deref for OwnedSqliteStatment {
    type Target = rusqlite::Statement<'static>;

    fn deref(&self) -> &Self::Target {
        &self.stmt
    }
}

impl DerefMut for OwnedSqliteStatment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stmt
    }
}

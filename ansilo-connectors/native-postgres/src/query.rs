use std::{io::Write, ops::DerefMut, sync::Arc};

use ansilo_connectors_base::{
    common::{data::QueryParamSink, query::QueryParam},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};
use ansilo_core::{
    data::DataValue,
    err::{ensure, Context, Result},
};
use serde::Serialize;
use tokio_postgres::{types::Type, Client, Statement};

use crate::{
    data::{from_pg_type, to_pg},
    result_set::PostgresResultSet,
    runtime::runtime,
};

/// Postgres query
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PostgresQuery {
    /// The postgres SQL query
    pub sql: String,
    /// List of parameters expected by the query
    pub params: Vec<QueryParam>,
}

impl PostgresQuery {
    pub fn new(sql: impl Into<String>, params: Vec<QueryParam>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

/// Postgres prepared query
pub struct PostgresPreparedQuery<T> {
    /// The postgres client
    client: Arc<T>,
    /// The postgres SQL query
    sql: String,
    /// The prepared postgres query
    statement: Statement,
    /// Logged params
    logged_params: Vec<(DataValue, Type)>,
    /// Buffer for storing query params
    sink: QueryParamSink,
}

impl<T> PostgresPreparedQuery<T> {
    pub fn new(
        client: Arc<T>,
        statement: Statement,
        sql: String,
        params: Vec<QueryParam>,
    ) -> Result<Self> {
        ensure!(params.len() == statement.params().len());

        let sink = QueryParamSink::new(params);

        Ok(Self {
            client,
            sql,
            statement,
            sink,
            logged_params: vec![],
        })
    }
}

impl<T: DerefMut<Target = Client>> QueryHandle for PostgresPreparedQuery<T> {
    type TResultSet = PostgresResultSet;

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

    fn execute(&mut self) -> Result<Self::TResultSet> {
        let vals = self.sink.get_all()?;
        let mut params = vec![];

        for (val, pg_t) in vals.into_iter().zip(self.statement.params().iter()) {
            params.push(to_pg(val.clone(), pg_t)?);
            self.logged_params.push((val.clone(), pg_t.clone()));
        }

        let stream = runtime()
            .block_on(
                self.client
                    .query_raw(&self.statement, params.iter().map(|p| &**p)),
            )
            .context("Failed to execute query")?;

        let cols = self
            .statement
            .columns()
            .iter()
            .map(|c| Ok((c.name().to_string(), from_pg_type(c.type_())?)))
            .collect::<Result<_>>()?;

        Ok(PostgresResultSet::new(stream, cols))
    }

    fn logged(&self) -> Result<LoggedQuery> {
        Ok(LoggedQuery::new(
            &self.sql,
            self.logged_params
                .iter()
                .map(|(val, pg_t)| format!("value={:?} type={}", val, pg_t))
                .collect(),
            None,
        ))
    }
}

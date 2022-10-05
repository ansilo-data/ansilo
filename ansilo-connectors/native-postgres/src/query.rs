use std::{io::Write, ops::DerefMut, pin::Pin, sync::Arc};

use ansilo_connectors_base::{
    common::{data::QueryParamSink, query::QueryParam},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};
use ansilo_core::{
    data::DataValue,
    err::{ensure, Context, Result},
};
use ansilo_logging::debug;
use serde::Serialize;
use tokio::sync::RwLock;
use tokio_postgres::{
    types::{ToSql, Type},
    Client, Statement,
};

use crate::{
    data::{from_pg_type, to_pg},
    result_set::PostgresResultSet,
    runtime::runtime,
    TransactionState, BATCH_SIZE,
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
    client: Pin<Arc<RwLock<T>>>,
    /// The current transaction state
    transaction: TransactionState<T>,
    /// The postgres SQL query
    sql: String,
    /// The prepared postgres query
    statement: Statement,
    /// Logged params
    logged_params: Vec<(DataValue, Type)>,
    /// Buffer for storing query params
    sink: QueryParamSink,
}

impl<T: DerefMut<Target = Client>> PostgresPreparedQuery<T> {
    pub fn new(
        client: Pin<Arc<RwLock<T>>>,
        transaction: TransactionState<T>,
        statement: Statement,
        sql: String,
        params: Vec<QueryParam>,
    ) -> Result<Self> {
        ensure!(params.len() == statement.params().len());

        let sink = QueryParamSink::new(params);

        Ok(Self {
            client,
            transaction,
            sql,
            statement,
            sink,
            logged_params: vec![],
        })
    }

    fn get_params(&mut self) -> Result<Vec<Box<dyn ToSql + Send + Sync>>> {
        let vals = self.sink.get_all()?;
        let mut params = vec![];

        for (val, pg_t) in vals.into_iter().zip(self.statement.params().iter()) {
            params.push(to_pg(val.clone(), pg_t)?);
            self.logged_params.push((val.clone(), pg_t.clone()));
        }

        Ok(params)
    }

    pub async fn execute_query_async(&mut self) -> Result<PostgresResultSet<T>> {
        let params = self.get_params()?;

        let transaction = self.transaction.get_transaction_async().await?;
        let portal = transaction
            .inner_async()
            .await
            .as_ref()
            .context("Transaction closed")?
            .bind_raw(&self.statement, params.into_iter().map(|p| p))
            .await?;

        let cols = self
            .statement
            .columns()
            .iter()
            .map(|c| Ok((c.name().to_string(), from_pg_type(c.type_())?)))
            .collect::<Result<_>>()?;

        // Ensure the query has actually been executed
        debug!("Retreiving first batch of up to {BATCH_SIZE} rows");
        let stream = transaction
            .inner_async()
            .await
            .as_ref()
            .context("Transaction closed")?
            .query_portal_raw(&portal, BATCH_SIZE as _)
            .await?;

        let rs = PostgresResultSet::new(transaction, portal, stream, cols);

        Ok(rs)
    }

    pub async fn execute_modify_async(&mut self) -> Result<Option<u64>> {
        let params = self.get_params()?;
        let client = self.client.read().await;

        let affected = client
            .execute_raw(&self.statement, params.into_iter().map(|p| p))
            .await?;

        Ok(Some(affected))
    }
}

impl<T: DerefMut<Target = Client>> QueryHandle for PostgresPreparedQuery<T> {
    type TResultSet = PostgresResultSet<T>;

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
        runtime().block_on(self.execute_query_async())
    }

    fn execute_modify(&mut self) -> Result<Option<u64>> {
        runtime().block_on(self.execute_modify_async())
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

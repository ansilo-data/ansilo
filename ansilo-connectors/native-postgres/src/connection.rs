use std::{ops::DerefMut, sync::Arc};

use ansilo_connectors_base::{
    common::query::QueryParam,
    interface::{Connection, QueryHandle, TransactionManager},
};
use ansilo_core::{data::DataValue, err::Result};
use tokio_postgres::Client;

use crate::{runtime, PostgresPreparedQuery, PostgresQuery, PostgresResultSet};

/// Connection to a postgres database
pub struct PostgresConnection<T> {
    /// The connection client
    client: Arc<T>,
    /// Whether a transaction is active
    transaction: bool,
}

impl<T: DerefMut<Target = Client>> PostgresConnection<T> {
    pub fn new(client: T) -> Self {
        Self {
            client: Arc::new(client),
            transaction: false,
        }
    }

    pub(crate) fn client<'a>(&'a self) -> &'a T {
        &*self.client
    }

    pub async fn prepare_async(&mut self, query: PostgresQuery) -> Result<PostgresPreparedQuery<T>> {
        let statement = self.client.prepare(&query.sql).await?;

        Ok(PostgresPreparedQuery::new(
            Arc::clone(&self.client),
            statement,
            query.sql,
            query.params,
        )?)
    }
}

impl<T: DerefMut<Target = Client>> Connection for PostgresConnection<T> {
    type TQuery = PostgresQuery;
    type TQueryHandle = PostgresPreparedQuery<T>;
    type TTransactionManager = Self;

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        runtime().block_on(self.prepare_async(query))
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        Some(self)
    }
}

impl<T: DerefMut<Target = Client>> PostgresConnection<T> {
    /// Executes the supplied sql on the connection
    pub fn execute(
        &mut self,
        query: impl Into<String>,
        params: Vec<DataValue>,
    ) -> Result<PostgresResultSet> {
        let params = params
            .iter()
            .map(|p| QueryParam::constant(p.clone()))
            .collect::<Vec<_>>();

        let mut prepared = self.prepare(PostgresQuery::new(query, params))?;

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

        let mut prepared = self.prepare(PostgresQuery::new(query, params))?;

        prepared.execute_modify()
    }
}

impl<T: DerefMut<Target = Client>> TransactionManager for PostgresConnection<T> {
    fn is_in_transaction(&mut self) -> Result<bool> {
        Ok(self.transaction)
    }

    fn begin_transaction(&mut self) -> Result<()> {
        runtime().block_on(self.client.batch_execute("BEGIN"))?;
        self.transaction = true;
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        runtime().block_on(self.client.batch_execute("ROLLBACK"))?;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        runtime().block_on(self.client.batch_execute("COMMIT"))?;
        Ok(())
    }
}

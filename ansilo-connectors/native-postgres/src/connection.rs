use std::{ops::DerefMut, sync::Arc};

use ansilo_connectors_base::interface::{Connection, TransactionManager};
use ansilo_core::err::Result;
use tokio_postgres::Client;

use crate::{runtime, to_pg_type, PostgresPreparedQuery, PostgresQuery};

/// Connection to a postgres database
pub struct PostgresConnection<T> {
    /// The connection client
    client: Arc<T>,
    /// Whether a transaction is active
    transaction: bool,
}

impl<T> PostgresConnection<T> {
    pub fn new(client: T) -> Self {
        Self {
            client: Arc::new(client),
            transaction: false,
        }
    }
}

impl<T: DerefMut<Target = Client>> Connection for PostgresConnection<T> {
    type TQuery = PostgresQuery;
    type TQueryHandle = PostgresPreparedQuery<T>;
    type TTransactionManager = Self;

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        let types = query
            .params
            .iter()
            .map(|p| to_pg_type(&p.r#type()))
            .collect::<Vec<_>>();

        let statement =
            runtime().block_on(self.client.prepare_typed(&query.sql, types.as_slice()))?;

        Ok(PostgresPreparedQuery::new(
            Arc::clone(&self.client),
            statement,
            query.sql,
            query.params,
        )?)
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        Some(self)
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

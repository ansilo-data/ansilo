use std::{
    ops::DerefMut,
    pin::Pin,
    sync::{Arc, Weak},
};

use ansilo_connectors_base::{
    common::query::QueryParam,
    interface::{Connection, QueryHandle, TransactionManager},
};
use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Result},
};
use ansilo_logging::debug;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use tokio_postgres::{Client, IsolationLevel, Transaction};

use crate::{runtime, PostgresPreparedQuery, PostgresQuery, PostgresResultSet};

/// Connection to a postgres database
pub struct PostgresConnection<T> {
    /// The connection client
    client: Pin<Arc<RwLock<T>>>,
    /// The current transaction state
    transaction_state: TransactionState<T>,
    /// When transaction has been opened explicitly
    /// we store a strong reference here.
    explicit_transaction: Option<Arc<OwnedTransaction<T>>>,
}

impl<T: DerefMut<Target = Client>> PostgresConnection<T> {
    pub fn new(client: T) -> Self {
        let client = Arc::pin(RwLock::new(client));
        Self {
            client: client.clone(),
            transaction_state: TransactionState::new(client),
            explicit_transaction: None,
        }
    }

    pub fn client<'a>(&'a self) -> RwLockReadGuard<'a, T> {
        runtime().block_on(self.client.read())
    }

    pub async fn client_async<'a>(&'a self) -> RwLockReadGuard<'a, T> {
        self.client.read().await
    }

    pub async fn prepare_async(
        &mut self,
        query: PostgresQuery,
    ) -> Result<PostgresPreparedQuery<T>> {
        let statement = self.client.read().await.prepare(&query.sql).await?;

        Ok(PostgresPreparedQuery::new(
            self.client.clone(),
            self.transaction_state.clone(),
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
    ) -> Result<PostgresResultSet<T>> {
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
        runtime().block_on(self.transaction_state.is_in_transaction_async())
    }

    fn begin_transaction(&mut self) -> Result<()> {
        debug!("Starting transaction");
        self.explicit_transaction = Some(self.transaction_state.get_transaction()?);
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        debug!("Rolling back transaction");
        let trans = match self.explicit_transaction.take() {
            Some(trans) => trans,
            None => bail!("No active transaction"),
        };

        runtime().block_on(trans.rollback_async())?;

        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        debug!("Committing transaction");
        let trans = match self.explicit_transaction.take() {
            Some(trans) => trans,
            None => bail!("No active transaction"),
        };

        runtime().block_on(trans.commit_async())?;

        Ok(())
    }
}

// We try to enforce a global transaction state
// for the connection
pub struct TransactionState<T> {
    client: Pin<Arc<RwLock<T>>>,
    current_transaction: Arc<Mutex<Weak<OwnedTransaction<T>>>>,
}

impl<T: DerefMut<Target = Client>> TransactionState<T> {
    pub(crate) fn new(client: Pin<Arc<RwLock<T>>>) -> Self {
        Self {
            client,
            current_transaction: Arc::new(Mutex::new(Weak::default())),
        }
    }

    // Gets the current transaction, or starts a new one if there is no transaction
    pub(crate) fn get_transaction(&self) -> Result<Arc<OwnedTransaction<T>>> {
        runtime().block_on(self.get_transaction_async())
    }

    // Gets the current transaction, or starts a new one if there is no transaction
    pub(crate) async fn get_transaction_async(&self) -> Result<Arc<OwnedTransaction<T>>> {
        if let Some(trans) = self.active_transaction_async().await {
            Ok(trans)
        } else {
            let mut transaction = self.current_transaction.lock().await;
            let trans = Arc::new(OwnedTransaction::new(self.client.clone()).await?);

            // Store the transaction in a weak reference so it is dropped automatically
            // when it is no longer needed
            *transaction = Arc::downgrade(&trans);
            Ok(trans)
        }
    }

    /// Gets the active transaction if any
    async fn active_transaction_async(&self) -> Option<Arc<OwnedTransaction<T>>> {
        let transaction = self.current_transaction.lock().await;

        if let Some(trans) = transaction.upgrade() {
            if trans.is_active_async().await {
                return Some(trans);
            }
        }

        None
    }

    /// Determines if there is an active transaction
    pub async fn is_in_transaction_async(&self) -> Result<bool> {
        Ok(self.active_transaction_async().await.is_some())
    }
}

impl<T> Clone for TransactionState<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            current_transaction: self.current_transaction.clone(),
        }
    }
}

// A workaround to elide the lifetime restriction on
// tokio_postgres::Transaction
pub struct OwnedTransaction<T> {
    _client: Pin<Arc<RwLock<T>>>,
    transaction: RwLock<Option<Transaction<'static>>>,
}

impl<T: DerefMut<Target = Client>> OwnedTransaction<T> {
    pub(crate) async fn new(client: Pin<Arc<RwLock<T>>>) -> Result<Self> {
        // SAFETY: We ensure the reference to the transaction remains valid for the lifetime of the transaction
        // by pinning the arc here.
        debug!("Starting transaction");

        let transaction = {
            let mut mut_client = client.write().await;

            let trans = mut_client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .start()
                .await?;

            unsafe { std::mem::transmute::<_, Transaction<'static>>(trans) }
        };

        Ok(Self {
            _client: client,
            transaction: RwLock::new(Some(transaction)),
        })
    }

    pub fn inner(&self) -> RwLockReadGuard<'_, Option<Transaction<'static>>> {
        self.transaction.blocking_read()
    }

    pub async fn inner_async(&self) -> RwLockReadGuard<'_, Option<Transaction<'static>>> {
        self.transaction.read().await
    }

    pub async fn rollback_async(&self) -> Result<()> {
        let mut transaction = self.transaction.write().await;
        transaction
            .take()
            .context("No active transaction")?
            .rollback()
            .await?;

        Ok(())
    }

    pub async fn commit_async(&self) -> Result<()> {
        let mut transaction = self.transaction.write().await;

        transaction
            .take()
            .context("No active transaction")?
            .commit()
            .await?;

        Ok(())
    }

    pub async fn is_active_async(&self) -> bool {
        self.inner_async().await.is_some()
    }
}

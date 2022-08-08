use ansilo_core::{
    err::Result,
};

use super::QueryHandle;

/// Opens a connection to the target data source
pub trait ConnectionPool: Clone + Sized + Send + Sync + 'static {
    type TConnection: Connection;

    /// Acquires a connection to the target data source
    fn acquire(&mut self) -> Result<Self::TConnection>;
}

/// An open connection to a data source
pub trait Connection: Sized {
    type TQuery;
    type TQueryHandle: QueryHandle;
    type TTransactionManager: TransactionManager;

    /// Prepares the supplied query
    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle>;

    /// Gets the transaction manager if transactions are supported for this data source
    fn transaction_manager(&mut self) -> Option<Self::TTransactionManager>;
}

/// Manages transaction state for data sources
/// TODO: Implement support for explicit isolation-level control.
pub trait TransactionManager {
    /// Checks if the current connection is in a transaction
    fn is_in_transaction(&mut self) -> Result<bool>;

    /// Starts a transaction
    fn begin_transaction(&mut self) -> Result<()>;

    /// Rolls back the current transaction
    fn rollback_transaction(&mut self) -> Result<()>;

    /// Commits the current transaction
    fn commit_transaction(&mut self) -> Result<()>;
    
    // TODO[low]: implement support for 2PC
}

/// Allow connectors which do not support transactions to use the unit type
/// in its place
impl TransactionManager for () {
    fn is_in_transaction(&self) -> Result<bool> {
        unimplemented!()
    }

    fn begin_transaction(&self) -> Result<()> {
        unimplemented!()
    }

    fn rollback_transaction(&self) -> Result<()> {
        unimplemented!()
    }

    fn commit_transaction(&self) -> Result<()> {
        unimplemented!()
    }
}
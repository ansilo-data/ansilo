use std::{sync::Arc, collections::HashMap};

use ansilo_core::{err::{Result, bail}, data::DataValue};

use crate::{
    common::entity::ConnectorEntityConfig,
    interface::{Connection, ConnectionPool, TransactionManager},
};

use super::{
    MemoryConnectionConfig, MemoryConnectorEntitySourceConfig, MemoryQuery, MemoryQueryHandle,
};

/// Implementation for opening JDBC connections
#[derive(Clone)]
pub struct MemoryConnectionPool {
    conf: Arc<MemoryConnectionConfig>,
    entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
}

impl MemoryConnectionPool {
    pub fn new(
        conf: MemoryConnectionConfig,
        entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ) -> Result<Self> {
        Ok(Self {
            conf: Arc::new(conf),
            entities,
        })
    }

    pub fn conf(&self) -> Arc<MemoryConnectionConfig> {
        Arc::clone(&self.conf)
    }
}

impl ConnectionPool for MemoryConnectionPool {
    type TConnection = MemoryConnection;

    fn acquire(&mut self) -> Result<MemoryConnection> {
        Ok(MemoryConnection(
            Arc::clone(&self.conf),
            self.entities.clone(),
        ))
    }
}

pub struct MemoryConnection(
    pub Arc<MemoryConnectionConfig>,
    ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
);

impl Connection for MemoryConnection {
    type TQuery = MemoryQuery;
    type TQueryHandle = MemoryQueryHandle;
    type TTransactionManager = ();

    fn prepare(&self, query: MemoryQuery) -> Result<MemoryQueryHandle> {
        Ok(MemoryQueryHandle::new(
            query,
            Arc::clone(&self.0),
            self.1.clone(),
        ))
    }

    fn transaction_manager(&self) -> Option<Self::TTransactionManager> {
        if self.0.transactions_enabled {
            Some(MemoryTransactionManager::new(Arc::clone(&self.0)))
        } else {
            None
        }
    }
}

pub struct MemoryTransactionManager {
    data: Arc<MemoryConnectionConfig>,
    rollback_state: Option<MemoryConnectionConfig>,
}

impl MemoryTransactionManager {
    pub fn new(data: Arc<MemoryConnectionConfig>) -> Self {
        Self {
            data,
        rollback_state: None
        }
    }
}

impl TransactionManager for MemoryTransactionManager {
    fn is_in_transaction(&self) -> Result<bool> {
        Ok(self.rollback_state.is_some())
    }

    fn begin_transaction(&self) -> Result<()> {
        self.rollback_state = Some(self.data.clone());
    }

    fn rollback_transaction(&self) -> Result<()> {
        if self.rollback_state.is_none() {
            bail!("No active transaction");
        }

        self.data.restore_from(self.rollback_state.unwrap());
    }

    fn commit_transaction(&self) -> Result<()> {
        todo!()
    }
}

use std::{mem, sync::Arc};

use ansilo_core::err::{bail, Result};

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
        Ok(MemoryConnection::new(
            Arc::clone(&self.conf),
            self.entities.clone(),
        ))
    }
}

pub struct MemoryConnection {
    pub data: Arc<MemoryConnectionConfig>,
    conf: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    rollback_state: Option<MemoryConnectionConfig>,
}

impl MemoryConnection {
    pub fn new(
        data: Arc<MemoryConnectionConfig>,
        conf: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ) -> Self {
        Self {
            data,
            conf,
            rollback_state: None,
        }
    }
}

impl Connection for MemoryConnection {
    type TQuery = MemoryQuery;
    type TQueryHandle = MemoryQueryHandle;
    type TTransactionManager = MemoryConnection;

    fn prepare(&mut self, query: MemoryQuery) -> Result<MemoryQueryHandle> {
        Ok(MemoryQueryHandle::new(
            query,
            Arc::clone(&self.data),
            self.conf.clone(),
        ))
    }

    fn transaction_manager(&mut self) -> Option<&mut Self> {
        if self.data.transactions_enabled {
            Some(self)
        } else {
            None
        }
    }
}

impl TransactionManager for MemoryConnection {
    fn is_in_transaction(&mut self) -> Result<bool> {
        Ok(self.rollback_state.is_some())
    }

    fn begin_transaction(&mut self) -> Result<()> {
        self.rollback_state = Some((*self.data).clone());
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        if self.rollback_state.is_none() {
            bail!("No active transaction");
        }

        let rb = mem::replace(&mut self.rollback_state, None);
        self.data.restore_from(rb.unwrap());
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        if self.rollback_state.is_none() {
            bail!("No active transaction");
        }

        self.rollback_state = None;
        Ok(())
    }
}

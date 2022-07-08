use std::sync::Arc;

use ansilo_core::err::Result;

use crate::{
    common::entity::ConnectorEntityConfig,
    interface::{Connection, ConnectionPool},
};

use super::{MemoryConnectionConfig, MemoryQuery, MemoryQueryHandle};

/// Implementation for opening JDBC connections
#[derive(Clone)]
pub struct MemoryConnectionPool {
    conf: Arc<MemoryConnectionConfig>,
    entities: ConnectorEntityConfig<()>,
}

impl MemoryConnectionPool {
    pub fn new(conf: MemoryConnectionConfig, entities: ConnectorEntityConfig<()>) -> Result<Self> {
        Ok(Self {
            conf: Arc::new(conf),
            entities,
        })
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

pub struct MemoryConnection(pub Arc<MemoryConnectionConfig>, ConnectorEntityConfig<()>);

impl Connection for MemoryConnection {
    type TQuery = MemoryQuery;
    type TQueryHandle = MemoryQueryHandle;

    fn prepare(&self, query: MemoryQuery) -> Result<MemoryQueryHandle> {
        Ok(MemoryQueryHandle::new(
            query,
            Arc::clone(&self.0),
            self.1.clone(),
        ))
    }
}

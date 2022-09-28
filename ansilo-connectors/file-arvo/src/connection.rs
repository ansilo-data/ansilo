use std::sync::Arc;

use ansilo_core::{auth::AuthContext, err::Result};

use ansilo_connectors_base::interface::{Connection, ConnectionPool};

use crate::ArvoConfig;

use super::{ArvoQuery, ArvoQueryHandle};

#[derive(Clone)]
pub struct ArvoConnectionUnpool {
    conf: Arc<ArvoConfig>,
}

impl ArvoConnectionUnpool {
    pub fn new(conf: ArvoConfig) -> Self {
        Self {
            conf: Arc::new(conf),
        }
    }
}

impl ConnectionPool for ArvoConnectionUnpool {
    type TConnection = ArvoConnection;

    fn acquire(&mut self, _auth: Option<&AuthContext>) -> Result<Self::TConnection> {
        Ok(ArvoConnection::new(Arc::clone(&self.conf)))
    }
}

#[derive(Clone)]
pub struct ArvoConnection {
    conf: Arc<ArvoConfig>,
}

impl ArvoConnection {
    pub fn new(conf: Arc<ArvoConfig>) -> Self {
        Self { conf }
    }

    pub fn conf(&self) -> &ArvoConfig {
        self.conf.as_ref()
    }
}

impl Connection for ArvoConnection {
    type TQuery = ArvoQuery;
    type TQueryHandle = ArvoQueryHandle;
    type TTransactionManager = ();

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        Ok(ArvoQueryHandle::new(query))
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        None
    }
}

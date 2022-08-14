use std::{mem, sync::Arc};

use ansilo_core::err::{bail, Result};

use crate::{
    common::entity::ConnectorEntityConfig,
    interface::{Connection, ConnectionPool, TransactionManager},
};

use super::{MemoryConnectorEntitySourceConfig, MemoryDatabase, MemoryQuery, MemoryQueryHandle};

/// Implementation for opening JDBC connections
#[derive(Clone)]
pub struct MemoryConnectionPool {
    conf: Arc<MemoryDatabase>,
    entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
}

impl MemoryConnectionPool {
    pub fn new(
        conf: MemoryDatabase,
        entities: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ) -> Result<Self> {
        Ok(Self {
            conf: Arc::new(conf),
            entities,
        })
    }

    pub fn conf(&self) -> Arc<MemoryDatabase> {
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
    pub data: Arc<MemoryDatabase>,
    pub(super) conf: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    transaction: Option<TransactionState>,
}

pub struct TransactionState {
    rollback_state: MemoryDatabase,
    commit_state: Arc<MemoryDatabase>,
}

impl MemoryConnection {
    pub fn new(
        data: Arc<MemoryDatabase>,
        conf: ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ) -> Self {
        Self {
            data,
            conf,
            transaction: None,
        }
    }
}

impl Connection for MemoryConnection {
    type TQuery = MemoryQuery;
    type TQueryHandle = MemoryQueryHandle;
    type TTransactionManager = MemoryConnection;

    fn prepare(&mut self, query: MemoryQuery) -> Result<MemoryQueryHandle> {
        let target = if let Some(transaction) = &self.transaction {
            &transaction.commit_state
        } else {
            &self.data
        };

        Ok(MemoryQueryHandle::new(
            query,
            Arc::clone(target),
            self.conf.clone(),
        ))
    }

    fn transaction_manager(&mut self) -> Option<&mut Self> {
        if self.data.conf().transactions_enabled {
            Some(self)
        } else {
            None
        }
    }
}

impl TransactionState {
    pub fn new(current_state: MemoryDatabase) -> Self {
        let rollback_state = current_state.clone();
        let commit_state = Arc::new(current_state);

        Self {
            rollback_state,
            commit_state,
        }
    }
}

impl TransactionManager for MemoryConnection {
    fn is_in_transaction(&mut self) -> Result<bool> {
        Ok(self.transaction.is_some())
    }

    fn begin_transaction(&mut self) -> Result<()> {
        self.transaction = Some(TransactionState::new((*self.data).clone()));
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        if self.transaction.is_none() {
            bail!("No active transaction");
        }

        let trans = mem::replace(&mut self.transaction, None).unwrap();
        self.data.restore_from(trans.rollback_state);
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        if self.transaction.is_none() {
            bail!("No active transaction");
        }

        let trans = mem::replace(&mut self.transaction, None).unwrap();
        self.data.restore_from((*trans.commit_state).clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::{
        config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig},
        data::{DataType, DataValue},
    };

    use crate::common::entity::EntitySource;

    use super::*;

    fn mock_data() -> (
        ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        MemoryDatabase,
    ) {
        let data = MemoryDatabase::new();
        let mut conf = ConnectorEntityConfig::new();

        conf.add(EntitySource::new(
            EntityConfig::minimal(
                "dummy",
                vec![EntityAttributeConfig::minimal("x", DataType::UInt32)],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::default(),
        ));

        data.set_data("dummy", vec![vec![DataValue::UInt32(1)]]);

        (conf, data)
    }

    fn setup_connection() -> MemoryConnection {
        let (conf, data) = mock_data();

        MemoryConnection::new(Arc::new(data), conf)
    }

    #[test]
    fn test_memory_connector_connection_transactions_disabled() {
        let mut con = setup_connection();

        con.data.update_conf(|conf| {
            conf.transactions_enabled = false;
        });

        assert!(con.transaction_manager().is_none());
    }

    #[test]
    fn test_memory_connector_connection_transactions_enabled() {
        let mut con = setup_connection();

        assert!(con.transaction_manager().is_some());
    }

    #[test]
    fn test_memory_connector_connection_transaction_rollback() {
        let mut con = setup_connection();

        let orig = con.data.get_data("dummy").unwrap();

        assert_eq!(con.is_in_transaction().unwrap(), false);
        con.begin_transaction().unwrap();
        assert_eq!(con.is_in_transaction().unwrap(), true);

        con.data
            .with_data_mut("dummy", |data| {
                data[0][0] = DataValue::UInt32(123);
                ()
            })
            .unwrap();

        con.rollback_transaction().unwrap();
        assert_eq!(con.is_in_transaction().unwrap(), false);

        let after_rollback = con.data.get_data("dummy").unwrap();

        assert_eq!(after_rollback, orig);
    }

    #[test]
    fn test_memory_connector_connection_transaction_commit() {
        let mut con = setup_connection();

        assert_eq!(con.is_in_transaction().unwrap(), false);
        con.begin_transaction().unwrap();
        assert_eq!(con.is_in_transaction().unwrap(), true);

        // Mutate commit state
        con.transaction
            .as_mut()
            .unwrap()
            .commit_state
            .with_data_mut("dummy", |data| {
                data[0][0] = DataValue::UInt32(123);
                ()
            })
            .unwrap();

        con.commit_transaction().unwrap();
        assert_eq!(con.is_in_transaction().unwrap(), false);

        let after_commit = con.data.get_data("dummy").unwrap();

        assert_eq!(after_commit, vec![vec![DataValue::UInt32(123)],]);
    }
}

use std::str::FromStr;

use ansilo_core::{
    config::{self, NodeConfig},
    err::{bail, Result},
};

use ansilo_connectors_base::{
    common::entity::ConnectorEntityConfig,
    interface::{QueryInputStructure, RowStructure},
};

use ansilo_connectors_memory::{
    MemoryConnection, MemoryConnectionPool, MemoryConnectorEntitySourceConfig, MemoryDatabase,
    MemoryQuery, MemoryQueryHandle, MemoryResultSet,
};

use ansilo_connectors_jdbc_base::JdbcQuery;

use ansilo_connectors_jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcEntitySourceConfig};

use ansilo_connectors_base::interface::{
    Connection, ConnectionPool, Connector, QueryHandle, ResultSet, TransactionManager,
};

pub use ansilo_connectors_jdbc_oracle::OracleJdbcConnector;
pub use ansilo_connectors_memory::MemoryConnector;

#[derive(Debug, PartialEq)]
pub enum Connectors {
    OracleJdbc,
    Memory,
}

#[derive(Debug)]
pub enum ConnectionConfigs {
    OracleJdbc(OracleJdbcConnectionConfig),
    Memory(MemoryDatabase),
}

#[derive(Debug)]
pub enum EntitySourceConfigs {
    OracleJdbc(OracleJdbcEntitySourceConfig),
    Memory(MemoryConnectorEntitySourceConfig),
}

#[derive(Clone)]
pub enum ConnectionPools {
    OracleJdbc(
        <OracleJdbcConnector as Connector>::TConnectionPool,
        ConnectorEntityConfig<OracleJdbcEntitySourceConfig>,
    ),
    Memory(
        MemoryConnectionPool,
        ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
    ),
}

pub enum Connections {
    OracleJdbc(<OracleJdbcConnector as Connector>::TConnection),
    Memory(MemoryConnection),
}

pub enum Queries {
    Jdbc(JdbcQuery),
    Memory(MemoryQuery),
}

pub enum QueryHandles {
    OracleJdbc(<OracleJdbcConnector as Connector>::TQueryHandle),
    Memory(MemoryQueryHandle),
}

pub enum ResultSets {
    OracleJdbc(<OracleJdbcConnector as Connector>::TResultSet),
    Memory(MemoryResultSet),
}

impl Connectors {
    pub fn from_type(r#type: &str) -> Option<Self> {
        Some(match r#type {
            OracleJdbcConnector::TYPE => Connectors::OracleJdbc,
            MemoryConnector::TYPE => Connectors::Memory,
            _ => return None
        })
    }

    pub fn r#type(&self) -> &'static str {
        match self {
            Connectors::OracleJdbc => OracleJdbcConnector::TYPE,
            Connectors::Memory => MemoryConnector::TYPE,
        }
    }

    pub fn parse_options(&self, options: config::Value) -> Result<ConnectionConfigs> {
        Ok(match self {
            Connectors::OracleJdbc => {
                ConnectionConfigs::OracleJdbc(OracleJdbcConnector::parse_options(options)?)
            }
            Connectors::Memory => {
                ConnectionConfigs::Memory(MemoryConnector::parse_options(options)?)
            }
        })
    }

    pub fn parse_entity_source_options(
        &self,
        options: config::Value,
    ) -> Result<EntitySourceConfigs> {
        Ok(match self {
            Connectors::OracleJdbc => EntitySourceConfigs::OracleJdbc(
                OracleJdbcConnector::parse_entity_source_options(options)?,
            ),
            Connectors::Memory => {
                EntitySourceConfigs::Memory(MemoryConnector::parse_entity_source_options(options)?)
            }
        })
    }

    pub fn create_connection_pool(
        &self,
        nc: &NodeConfig,
        data_source_id: &str,
        options: ConnectionConfigs,
    ) -> Result<ConnectionPools> {
        Ok(match (self, options) {
            (Connectors::OracleJdbc, ConnectionConfigs::OracleJdbc(options)) => {
                let (pool, entities) =
                    Self::create_pool::<OracleJdbcConnector>(options, nc, data_source_id)?;
                ConnectionPools::OracleJdbc(pool, entities)
            }
            (Connectors::Memory, ConnectionConfigs::Memory(options)) => {
                let (pool, entities) =
                    Self::create_pool::<MemoryConnector>(options, nc, data_source_id)?;
                ConnectionPools::Memory(pool, entities)
            }
            (this, options) => bail!(
                "Type mismatch between connector {:?} and config {:?}",
                this,
                options
            ),
        })
    }

    fn create_pool<TConnector: Connector>(
        options: TConnector::TConnectionConfig,
        nc: &NodeConfig,
        data_source_id: &str,
    ) -> Result<(
        TConnector::TConnectionPool,
        ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
    )> {
        let entities = ConnectorEntityConfig::<TConnector::TEntitySourceConfig>::from::<TConnector>(
            nc,
            data_source_id,
        )?;

        let pool = TConnector::create_connection_pool(options, nc, &entities)?;

        Ok((pool, entities))
    }
}

impl FromStr for Connectors {
    type Err = ansilo_core::err::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            OracleJdbcConnector::TYPE => Connectors::OracleJdbc,
            s @ _ => bail!("Unknown connector type {}", s),
        })
    }
}

impl ConnectionPool for ConnectionPools {
    type TConnection = Connections;

    fn acquire(&mut self) -> Result<Self::TConnection> {
        Ok(match self {
            ConnectionPools::OracleJdbc(p, _) => Connections::OracleJdbc(p.acquire()?),
            ConnectionPools::Memory(p, _) => Connections::Memory(p.acquire()?),
        })
    }
}

impl Connection for Connections {
    type TQuery = Queries;
    type TQueryHandle = QueryHandles;
    type TTransactionManager = Self;

    fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        Ok(match (self, query) {
            (Connections::OracleJdbc(c), Queries::Jdbc(q)) => {
                QueryHandles::OracleJdbc(c.prepare(q)?)
            }
            (Connections::Memory(c), Queries::Memory(q)) => QueryHandles::Memory(c.prepare(q)?),
            (_, _) => bail!("Type mismatch between connection and query",),
        })
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        let supports_transactions = match self {
            Connections::OracleJdbc(c) => c.transaction_manager().is_some(),
            Connections::Memory(c) => c.transaction_manager().is_some(),
        };

        if supports_transactions {
            Some(self)
        } else {
            None
        }
    }
}

impl TransactionManager for Connections {
    fn is_in_transaction(&mut self) -> Result<bool> {
        match self {
            Connections::OracleJdbc(t) => t.transaction_manager().unwrap().is_in_transaction(),
            Connections::Memory(t) => t.transaction_manager().unwrap().is_in_transaction(),
        }
    }

    fn begin_transaction(&mut self) -> Result<()> {
        match self {
            Connections::OracleJdbc(t) => t.transaction_manager().unwrap().begin_transaction(),
            Connections::Memory(t) => t.transaction_manager().unwrap().begin_transaction(),
        }
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        match self {
            Connections::OracleJdbc(t) => t.transaction_manager().unwrap().rollback_transaction(),
            Connections::Memory(t) => t.transaction_manager().unwrap().rollback_transaction(),
        }
    }

    fn commit_transaction(&mut self) -> Result<()> {
        match self {
            Connections::OracleJdbc(t) => t.transaction_manager().unwrap().commit_transaction(),
            Connections::Memory(t) => t.transaction_manager().unwrap().commit_transaction(),
        }
    }
}

impl QueryHandle for QueryHandles {
    type TResultSet = ResultSets;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        match self {
            QueryHandles::OracleJdbc(h) => h.get_structure(),
            QueryHandles::Memory(h) => h.get_structure(),
        }
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        match self {
            QueryHandles::OracleJdbc(h) => h.write(buff),
            QueryHandles::Memory(h) => h.write(buff),
        }
    }

    fn restart(&mut self) -> Result<()> {
        match self {
            QueryHandles::OracleJdbc(h) => h.restart(),
            QueryHandles::Memory(h) => h.restart(),
        }
    }

    fn execute(&mut self) -> Result<Self::TResultSet> {
        Ok(match self {
            QueryHandles::OracleJdbc(h) => ResultSets::OracleJdbc(h.execute()?),
            QueryHandles::Memory(h) => ResultSets::Memory(h.execute()?),
        })
    }
}

impl ResultSet for ResultSets {
    fn get_structure(&self) -> Result<RowStructure> {
        match self {
            ResultSets::OracleJdbc(r) => r.get_structure(),
            ResultSets::Memory(r) => r.get_structure(),
        }
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        match self {
            ResultSets::OracleJdbc(r) => r.read(buff),
            ResultSets::Memory(r) => r.read(buff),
        }
    }
}

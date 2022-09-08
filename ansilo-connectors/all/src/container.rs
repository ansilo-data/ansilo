use std::{convert::TryInto, str::FromStr};

use ansilo_connectors_jdbc_mysql::{MysqlJdbcConnectionConfig, MysqlJdbcEntitySourceConfig};
use ansilo_connectors_native_postgres::{
    PooledClient, PostgresConnection, PostgresConnectionConfig, PostgresConnectionPool,
    PostgresEntitySourceConfig,
};
use ansilo_core::{
    auth::AuthContext,
    config::{self, NodeConfig},
    err::{bail, Context, Result},
};

use ansilo_connectors_base::common::entity::ConnectorEntityConfig;

use ansilo_connectors_memory::{
    MemoryConnection, MemoryConnectionPool, MemoryConnectorEntitySourceConfig, MemoryDatabase,
};

use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcConnectionPool};

use ansilo_connectors_jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcEntitySourceConfig};

use ansilo_connectors_base::interface::{ConnectionPool, Connector};

pub use ansilo_connectors_jdbc_mysql::MysqlJdbcConnector;
pub use ansilo_connectors_jdbc_oracle::OracleJdbcConnector;
pub use ansilo_connectors_memory::MemoryConnector;
pub use ansilo_connectors_native_postgres::PostgresConnector;

#[derive(Debug, PartialEq)]
pub enum Connectors {
    OracleJdbc,
    MysqlJdbc,
    NativePostgres,
    Memory,
}

#[derive(Debug)]
pub enum ConnectionConfigs {
    OracleJdbc(OracleJdbcConnectionConfig),
    MysqlJdbc(MysqlJdbcConnectionConfig),
    NativePostgres(PostgresConnectionConfig),
    Memory(MemoryDatabase),
}

#[derive(Debug)]
pub enum EntitySourceConfigs {
    OracleJdbc(OracleJdbcEntitySourceConfig),
    MysqlJdbc(MysqlJdbcEntitySourceConfig),
    NativePostgres(PostgresEntitySourceConfig),
    Memory(MemoryConnectorEntitySourceConfig),
}

#[derive(Clone)]
pub enum ConnectionPools {
    Jdbc(JdbcConnectionPool),
    NativePostgres(PostgresConnectionPool),
    Memory(MemoryConnectionPool),
}

#[derive(Clone)]
pub enum ConnectorEntityConfigs {
    OracleJdbc(ConnectorEntityConfig<OracleJdbcEntitySourceConfig>),
    MysqlJdbc(ConnectorEntityConfig<MysqlJdbcEntitySourceConfig>),
    NativePostgres(ConnectorEntityConfig<PostgresEntitySourceConfig>),
    Memory(ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>),
}

impl<'a> TryInto<&'a mut ConnectorEntityConfig<OracleJdbcEntitySourceConfig>>
    for &'a mut ConnectorEntityConfigs
{
    type Error = ansilo_core::err::Error;

    fn try_into(
        self,
    ) -> Result<&'a mut ConnectorEntityConfig<OracleJdbcEntitySourceConfig>, Self::Error> {
        match self {
            ConnectorEntityConfigs::OracleJdbc(c) => Ok(c),
            _ => bail!("Unexpected type"),
        }
    }
}

pub enum Connections {
    Jdbc(JdbcConnection),
    NativePostgres(PostgresConnection<PooledClient>),
    Memory(MemoryConnection),
}

impl Connectors {
    pub fn from_type(r#type: &str) -> Option<Self> {
        Some(match r#type {
            OracleJdbcConnector::TYPE => Connectors::OracleJdbc,
            MysqlJdbcConnector::TYPE => Connectors::MysqlJdbc,
            PostgresConnector::TYPE => Connectors::NativePostgres,
            MemoryConnector::TYPE => Connectors::Memory,
            _ => return None,
        })
    }

    pub fn r#type(&self) -> &'static str {
        match self {
            Connectors::OracleJdbc => OracleJdbcConnector::TYPE,
            Connectors::MysqlJdbc => MysqlJdbcConnector::TYPE,
            Connectors::NativePostgres => PostgresConnector::TYPE,
            Connectors::Memory => MemoryConnector::TYPE,
        }
    }

    pub fn parse_options(&self, options: config::Value) -> Result<ConnectionConfigs> {
        Ok(match self {
            Connectors::OracleJdbc => {
                ConnectionConfigs::OracleJdbc(OracleJdbcConnector::parse_options(options)?)
            }
            Connectors::MysqlJdbc => {
                ConnectionConfigs::MysqlJdbc(MysqlJdbcConnector::parse_options(options)?)
            }
            Connectors::NativePostgres => {
                ConnectionConfigs::NativePostgres(PostgresConnector::parse_options(options)?)
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
            Connectors::MysqlJdbc => EntitySourceConfigs::MysqlJdbc(
                MysqlJdbcConnector::parse_entity_source_options(options)?,
            ),
            Connectors::NativePostgres => EntitySourceConfigs::NativePostgres(
                PostgresConnector::parse_entity_source_options(options)?,
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
    ) -> Result<(ConnectionPools, ConnectorEntityConfigs)> {
        Ok(match (self, options) {
            (Connectors::OracleJdbc, ConnectionConfigs::OracleJdbc(options)) => {
                let (pool, entities) =
                    Self::create_pool::<OracleJdbcConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::Jdbc(pool),
                    ConnectorEntityConfigs::OracleJdbc(entities),
                )
            }
            (Connectors::MysqlJdbc, ConnectionConfigs::MysqlJdbc(options)) => {
                let (pool, entities) =
                    Self::create_pool::<MysqlJdbcConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::Jdbc(pool),
                    ConnectorEntityConfigs::MysqlJdbc(entities),
                )
            }
            (Connectors::Memory, ConnectionConfigs::Memory(options)) => {
                let (pool, entities) =
                    Self::create_pool::<MemoryConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::Memory(pool),
                    ConnectorEntityConfigs::Memory(entities),
                )
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
        Connectors::from_type(s).with_context(|| format!("Unknown connector type: {}", s))
    }
}

impl ConnectionPools {
    pub fn acquire(&mut self, auth: Option<&AuthContext>) -> Result<Connections> {
        Ok(match self {
            ConnectionPools::Jdbc(p) => Connections::Jdbc(p.acquire(auth)?),
            ConnectionPools::NativePostgres(p) => Connections::NativePostgres(p.acquire(auth)?),
            ConnectionPools::Memory(p) => Connections::Memory(p.acquire(auth)?),
        })
    }
}

// impl Connection for Connections {
//     type TQuery = Queries;
//     type TQueryHandle = QueryHandles;
//     type TTransactionManager = Self;

//     fn prepare(&mut self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
//         Ok(match (self, query) {
//             (Connections::Jdbc(c), Queries::Jdbc(q)) => QueryHandles::Jdbc(c.prepare(q)?),
//             (Connections::N(c), Queries::Jdbc(q)) => QueryHandles::Jdbc(c.prepare(q)?),
//             (Connections::Memory(c), Queries::Memory(q)) => QueryHandles::Memory(c.prepare(q)?),
//             (_, _) => bail!("Type mismatch between connection and query",),
//         })
//     }

//     fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
//         let supports_transactions = match self {
//             Connections::Jdbc(c) => c.transaction_manager().is_some(),
//             Connections::Memory(c) => c.transaction_manager().is_some(),
//         };

//         if supports_transactions {
//             Some(self)
//         } else {
//             None
//         }
//     }
// }

// impl TransactionManager for Connections {
//     fn is_in_transaction(&mut self) -> Result<bool> {
//         match self {
//             Connections::Jdbc(t) => t.transaction_manager().unwrap().is_in_transaction(),
//             Connections::Memory(t) => t.transaction_manager().unwrap().is_in_transaction(),
//         }
//     }

//     fn begin_transaction(&mut self) -> Result<()> {
//         match self {
//             Connections::Jdbc(t) => t.transaction_manager().unwrap().begin_transaction(),
//             Connections::Memory(t) => t.transaction_manager().unwrap().begin_transaction(),
//         }
//     }

//     fn rollback_transaction(&mut self) -> Result<()> {
//         match self {
//             Connections::Jdbc(t) => t.transaction_manager().unwrap().rollback_transaction(),
//             Connections::Memory(t) => t.transaction_manager().unwrap().rollback_transaction(),
//         }
//     }

//     fn commit_transaction(&mut self) -> Result<()> {
//         match self {
//             Connections::Jdbc(t) => t.transaction_manager().unwrap().commit_transaction(),
//             Connections::Memory(t) => t.transaction_manager().unwrap().commit_transaction(),
//         }
//     }
// }

// impl QueryHandle for QueryHandles {
//     type TResultSet = ResultSets;

//     fn get_structure(&self) -> Result<QueryInputStructure> {
//         match self {
//             QueryHandles::Jdbc(h) => h.get_structure(),
//             QueryHandles::Memory(h) => h.get_structure(),
//         }
//     }

//     fn write(&mut self, buff: &[u8]) -> Result<usize> {
//         match self {
//             QueryHandles::Jdbc(h) => h.write(buff),
//             QueryHandles::Memory(h) => h.write(buff),
//         }
//     }

//     fn restart(&mut self) -> Result<()> {
//         match self {
//             QueryHandles::Jdbc(h) => h.restart(),
//             QueryHandles::Memory(h) => h.restart(),
//         }
//     }

//     fn execute_query(&mut self) -> Result<Self::TResultSet> {
//         Ok(match self {
//             QueryHandles::Jdbc(h) => ResultSets::Jdbc(h.execute_query()?),
//             QueryHandles::Memory(h) => ResultSets::Memory(h.execute_query()?),
//         })
//     }

//     fn execute_modify(&mut self) -> Result<Option<u64>> {
//         Ok(match self {
//             QueryHandles::Jdbc(h) => h.execute_modify()?,
//             QueryHandles::Memory(h) => h.execute_modify()?,
//         })
//     }

//     fn logged(&self) -> Result<LoggedQuery> {
//         match self {
//             QueryHandles::Jdbc(h) => h.logged(),
//             QueryHandles::Memory(h) => h.logged(),
//         }
//     }
// }

// impl ResultSet for ResultSets {
//     fn get_structure(&self) -> Result<RowStructure> {
//         match self {
//             ResultSets::Jdbc(r) => r.get_structure(),
//             ResultSets::Memory(r) => r.get_structure(),
//         }
//     }

//     fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
//         match self {
//             ResultSets::Jdbc(r) => r.read(buff),
//             ResultSets::Memory(r) => r.read(buff),
//         }
//     }
// }

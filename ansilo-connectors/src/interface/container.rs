use std::str::FromStr;

use ansilo_core::{
    config::{self, NodeConfig},
    err::{bail, Result},
};

use crate::{
    common::entity::ConnectorEntityConfig,
    jdbc::{JdbcConnection, JdbcConnectionPool, JdbcPreparedQuery, JdbcQuery, JdbcResultSet},
    jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcConnector, OracleJdbcEntitySourceConfig},
};

use super::{Connection, ConnectionPool, Connector, QueryHandle, ResultSet};

#[derive(Debug, PartialEq)]
pub enum Connectors {
    OracleJdbc,
}

#[derive(Debug)]
pub enum ConnectionConfigs {
    OracleJdbc(OracleJdbcConnectionConfig),
}

#[derive(Debug)]
pub enum EntitySourceConfigs {
    OracleJdbc(OracleJdbcEntitySourceConfig),
}

#[derive(Clone)]
pub enum ConnectionPools {
    OracleJdbc(
        JdbcConnectionPool,
        ConnectorEntityConfig<OracleJdbcEntitySourceConfig>,
    ),
}

pub enum Connections {
    Jdbc(JdbcConnection),
}

pub enum Queries {
    Jdbc(JdbcQuery),
}

pub enum QueryHandles {
    Jdbc(JdbcPreparedQuery),
}

pub enum ResultSets {
    Jdbc(JdbcResultSet),
}

impl Connectors {
    pub fn r#type(&self) -> &'static str {
        match self {
            Connectors::OracleJdbc => OracleJdbcConnector::TYPE,
        }
    }

    pub fn parse_options(&self, options: config::Value) -> Result<ConnectionConfigs> {
        Ok(match self {
            Connectors::OracleJdbc => {
                ConnectionConfigs::OracleJdbc(OracleJdbcConnector::parse_options(options)?)
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
                let entities = Self::get_entity_config::<OracleJdbcConnector>(nc, data_source_id)?;
                ConnectionPools::OracleJdbc(
                    OracleJdbcConnector::create_connection_pool(options, nc, &entities)?,
                    entities,
                )
            }
            #[allow(unreachable_patterns)]
            (this, options) => bail!(
                "Type mismatch between connector {:?} and config {:?}",
                this,
                options
            ),
        })
    }

    fn get_entity_config<TConnector: Connector>(
        nc: &NodeConfig,
        data_source_id: &str,
    ) -> Result<ConnectorEntityConfig<TConnector::TEntitySourceConfig>> {
        ConnectorEntityConfig::<TConnector::TEntitySourceConfig>::from::<TConnector>(
            nc,
            data_source_id,
        )
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
            ConnectionPools::OracleJdbc(p, _) => Connections::Jdbc(p.acquire()?),
        })
    }
}

impl Connection for Connections {
    type TQuery = Queries;
    type TQueryHandle = QueryHandles;

    fn prepare(&self, query: Self::TQuery) -> Result<Self::TQueryHandle> {
        Ok(match (self, query) {
            (Connections::Jdbc(c), Queries::Jdbc(q)) => QueryHandles::Jdbc(c.prepare(q)?),
        })
    }
}

impl QueryHandle for QueryHandles {
    type TResultSet = ResultSets;

    fn get_structure(&self) -> Result<super::QueryInputStructure> {
        match self {
            QueryHandles::Jdbc(h) => h.get_structure(),
        }
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        match self {
            QueryHandles::Jdbc(h) => h.write(buff),
        }
    }

    fn execute(&mut self) -> Result<Self::TResultSet> {
        Ok(match self {
            QueryHandles::Jdbc(h) => ResultSets::Jdbc(h.execute()?),
        })
    }
}

impl ResultSet for ResultSets {
    fn get_structure(&self) -> Result<super::RowStructure> {
        match self {
            ResultSets::Jdbc(r) => r.get_structure(),
        }
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        match self {
            ResultSets::Jdbc(r) => r.read(buff),
        }
    }
}

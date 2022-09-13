use std::str::FromStr;

use ansilo_connectors_jdbc_mysql::{MysqlJdbcConnectionConfig, MysqlJdbcEntitySourceConfig};
use ansilo_connectors_native_postgres::{
    PooledClient, PostgresConnection, PostgresConnectionConfig, PostgresConnectionPool,
    PostgresEntitySourceConfig, UnpooledClient,
};
use ansilo_connectors_native_sqlite::{
    SqliteConnection, SqliteConnectionConfig, SqliteConnectionUnpool, SqliteEntitySourceConfig,
};
use ansilo_connectors_peer::{conf::PeerConfig, pool::PeerConnectionUnpool};
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
pub use ansilo_connectors_native_sqlite::SqliteConnector;
pub use ansilo_connectors_peer::PeerConnector;

#[derive(Debug, PartialEq)]
pub enum Connectors {
    OracleJdbc,
    MysqlJdbc,
    NativePostgres,
    NativeSqlite,
    Peer,
    Memory,
}

#[derive(Debug)]
pub enum ConnectionConfigs {
    OracleJdbc(OracleJdbcConnectionConfig),
    MysqlJdbc(MysqlJdbcConnectionConfig),
    NativePostgres(PostgresConnectionConfig),
    NativeSqlite(SqliteConnectionConfig),
    Peer(PeerConfig),
    Memory(MemoryDatabase),
}

#[derive(Debug)]
pub enum EntitySourceConfigs {
    OracleJdbc(OracleJdbcEntitySourceConfig),
    MysqlJdbc(MysqlJdbcEntitySourceConfig),
    NativePostgres(PostgresEntitySourceConfig),
    NativeSqlite(SqliteEntitySourceConfig),
    Peer(PostgresEntitySourceConfig),
    Memory(MemoryConnectorEntitySourceConfig),
}

#[derive(Clone)]
pub enum ConnectionPools {
    Jdbc(JdbcConnectionPool),
    NativePostgres(PostgresConnectionPool),
    NativeSqlite(SqliteConnectionUnpool),
    Peer(PeerConnectionUnpool),
    Memory(MemoryConnectionPool),
}

#[derive(Clone)]
pub enum ConnectorEntityConfigs {
    OracleJdbc(ConnectorEntityConfig<OracleJdbcEntitySourceConfig>),
    MysqlJdbc(ConnectorEntityConfig<MysqlJdbcEntitySourceConfig>),
    NativePostgres(ConnectorEntityConfig<PostgresEntitySourceConfig>),
    NativeSqlite(ConnectorEntityConfig<SqliteEntitySourceConfig>),
    Peer(ConnectorEntityConfig<PostgresEntitySourceConfig>),
    Memory(ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>),
}

pub enum Connections {
    Jdbc(JdbcConnection),
    NativePostgres(PostgresConnection<PooledClient>),
    NativeSqlite(SqliteConnection),
    Peer(PostgresConnection<UnpooledClient>),
    Memory(MemoryConnection),
}

impl Connectors {
    pub fn from_type(r#type: &str) -> Option<Self> {
        Some(match r#type {
            OracleJdbcConnector::TYPE => Connectors::OracleJdbc,
            MysqlJdbcConnector::TYPE => Connectors::MysqlJdbc,
            PostgresConnector::TYPE => Connectors::NativePostgres,
            SqliteConnector::TYPE => Connectors::NativeSqlite,
            PeerConnector::TYPE => Connectors::Peer,
            MemoryConnector::TYPE => Connectors::Memory,
            _ => return None,
        })
    }

    pub fn r#type(&self) -> &'static str {
        match self {
            Connectors::OracleJdbc => OracleJdbcConnector::TYPE,
            Connectors::MysqlJdbc => MysqlJdbcConnector::TYPE,
            Connectors::NativePostgres => PostgresConnector::TYPE,
            Connectors::NativeSqlite => SqliteConnector::TYPE,
            Connectors::Peer => PeerConnector::TYPE,
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
            Connectors::NativeSqlite => {
                ConnectionConfigs::NativeSqlite(SqliteConnector::parse_options(options)?)
            }
            Connectors::Peer => ConnectionConfigs::Peer(PeerConnector::parse_options(options)?),
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
            Connectors::NativeSqlite => EntitySourceConfigs::NativeSqlite(
                SqliteConnector::parse_entity_source_options(options)?,
            ),
            Connectors::Peer => {
                EntitySourceConfigs::Peer(PeerConnector::parse_entity_source_options(options)?)
            }
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
            (Connectors::NativePostgres, ConnectionConfigs::NativePostgres(options)) => {
                let (pool, entities) =
                    Self::create_pool::<PostgresConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::NativePostgres(pool),
                    ConnectorEntityConfigs::NativePostgres(entities),
                )
            }
            (Connectors::NativeSqlite, ConnectionConfigs::NativeSqlite(options)) => {
                let (pool, entities) =
                    Self::create_pool::<SqliteConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::NativeSqlite(pool),
                    ConnectorEntityConfigs::NativeSqlite(entities),
                )
            }
            (Connectors::Peer, ConnectionConfigs::Peer(options)) => {
                let (pool, entities) =
                    Self::create_pool::<PeerConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::Peer(pool),
                    ConnectorEntityConfigs::Peer(entities),
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
            ConnectionPools::NativeSqlite(p) => Connections::NativeSqlite(p.acquire(auth)?),
            ConnectionPools::Peer(p) => Connections::Peer(p.acquire(auth)?),
            ConnectionPools::Memory(p) => Connections::Memory(p.acquire(auth)?),
        })
    }
}

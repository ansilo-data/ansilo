use std::str::FromStr;

use ansilo_connectors_jdbc_mssql::{MssqlJdbcConnectionConfig, MssqlJdbcEntitySourceConfig};
use ansilo_connectors_jdbc_mysql::{MysqlJdbcConnectionConfig, MysqlJdbcEntitySourceConfig};
use ansilo_connectors_jdbc_teradata::{
    TeradataJdbcConnectionConfig, TeradataJdbcEntitySourceConfig,
};
use ansilo_connectors_native_mongodb::{
    MongodbConnection, MongodbConnectionConfig, MongodbConnectionUnpool, MongodbEntitySourceConfig,
};
use ansilo_connectors_native_postgres::{
    PooledClient, PostgresConnection, PostgresConnectionConfig, PostgresConnectionPool,
    PostgresEntitySourceConfig, UnpooledClient,
};
use ansilo_connectors_native_sqlite::{
    SqliteConnection, SqliteConnectionConfig, SqliteConnectionUnpool, SqliteEntitySourceConfig,
};
use ansilo_connectors_peer::{conf::PeerConfig, pool::PeerConnectionUnpool};
use ansilo_core::{
    config::{self, NodeConfig},
    err::{bail, Context, Result},
};

use ansilo_connectors_base::common::entity::ConnectorEntityConfig;

use ansilo_connectors_memory::{
    MemoryConnection, MemoryConnectionPool, MemoryConnectorEntitySourceConfig, MemoryDatabase,
};

use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcConnectionPool};

use ansilo_connectors_jdbc_oracle::{OracleJdbcConnectionConfig, OracleJdbcEntitySourceConfig};

use ansilo_connectors_base::interface::Connector;

pub use ansilo_connectors_internal::{InternalConnection, InternalConnector};
pub use ansilo_connectors_jdbc_mssql::MssqlJdbcConnector;
pub use ansilo_connectors_jdbc_mysql::MysqlJdbcConnector;
pub use ansilo_connectors_jdbc_oracle::OracleJdbcConnector;
pub use ansilo_connectors_jdbc_teradata::TeradataJdbcConnector;
pub use ansilo_connectors_memory::MemoryConnector;
pub use ansilo_connectors_native_mongodb::MongodbConnector;
pub use ansilo_connectors_native_postgres::PostgresConnector;
pub use ansilo_connectors_native_sqlite::SqliteConnector;
pub use ansilo_connectors_peer::PeerConnector;

#[derive(Debug, PartialEq)]
pub enum Connectors {
    OracleJdbc,
    MysqlJdbc,
    TeradataJdbc,
    MssqlJdbc,
    NativePostgres,
    NativeSqlite,
    NativeMongodb,
    Peer,
    Internal,
    Memory,
}

#[derive(Debug)]
pub enum ConnectionConfigs {
    OracleJdbc(OracleJdbcConnectionConfig),
    MysqlJdbc(MysqlJdbcConnectionConfig),
    TeradataJdbc(TeradataJdbcConnectionConfig),
    MssqlJdbc(MssqlJdbcConnectionConfig),
    NativePostgres(PostgresConnectionConfig),
    NativeSqlite(SqliteConnectionConfig),
    NativeMongodb(MongodbConnectionConfig),
    Peer(PeerConfig),
    Internal,
    Memory(MemoryDatabase),
}

#[derive(Debug)]
pub enum EntitySourceConfigs {
    OracleJdbc(OracleJdbcEntitySourceConfig),
    MysqlJdbc(MysqlJdbcEntitySourceConfig),
    TeradataJdbc(TeradataJdbcEntitySourceConfig),
    MssqlJdbc(MssqlJdbcEntitySourceConfig),
    NativePostgres(PostgresEntitySourceConfig),
    NativeSqlite(SqliteEntitySourceConfig),
    NativeMongodb(MongodbEntitySourceConfig),
    Peer(PostgresEntitySourceConfig),
    Internal,
    Memory(MemoryConnectorEntitySourceConfig),
}

#[derive(Clone)]
pub enum ConnectorEntityConfigs {
    OracleJdbc(ConnectorEntityConfig<OracleJdbcEntitySourceConfig>),
    MysqlJdbc(ConnectorEntityConfig<MysqlJdbcEntitySourceConfig>),
    TeradataJdbc(ConnectorEntityConfig<TeradataJdbcEntitySourceConfig>),
    MssqlJdbc(ConnectorEntityConfig<MssqlJdbcEntitySourceConfig>),
    NativePostgres(ConnectorEntityConfig<PostgresEntitySourceConfig>),
    NativeSqlite(ConnectorEntityConfig<SqliteEntitySourceConfig>),
    NativeMongodb(ConnectorEntityConfig<MongodbEntitySourceConfig>),
    Peer(ConnectorEntityConfig<PostgresEntitySourceConfig>),
    Internal,
    Memory(ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>),
}

#[derive(Clone)]
pub enum ConnectionPools {
    Jdbc(JdbcConnectionPool),
    NativePostgres(PostgresConnectionPool),
    NativeSqlite(SqliteConnectionUnpool),
    NativeMongodb(MongodbConnectionUnpool),
    Peer(PeerConnectionUnpool),
    Internal(InternalConnection),
    Memory(MemoryConnectionPool),
}

pub enum Connections {
    Jdbc(JdbcConnection),
    NativePostgres(PostgresConnection<PooledClient>),
    NativeSqlite(SqliteConnection),
    NativeMongodb(MongodbConnection),
    Peer(PostgresConnection<UnpooledClient>),
    Internal(InternalConnection),
    Memory(MemoryConnection),
}

impl Connectors {
    pub fn from_type(r#type: &str) -> Option<Self> {
        Some(match r#type {
            OracleJdbcConnector::TYPE => Connectors::OracleJdbc,
            MysqlJdbcConnector::TYPE => Connectors::MysqlJdbc,
            TeradataJdbcConnector::TYPE => Connectors::TeradataJdbc,
            MssqlJdbcConnector::TYPE => Connectors::MssqlJdbc,
            PostgresConnector::TYPE => Connectors::NativePostgres,
            SqliteConnector::TYPE => Connectors::NativeSqlite,
            MongodbConnector::TYPE => Connectors::NativeMongodb,
            PeerConnector::TYPE => Connectors::Peer,
            InternalConnector::TYPE => Connectors::Internal,
            MemoryConnector::TYPE => Connectors::Memory,
            _ => return None,
        })
    }

    pub fn r#type(&self) -> &'static str {
        match self {
            Connectors::OracleJdbc => OracleJdbcConnector::TYPE,
            Connectors::MysqlJdbc => MysqlJdbcConnector::TYPE,
            Connectors::TeradataJdbc => TeradataJdbcConnector::TYPE,
            Connectors::MssqlJdbc => MssqlJdbcConnector::TYPE,
            Connectors::NativePostgres => PostgresConnector::TYPE,
            Connectors::NativeSqlite => SqliteConnector::TYPE,
            Connectors::NativeMongodb => MongodbConnector::TYPE,
            Connectors::Peer => PeerConnector::TYPE,
            Connectors::Internal => InternalConnector::TYPE,
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
            Connectors::TeradataJdbc => {
                ConnectionConfigs::TeradataJdbc(TeradataJdbcConnector::parse_options(options)?)
            }
            Connectors::MssqlJdbc => {
                ConnectionConfigs::MssqlJdbc(MssqlJdbcConnector::parse_options(options)?)
            }
            Connectors::NativePostgres => {
                ConnectionConfigs::NativePostgres(PostgresConnector::parse_options(options)?)
            }
            Connectors::NativeSqlite => {
                ConnectionConfigs::NativeSqlite(SqliteConnector::parse_options(options)?)
            }
            Connectors::NativeMongodb => {
                ConnectionConfigs::NativeMongodb(MongodbConnector::parse_options(options)?)
            }
            Connectors::Peer => ConnectionConfigs::Peer(PeerConnector::parse_options(options)?),
            Connectors::Internal => ConnectionConfigs::Internal,
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
            Connectors::TeradataJdbc => EntitySourceConfigs::TeradataJdbc(
                TeradataJdbcConnector::parse_entity_source_options(options)?,
            ),
            Connectors::MssqlJdbc => EntitySourceConfigs::MssqlJdbc(
                MssqlJdbcConnector::parse_entity_source_options(options)?,
            ),
            Connectors::NativePostgres => EntitySourceConfigs::NativePostgres(
                PostgresConnector::parse_entity_source_options(options)?,
            ),
            Connectors::NativeSqlite => EntitySourceConfigs::NativeSqlite(
                SqliteConnector::parse_entity_source_options(options)?,
            ),
            Connectors::NativeMongodb => EntitySourceConfigs::NativeMongodb(
                MongodbConnector::parse_entity_source_options(options)?,
            ),
            Connectors::Peer => {
                EntitySourceConfigs::Peer(PeerConnector::parse_entity_source_options(options)?)
            }
            Connectors::Internal => EntitySourceConfigs::Internal,
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
            (Connectors::TeradataJdbc, ConnectionConfigs::TeradataJdbc(options)) => {
                let (pool, entities) =
                    Self::create_pool::<TeradataJdbcConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::Jdbc(pool),
                    ConnectorEntityConfigs::TeradataJdbc(entities),
                )
            }
            (Connectors::MssqlJdbc, ConnectionConfigs::MssqlJdbc(options)) => {
                let (pool, entities) =
                    Self::create_pool::<MssqlJdbcConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::Jdbc(pool),
                    ConnectorEntityConfigs::MssqlJdbc(entities),
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
            (Connectors::NativeMongodb, ConnectionConfigs::NativeMongodb(options)) => {
                let (pool, entities) =
                    Self::create_pool::<MongodbConnector>(options, nc, data_source_id)?;
                (
                    ConnectionPools::NativeMongodb(pool),
                    ConnectorEntityConfigs::NativeMongodb(entities),
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
            (Connectors::Internal, ConnectionConfigs::Internal) => {
                let (pool, _) = Self::create_pool::<InternalConnector>((), nc, data_source_id)?;
                (
                    ConnectionPools::Internal(pool),
                    ConnectorEntityConfigs::Internal,
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

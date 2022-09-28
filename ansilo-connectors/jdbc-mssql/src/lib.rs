use ansilo_connectors_base::{
    common::entity::ConnectorEntityConfig,
    interface::{ConnectionPool, Connector},
};
use ansilo_connectors_jdbc_base::{
    JdbcConnection, JdbcConnectionPool, JdbcPreparedQuery, JdbcQuery, JdbcResultSet,
    JdbcTransactionManager,
};

mod conf;
use ansilo_core::{
    config::{self, NodeConfig},
    err::Result,
};
pub use conf::*;
mod entity_searcher;
pub use entity_searcher::*;
mod entity_validator;
pub use entity_validator::*;
mod query_planner;
pub use query_planner::*;
mod query_compiler;
pub use query_compiler::*;

/// The connector for Mssql, built on their JDBC driver
#[derive(Default)]
pub struct MssqlJdbcConnector;

impl Connector for MssqlJdbcConnector {
    type TConnectionPool = JdbcConnectionPool;
    type TConnection = JdbcConnection;
    type TConnectionConfig = MssqlJdbcConnectionConfig;
    type TEntitySearcher = MssqlJdbcEntitySearcher;
    type TEntityValidator = MssqlJdbcEntityValidator;
    type TEntitySourceConfig = MssqlJdbcEntitySourceConfig;
    type TQueryPlanner = MssqlJdbcQueryPlanner;
    type TQueryCompiler = MssqlJdbcQueryCompiler;
    type TQueryHandle = JdbcPreparedQuery;
    type TQuery = JdbcQuery;
    type TResultSet = JdbcResultSet;
    type TTransactionManager = JdbcTransactionManager;

    const TYPE: &'static str = "jdbc.mssql";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        MssqlJdbcConnectionConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        MssqlJdbcEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        options: MssqlJdbcConnectionConfig,
        _nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        JdbcConnectionPool::new(options)
    }
}

impl MssqlJdbcConnector {
    /// Connects an mssql database
    pub fn connect(config: MssqlJdbcConnectionConfig) -> Result<<Self as Connector>::TConnection> {
        MssqlJdbcConnector::create_connection_pool(
            config.clone(),
            &NodeConfig::default(),
            &ConnectorEntityConfig::new(),
        )?
        .acquire(None)
    }
}

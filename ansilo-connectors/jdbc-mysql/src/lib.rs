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

/// The connector for Mysql, built on their JDBC driver
#[derive(Default)]
pub struct MysqlJdbcConnector;

impl Connector for MysqlJdbcConnector {
    type TConnectionPool = JdbcConnectionPool;
    type TConnection = JdbcConnection;
    type TConnectionConfig = MysqlJdbcConnectionConfig;
    type TEntitySearcher = MysqlJdbcEntitySearcher;
    type TEntityValidator = MysqlJdbcEntityValidator;
    type TEntitySourceConfig = MysqlJdbcEntitySourceConfig;
    type TQueryPlanner = MysqlJdbcQueryPlanner;
    type TQueryCompiler = MysqlJdbcQueryCompiler;
    type TQueryHandle = JdbcPreparedQuery;
    type TQuery = JdbcQuery;
    type TResultSet = JdbcResultSet;
    type TTransactionManager = JdbcTransactionManager;

    const TYPE: &'static str = "jdbc.mysql";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        MysqlJdbcConnectionConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        MysqlJdbcEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        options: MysqlJdbcConnectionConfig,
        _nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        JdbcConnectionPool::new(options)
    }
}

impl MysqlJdbcConnector {
    /// Connects an mysql database
    pub fn connect(config: MysqlJdbcConnectionConfig) -> Result<<Self as Connector>::TConnection> {
        MysqlJdbcConnector::create_connection_pool(
            config.clone(),
            &NodeConfig::default(),
            &ConnectorEntityConfig::new(),
        )?
        .acquire()
    }
}

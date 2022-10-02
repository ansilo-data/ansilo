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

/// The connector for Teradata, built on their JDBC driver
#[derive(Default)]
pub struct TeradataJdbcConnector;

impl Connector for TeradataJdbcConnector {
    type TConnectionPool = JdbcConnectionPool;
    type TConnection = JdbcConnection;
    type TConnectionConfig = TeradataJdbcConnectionConfig;
    type TEntitySearcher = TeradataJdbcEntitySearcher;
    type TEntityValidator = TeradataJdbcEntityValidator;
    type TEntitySourceConfig = TeradataJdbcEntitySourceConfig;
    type TQueryPlanner = TeradataJdbcQueryPlanner;
    type TQueryCompiler = TeradataJdbcQueryCompiler;
    type TQueryHandle = JdbcPreparedQuery;
    type TQuery = JdbcQuery;
    type TResultSet = JdbcResultSet;
    type TTransactionManager = JdbcTransactionManager;

    const TYPE: &'static str = "jdbc.teradata";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        TeradataJdbcConnectionConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        TeradataJdbcEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        options: TeradataJdbcConnectionConfig,
        nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        JdbcConnectionPool::new(&nc.resources, options)
    }
}

impl TeradataJdbcConnector {
    /// Connects an teradata database
    pub fn connect(
        config: TeradataJdbcConnectionConfig,
    ) -> Result<<Self as Connector>::TConnection> {
        TeradataJdbcConnector::create_connection_pool(
            config.clone(),
            &NodeConfig::default(),
            &ConnectorEntityConfig::new(),
        )?
        .acquire(None)
    }
}

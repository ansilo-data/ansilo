use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::Connector};
use ansilo_connectors_jdbc_base::{
    JdbcConnection, JdbcConnectionPool, JdbcDefaultTypeMapping, JdbcPreparedQuery, JdbcQuery,
    JdbcResultSet, JdbcTransactionManager,
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

/// The connector for Oracle, built on their JDBC driver
#[derive(Default)]
pub struct OracleJdbcConnector;

impl Connector for OracleJdbcConnector {
    type TConnectionPool = JdbcConnectionPool<JdbcDefaultTypeMapping>;
    type TConnection = JdbcConnection<JdbcDefaultTypeMapping>;
    type TConnectionConfig = OracleJdbcConnectionConfig;
    type TEntitySearcher = OracleJdbcEntitySearcher;
    type TEntityValidator = OracleJdbcEntityValidator;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;
    type TQueryPlanner = OracleJdbcQueryPlanner;
    type TQueryCompiler = OracleJdbcQueryCompiler;
    type TQueryHandle = JdbcPreparedQuery<JdbcDefaultTypeMapping>;
    type TQuery = JdbcQuery;
    type TResultSet = JdbcResultSet<JdbcDefaultTypeMapping>;
    type TTransactionManager = JdbcTransactionManager;

    const TYPE: &'static str = "jdbc.oracle";

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        OracleJdbcConnectionConfig::parse(options)
    }

    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig> {
        OracleJdbcEntitySourceConfig::parse(options)
    }

    fn create_connection_pool(
        options: OracleJdbcConnectionConfig,
        _nc: &NodeConfig,
        _entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool> {
        JdbcConnectionPool::new(options)
    }
}

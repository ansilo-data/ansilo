use crate::{
    interface::Connector,
    jdbc::{JdbcConnection, JdbcConnectionPool, JdbcPreparedQuery, JdbcQuery, JdbcResultSet},
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

impl<'a> Connector<'a> for OracleJdbcConnector {
    type TConnectionPool = JdbcConnectionPool<OracleJdbcConnectionConfig>;
    type TConnection = JdbcConnection<'a>;
    type TConnectionConfig = OracleJdbcConnectionConfig;
    type TEntitySearcher = OracleJdbcEntitySearcher;
    type TEntityValidator = OracleJdbcEntityValidator;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;
    type TQueryPlanner = OracleJdbcQueryPlanner;
    type TQueryCompiler = OracleJdbcQueryCompiler;
    type TQueryHandle = JdbcPreparedQuery<'a>;
    type TQuery = JdbcQuery;
    type TResultSet = JdbcResultSet<'a>;

    fn r#type() -> &'static str {
        "jdbc.oracle"
    }

    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig> {
        OracleJdbcConnectionConfig::parse(options)
    }

    fn create_connection_pool(
        options: OracleJdbcConnectionConfig,
        nc: &NodeConfig,
    ) -> Result<Self::TConnectionPool> {
        JdbcConnectionPool::new(options)
    }
}

#[cfg(test)]
mod tests {}

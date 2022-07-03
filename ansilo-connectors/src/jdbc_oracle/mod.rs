use crate::{jdbc::{JdbcConnectionOpener, JdbcConnection, JdbcPreparedQuery, JdbcQuery, JdbcResultSet}, interface::Connector};

mod conf;
use ansilo_core::{config, err::Result};
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
    type TConnectionOpener = JdbcConnectionOpener;
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

    fn parse_options(options: config::Value) -> Result<OracleJdbcConnectionConfig> {
        OracleJdbcConnectionConfig::parse(options)
    }

    fn create_connection_opener(
        _options: &OracleJdbcConnectionConfig,
    ) -> Result<JdbcConnectionOpener> {
        Ok(JdbcConnectionOpener::new())
    }

}

#[cfg(test)]
mod tests {}

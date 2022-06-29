use crate::jdbc::{JdbcConnectionOpener, JdbcConnector};

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
pub struct OracleJdbcConnector;

impl<'a>
    JdbcConnector<
        'a,
        OracleJdbcConnectionConfig,
        OracleJdbcEntitySearcher,
        OracleJdbcEntityValidator,
        OracleJdbcEntitySourceConfig,
        OracleJdbcQueryPlanner,
        OracleJdbcQueryCompiler,
    > for OracleJdbcConnector
{
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

    fn create_entity_searcher() -> Result<OracleJdbcEntitySearcher> {
        Ok(OracleJdbcEntitySearcher {})
    }

    fn create_entity_validator() -> Result<OracleJdbcEntityValidator> {
        Ok(OracleJdbcEntityValidator {})
    }

    fn create_query_planner() -> Result<OracleJdbcQueryPlanner> {
        Ok(OracleJdbcQueryPlanner {})
    }

    fn create_query_compiler() -> Result<OracleJdbcQueryCompiler> {
        Ok(OracleJdbcQueryCompiler {})
    }
}

#[cfg(test)]
mod tests {
}

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
    use std::collections::HashMap;

    use ansilo_core::common::data::DataValue;

    use crate::{
        common::ResultSetReader,
        interface::{Connection, ConnectionOpener, QueryHandle},
        jdbc::JdbcQuery,
    };

    use super::*;

    #[test]
    fn test_oracle_jdbc_open_connection_and_execute_query() {
        let config = OracleJdbcConnectionConfig::new("jdbc:oracle:thin:@oracle-database-dev.c52iuycbernx.ap-southeast-2.rds.amazonaws.com:1521/ANSILO".to_string(), {
            let mut props = HashMap::<String, String>::new();
            props.insert("oracle.jdbc.user".to_string(), "admin".to_string());
            props.insert("oracle.jdbc.password".to_string(), "&Qra8tMwifLV#yWHq74o".to_string());
            props
        });

        let con = OracleJdbcConnector::create_connection_opener(&config)
            .unwrap()
            .open(config)
            .unwrap();
        let mut query = con
            .prepare(JdbcQuery::new("SELECT * FROM DUAL", vec![]))
            .unwrap();
        let res = query.execute().unwrap();
        let mut res = ResultSetReader::new(res).unwrap();

        assert_eq!(
            res.read_data_value().unwrap(),
            Some(DataValue::Varchar("X".as_bytes().to_vec()))
        );
        assert_eq!(res.read_data_value().unwrap(), None);
    }
}

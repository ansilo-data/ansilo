use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use ansilo_connectors_base::interface::EntitySearcher;
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcDefaultTypeMapping};

use super::OracleJdbcEntitySourceConfig;

/// The entity searcher for Oracle JDBC
pub struct OracleJdbcEntitySearcher {}

impl EntitySearcher for OracleJdbcEntitySearcher {
    type TConnection = JdbcConnection<JdbcDefaultTypeMapping>;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

    fn discover(
        _connection: &mut Self::TConnection,
        _nc: &NodeConfig,
    ) -> Result<Vec<EntityConfig>> {
        todo!()
    }
}

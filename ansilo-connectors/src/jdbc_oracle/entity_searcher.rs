use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use crate::{
    interface::EntitySearcher,
    jdbc::{JdbcConnection, JdbcDefaultTypeMapping},
};

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

use ansilo_core::{config::NodeConfig, err::Result};

use crate::{common::entity::EntitySource, interface::EntitySearcher, jdbc::JdbcConnection};

use super::OracleJdbcEntitySourceConfig;

/// The entity searcher for Oracle JDBC
pub struct OracleJdbcEntitySearcher {}

impl EntitySearcher for OracleJdbcEntitySearcher {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

    fn discover(
        connection: &JdbcConnection,
        nc: &NodeConfig,
    ) -> Result<Vec<EntitySource<OracleJdbcEntitySourceConfig>>> {
        todo!()
    }
}

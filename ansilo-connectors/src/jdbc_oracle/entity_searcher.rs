use ansilo_core::{config::NodeConfig, err::Result};

use crate::{common::entity::EntitySource, interface::EntitySearcher, jdbc::JdbcConnection};

use super::OracleJdbcEntitySourceConfig;

/// The entity searcher for Oracle JDBC
pub struct OracleJdbcEntitySearcher {}

impl<'a> EntitySearcher<JdbcConnection<'a>, OracleJdbcEntitySourceConfig>
    for OracleJdbcEntitySearcher
{
    fn discover(
        &self,
        connection: &JdbcConnection<'a>,
        nc: &NodeConfig,
    ) -> Result<Vec<EntitySource<OracleJdbcEntitySourceConfig>>> {
        todo!()
    }
}

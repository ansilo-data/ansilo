use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use super::TeradataJdbcEntitySourceConfig;
use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};
use ansilo_connectors_jdbc_base::JdbcConnection;

/// The entity validator for Teradata JDBC
pub struct TeradataJdbcEntityValidator {}

impl EntityValidator for TeradataJdbcEntityValidator {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = TeradataJdbcEntitySourceConfig;

    fn validate(
        _connection: &mut Self::TConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<TeradataJdbcEntitySourceConfig>> {
        Ok(EntitySource::new(
            entity.clone(),
            TeradataJdbcEntitySourceConfig::parse(entity.source.options.clone())?,
        ))
    }
}

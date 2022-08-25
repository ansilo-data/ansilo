use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use super::OracleJdbcEntitySourceConfig;
use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};
use ansilo_connectors_jdbc_base::JdbcConnection;

/// The entity validator for Oracle JDBC
pub struct OracleJdbcEntityValidator {}

impl EntityValidator for OracleJdbcEntityValidator {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

    fn validate(
        _connection: &mut Self::TConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<OracleJdbcEntitySourceConfig>> {
        Ok(EntitySource::new(
            entity.clone(),
            OracleJdbcEntitySourceConfig::parse(entity.source.options.clone())?,
        ))
    }
}

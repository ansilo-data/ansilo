use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use super::MssqlJdbcEntitySourceConfig;
use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};
use ansilo_connectors_jdbc_base::JdbcConnection;

/// The entity validator for Mssql JDBC
pub struct MssqlJdbcEntityValidator {}

impl EntityValidator for MssqlJdbcEntityValidator {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = MssqlJdbcEntitySourceConfig;

    fn validate(
        _connection: &mut Self::TConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<MssqlJdbcEntitySourceConfig>> {
        Ok(EntitySource::new(
            entity.clone(),
            MssqlJdbcEntitySourceConfig::parse(entity.source.options.clone())?,
        ))
    }
}

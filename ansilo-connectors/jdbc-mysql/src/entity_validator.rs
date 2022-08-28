use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use super::MysqlJdbcEntitySourceConfig;
use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};
use ansilo_connectors_jdbc_base::JdbcConnection;

/// The entity validator for Mysql JDBC
pub struct MysqlJdbcEntityValidator {}

impl EntityValidator for MysqlJdbcEntityValidator {
    type TConnection = JdbcConnection;
    type TEntitySourceConfig = MysqlJdbcEntitySourceConfig;

    fn validate(
        _connection: &mut Self::TConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<MysqlJdbcEntitySourceConfig>> {
        Ok(EntitySource::new(
            entity.clone(),
            MysqlJdbcEntitySourceConfig::parse(entity.source.options.clone())?,
        ))
    }
}

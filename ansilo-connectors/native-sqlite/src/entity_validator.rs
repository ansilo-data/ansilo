use crate::SqliteConnection;
use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use super::SqliteEntitySourceConfig;
use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};

/// The entity validator for Sqlite
pub struct SqliteEntityValidator {}

impl EntityValidator for SqliteEntityValidator {
    type TConnection = SqliteConnection;
    type TEntitySourceConfig = SqliteEntitySourceConfig;

    fn validate(
        _connection: &mut Self::TConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<SqliteEntitySourceConfig>> {
        Ok(EntitySource::new(
            entity.clone(),
            SqliteEntitySourceConfig::parse(entity.source.options.clone())?,
        ))
    }
}

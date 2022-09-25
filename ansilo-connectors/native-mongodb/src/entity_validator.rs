use crate::MongodbConnection;
use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use super::MongodbEntitySourceConfig;
use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};

/// The entity validator for Mongodb
pub struct MongodbEntityValidator {}

impl EntityValidator for MongodbEntityValidator {
    type TConnection = MongodbConnection;
    type TEntitySourceConfig = MongodbEntitySourceConfig;

    fn validate(
        _connection: &mut Self::TConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<MongodbEntitySourceConfig>> {
        Ok(EntitySource::new(
            entity.clone(),
            MongodbEntitySourceConfig::parse(entity.source.options.clone())?,
        ))
    }
}

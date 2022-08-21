use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};

use super::{MemoryConnection, MemoryConnectorEntitySourceConfig};

pub struct MemoryEntityValidator {}

impl EntityValidator for MemoryEntityValidator {
    type TConnection = MemoryConnection;
    type TEntitySourceConfig = MemoryConnectorEntitySourceConfig;

    fn validate(
        _connection: &mut MemoryConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<MemoryConnectorEntitySourceConfig>> {
        Ok(EntitySource::new(
            entity.clone(),
            MemoryConnectorEntitySourceConfig::parse(entity.source.options.clone())?,
        ))
    }
}

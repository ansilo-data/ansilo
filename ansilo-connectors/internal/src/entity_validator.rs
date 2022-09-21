use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};

use crate::InternalConnection;

pub struct InternalEntityValidator;

impl EntityValidator for InternalEntityValidator {
    type TConnection = InternalConnection;
    type TEntitySourceConfig = ();

    fn validate(
        _connection: &mut InternalConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<()>> {
        Ok(EntitySource::new(entity.clone(), ()))
    }
}

use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};

use super::{ArvoConnection, ArvoFile};

pub struct ArvoEntityValidator {}

impl EntityValidator for ArvoEntityValidator {
    type TConnection = ArvoConnection;
    type TEntitySourceConfig = ArvoFile;

    fn validate(
        _con: &mut ArvoConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<ArvoFile>> {
        Ok(EntitySource::new(
            entity.clone(),
            ArvoFile::parse(entity.source.options.clone())?,
        ))
    }
}

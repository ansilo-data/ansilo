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
        con: &mut ArvoConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<ArvoFile>> {
        Ok(EntitySource::new(
            entity.clone(),
            ArvoFile::new(
                con.conf().path().join(entity.id).with_extension(".arvo"),
                None,
            ),
        ))
    }
}

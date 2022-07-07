use ansilo_core::{
    config::{EntityVersionConfig, NodeConfig},
    err::Result,
};

use crate::{common::entity::EntitySource, interface::EntityValidator, jdbc::JdbcConnection};

use super::MemoryConnection;

pub struct MemoryEntityValidator {}

impl EntityValidator for MemoryEntityValidator {
    type TConnection = MemoryConnection;
    type TEntitySourceConfig = ();

    fn validate(
        _connection: &MemoryConnection,
        _entity_version: &EntityVersionConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<()>> {
        todo!()
    }
}

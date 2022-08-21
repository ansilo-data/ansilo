use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use crate::common::entity::EntitySource;

use super::Connection;

/// Validates custom entity config
pub trait EntityValidator {
    type TConnection: Connection;
    type TEntitySourceConfig;

    /// Validate the supplied entity config
    fn validate(
        connection: &mut Self::TConnection,
        entity: &EntityConfig,
        nc: &NodeConfig,
    ) -> Result<EntitySource<Self::TEntitySourceConfig>>;
}

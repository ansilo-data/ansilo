use ansilo_core::{
    config::{EntityVersionConfig, NodeConfig},
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
        connection: &Self::TConnection,
        entity_version: &EntityVersionConfig,
        nc: &NodeConfig,
    ) -> Result<EntitySource<Self::TEntitySourceConfig>>;
}

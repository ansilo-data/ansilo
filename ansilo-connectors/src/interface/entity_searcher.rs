use crate::common::entity::EntitySource;
use ansilo_core::{config::NodeConfig, err::Result};

use super::Connection;

/// Discovers entity schemas from the data source
pub trait EntitySearcher {
    type TConnection: Connection;
    type TEntitySourceConfig;

    /// Retrieves the list of entities from the target data source
    /// Typlically these entities will have their accessibility set to internal
    fn discover(
        connection: &mut Self::TConnection,
        nc: &NodeConfig,
    ) -> Result<Vec<EntitySource<Self::TEntitySourceConfig>>>;
}

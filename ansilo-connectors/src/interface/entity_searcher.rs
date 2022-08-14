use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use super::Connection;

/// Discovers entity schemas from the data source
pub trait EntitySearcher {
    type TConnection: Connection;
    type TEntitySourceConfig;

    /// Retrieves the list of entities from the target data source
    fn discover(connection: &mut Self::TConnection, nc: &NodeConfig) -> Result<Vec<EntityConfig>>;
}

use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use ansilo_connectors_base::interface::EntitySearcher;

use super::{MemoryConnection, MemoryConnectorEntitySourceConfig};

pub struct MemoryEntitySearcher {}

impl EntitySearcher for MemoryEntitySearcher {
    type TConnection = MemoryConnection;
    type TEntitySourceConfig = MemoryConnectorEntitySourceConfig;

    fn discover(connection: &mut MemoryConnection, _nc: &NodeConfig) -> Result<Vec<EntityConfig>> {
        Ok(connection
            .conf
            .entities()
            .map(|i| i.conf.clone())
            .collect::<Vec<_>>())
    }
}

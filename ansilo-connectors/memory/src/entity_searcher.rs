use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use super::{MemoryConnection, MemoryConnectorEntitySourceConfig};

pub struct MemoryEntitySearcher {}

impl EntitySearcher for MemoryEntitySearcher {
    type TConnection = MemoryConnection;
    type TEntitySourceConfig = MemoryConnectorEntitySourceConfig;

    fn discover(
        connection: &mut MemoryConnection,
        _nc: &NodeConfig,
        _opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        Ok(connection
            .conf
            .entities()
            .map(|i| i.conf.clone())
            .collect::<Vec<_>>())
    }
}

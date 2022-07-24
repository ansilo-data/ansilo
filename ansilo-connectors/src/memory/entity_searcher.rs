use ansilo_core::{config::NodeConfig, err::Result};

use crate::{common::entity::EntitySource, interface::EntitySearcher};

use super::{MemoryConnection, MemoryConnectorEntitySourceConfig};

pub struct MemoryEntitySearcher {}

impl EntitySearcher for MemoryEntitySearcher {
    type TConnection = MemoryConnection;
    type TEntitySourceConfig = MemoryConnectorEntitySourceConfig;

    fn discover(_connection: &MemoryConnection, _nc: &NodeConfig) -> Result<Vec<EntitySource<MemoryConnectorEntitySourceConfig>>> {
        Ok(vec![])
    }
}

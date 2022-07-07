use ansilo_core::{config::NodeConfig, err::Result};

use crate::{common::entity::EntitySource, interface::EntitySearcher, jdbc::JdbcConnection};

use super::MemoryConnection;

pub struct MemoryEntitySearcher {}

impl EntitySearcher for MemoryEntitySearcher {
    type TConnection = MemoryConnection;
    type TEntitySourceConfig = ();

    fn discover(_connection: &MemoryConnection, _nc: &NodeConfig) -> Result<Vec<EntitySource<()>>> {
        Ok(vec![])
    }
}

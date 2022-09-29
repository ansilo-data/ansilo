use std::collections::HashMap;

use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::Connection;

/// Options for discovering entity schemas from remote data sources
/// It is up to the connector as to how these options are interpreted.
#[derive(PartialEq, Debug, Default, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EntityDiscoverOptions {
    /// Remote schema specifier
    pub remote_schema: Option<String>,
    /// Other options
    pub other: HashMap<String, String>,
}

impl EntityDiscoverOptions {
    pub fn new(remote_schema: impl Into<String>, other: HashMap<String, String>) -> Self {
        let remote_schema = remote_schema.into();

        Self {
            remote_schema: if remote_schema.is_empty() {
                None
            } else {
                Some(remote_schema)
            },
            other,
        }
    }

    pub fn schema(remote_schema: impl Into<String>) -> Self {
        Self {
            remote_schema: Some(remote_schema.into()),
            other: HashMap::default(),
        }
    }
}

/// Discovers entity schemas from the data source
pub trait EntitySearcher {
    type TConnection: Connection;
    type TEntitySourceConfig;

    /// Retrieves the list of entities from the target data source
    fn discover(
        connection: &mut Self::TConnection,
        nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>>;
}

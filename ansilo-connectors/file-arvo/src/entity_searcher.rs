use std::fs;

use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::{Context, Result},
};

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};
use ansilo_logging::warn;
use wildmatch::WildMatch;

use crate::schema::parse_arvo_schema;

use super::{ArvoConnection, ArvoFile};

pub struct ArvoEntitySearcher {}

impl EntitySearcher for ArvoEntitySearcher {
    type TConnection = ArvoConnection;
    type TEntitySourceConfig = ArvoFile;

    fn discover(
        con: &mut ArvoConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        let pattern = WildMatch::new(opts.remote_schema.as_ref().unwrap_or(&"*".into()));
        let mut entities = vec![];

        // Find arvo files in the configured path
        for file in fs::read_dir(con.conf().path()).context("Failed to read dir")? {
            let file = file?;
            let name = file.file_name().to_string_lossy().to_string();

            if !name.ends_with(".arvo") {
                continue;
            }

            if !pattern.matches(&name) {
                continue;
            }

            let path = file.path();
            match parse_arvo_schema(path.clone()) {
                Ok(e) => entities.push(e),
                Err(err) => warn!("Failed to parse {}: {:?}", path.display(), err),
            }
        }

        Ok(entities)
    }
}

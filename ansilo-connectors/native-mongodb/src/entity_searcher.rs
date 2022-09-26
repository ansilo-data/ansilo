use ansilo_core::{
    config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    data::DataType,
    err::{Context, Result},
};

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};
use ansilo_logging::warn;
use mongodb::results::CollectionSpecification;
use wildmatch::WildMatch;

use crate::{MongodbCollectionOptions, MongodbConnection};

use super::MongodbEntitySourceConfig;

/// The entity searcher for Mongodb
pub struct MongodbEntitySearcher {}

impl EntitySearcher for MongodbEntitySearcher {
    type TConnection = MongodbConnection;
    type TEntitySourceConfig = MongodbEntitySourceConfig;

    fn discover(
        connection: &mut Self::TConnection,
        _nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        let client = connection.client();

        // Parse the collection filters
        let filter = opts.remote_schema.as_ref().cloned().unwrap_or_default();
        let mut filter = filter.split('.');
        let filter_db = WildMatch::new(filter.next().unwrap_or("*"));
        let filter_collection = WildMatch::new(filter.next().unwrap_or("*"));

        // Get all remote dbs
        let dbs = client
            .list_database_names(None, None)
            .context("Failed to list databases")?;

        // Filter dbs
        let dbs = dbs
            .into_iter()
            .filter(|db| filter_db.matches(db))
            .collect::<Vec<_>>();

        let mut collections = vec![];

        for db in dbs.into_iter() {
            // Get collections
            let db = client.database(&db);
            let cols = db
                .list_collections(None, None)
                .context("Failed to list collections")?;

            // Filter collections
            for col in cols {
                let col = col?;
                if filter_collection.matches(&col.name) {
                    collections.push((db.name().to_string(), col));
                }
            }
        }

        let entities = collections
            .into_iter()
            .filter_map(|(db, col)| match parse_entity_config(&db, col.clone()) {
                Ok(conf) => Some(conf),
                Err(err) => {
                    warn!(
                        "Failed to import schema for collection \"{}\": {:?}",
                        col.name, err
                    );
                    None
                }
            })
            .collect();

        Ok(entities)
    }
}

// We expose collections as tables with a single JSON column
// Perhaps in future we could support mapping to a more rigid schema.
pub(crate) fn parse_entity_config(
    database: &str,
    collection: CollectionSpecification,
) -> Result<EntityConfig> {
    Ok(EntityConfig::new(
        collection.name.clone(),
        None,
        None,
        vec![],
        vec![EntityAttributeConfig::new(
            "doc".to_string(),
            None,
            DataType::JSON,
            false,
            false,
        )],
        vec![],
        EntitySourceConfig::from(MongodbEntitySourceConfig::Collection(
            MongodbCollectionOptions::new(database.to_string(), collection.name.clone()),
        ))?,
    ))
}

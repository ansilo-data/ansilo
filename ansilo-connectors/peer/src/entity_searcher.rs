use ansilo_connectors_native_postgres::{PostgresEntitySearcher, UnpooledClient};
use ansilo_core::{
    config::{DataSourceConfig, EntityConfig, EntitySourceConfig, NodeConfig},
    err::{Context, Result},
    web::catalog::{Catalog, CatalogEntitySource},
};

use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};

use crate::{conf::PeerConfig, PostgresConnection, PostgresTableOptions};

use super::PostgresEntitySourceConfig;

/// The entity searcher for peer nodes
///
/// The public API schema of peer nodes are exposed through a http endpoint
/// and do not require authentication to access.
pub struct PeerEntitySearcher {}

impl EntitySearcher for PeerEntitySearcher {
    type TConnection = PostgresConnection<UnpooledClient>;
    type TEntitySourceConfig = PostgresEntitySourceConfig;

    fn discover(
        connection: &mut Self::TConnection,
        nc: &NodeConfig,
        opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        PostgresEntitySearcher::discover(connection, nc, opts)
    }
}

impl PeerEntitySearcher {
    pub fn discover_unauthenticated(
        conf: &DataSourceConfig,
        _opts: EntityDiscoverOptions,
    ) -> Result<Vec<EntityConfig>> {
        let conf = PeerConfig::parse(conf.options.clone()).context("Failed to parse options")?;
        let mut url = conf.url.clone();
        url.set_path("/api/v1/catalog");

        let catalog = reqwest::blocking::get(url.clone())
            .context("Failed to retrieve schema from peer")?
            .error_for_status()
            .context("Error response returned from peer")?
            .json::<Catalog>()
            .context("Failed to parse entity schema from catalog response")?;

        Ok(catalog
            .entities
            .into_iter()
            .map(|e| {
                EntityConfig::new(
                    e.id,
                    e.name,
                    e.description,
                    e.tags,
                    e.attributes.into_iter().map(|a| a.attribute).collect(),
                    e.constraints,
                    EntitySourceConfig::from(PostgresEntitySourceConfig::Table(
                        PostgresTableOptions::peer(
                            "public".into(),
                            CatalogEntitySource::parent(
                                e.source.table_name.clone(),
                                conf.url.to_string(),
                                e.source.clone(),
                            ),
                        ),
                    ))
                    .unwrap(),
                )
            })
            .collect())
    }
}

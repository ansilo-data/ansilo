use ansilo_connectors_base::interface::EntityDiscoverOptions;
use ansilo_connectors_native_postgres::{PostgresEntitySearcher, UnpooledClient};
use ansilo_core::{
    config::{
        EntityAttributeConfig, EntityConfig, EntityConstraintConfig, NodeConfig, TagValueConfig,
    },
    err::{Context, Result},
};
use ansilo_logging::error;
use axum::{extract::State, Json};
use hyper::StatusCode;
use itertools::Itertools;
use serde::Serialize;

use crate::HttpApiState;

/// Model for exposing the data catalog of this instance.
/// As a convention, we define the data catalog as all tables and
/// views in the postgres "public" schema.
///
/// We dont want to all underlying config of the entity, only
/// the schema itself, the type of data source, and if this
/// entity is imported from a peer instance, we expose its lineage.
#[derive(Debug, Serialize)]

pub struct Catalog {
    entities: Vec<CatalogEntity>,
}
#[derive(Debug, Serialize)]
pub struct CatalogEntity {
    id: String,
    name: Option<String>,
    description: Option<String>,
    tags: Vec<TagValueConfig>,
    attributes: Vec<CatalogEntityAttribue>,
    constraints: Vec<EntityConstraintConfig>,
    source: CatalogEntitySource,
}

#[derive(Debug, Serialize)]
pub struct CatalogEntitySource {
    /// If this entity is imported from a peer node, we expose the URL
    /// of that node.
    /// This allows a lineage to be formed if data is exposed through
    /// "hops" along multiple nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    /// If this entity is imported from a peer node, we also expose
    /// the source provided by the peer. This is exposed recursively
    /// allowing the full lineage to appear.
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<Box<Self>>,
}

#[derive(Debug, Serialize)]
pub struct CatalogEntityAttribue {
    #[serde(flatten)]
    attribute: EntityAttributeConfig,
    // TODO: expose data lineage through querying information schema VIEW_COLUMN_USAGE
    // sources: Vec<String>
}

pub(super) async fn handler(
    State(state): State<HttpApiState>,
) -> Result<Json<Catalog>, (StatusCode, &'static str)> {
    // First retrieve an admin connection to postgres
    let mut con = state.pools().admin().await.map_err(|e| {
        error!("{:?}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Connection error")
    })?;

    // Then discover all the table schema's from the "public" schema
    let entities = PostgresEntitySearcher::<&mut UnpooledClient>::discover_async(
        &mut con,
        EntityDiscoverOptions::new("public.%".into(), Default::default()),
    )
    .await
    .map_err(|e| {
        error!("{:?}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
    })?;

    // Now some of those tables could be foreign tables which have
    // been imported from a peer node. In this case we want get the
    // entity config's directly, as these will contain the source
    // of these entities, providing a lineage through the network.
    let foreign_entities = con
        .query(
            r#"
            SELECT
                __ansilo_private.get_entity_config(t.oid) as conf
            FROM pg_class t
            WHERE t.relnamespace = 'public'::regnamespace
            AND t.relkind = 'f'
            "#,
            &[],
        )
        .await
        .map_err(|e| {
            error!("{:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
        })?;

    let foreign_entities = foreign_entities
        .into_iter()
        .map(|e| serde_json::from_value(e.get(0)))
        .collect::<Result<Vec<EntityConfig>, _>>()
        .map_err(|e| {
            error!("{:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
        })?;

    // The original discovery may contain entries for the entities
    // discovered through through the above query, we want our
    // ones to take precedence so we combine them below
    let entities = foreign_entities
        .into_iter()
        .chain(entities.into_iter())
        .unique_by(|e| e.id.clone());

    // Finally, map our entities to the data models we want to expose
    let entities = entities
        .map(|e| CatalogEntity::from(state.conf(), e))
        .collect::<Result<Vec<_>>>()
        .map_err(|e| {
            error!("{:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
        })?;

    Ok(Json(Catalog { entities }))
}

impl CatalogEntity {
    fn from(conf: &NodeConfig, e: EntityConfig) -> Result<Self> {
        let source = conf.sources.iter().find(|i| i.id == e.source.data_source);

        Ok(Self {
            id: e.id,
            name: e.name,
            description: e.description,
            tags: e.tags,
            attributes: e
                .attributes
                .into_iter()
                .map(|a| CatalogEntityAttribue { attribute: a })
                .collect(),
            constraints: e.constraints,
            source: CatalogEntitySource {
                url: if source.map(|i| i.r#type.as_str()) == Some("peer") {
                    Some(
                        source.unwrap().options["url"]
                            .as_str()
                            .context("type")?
                            .into(),
                    )
                } else {
                    None
                },
                source: if source.map(|i| i.r#type.as_str()) == Some("peer") {
                    // TODO: deserialise from
                    // source.unwrap().options["source"]
                    todo!()
                } else {
                    None
                },
            },
        })
    }
}

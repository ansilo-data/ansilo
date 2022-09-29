use ansilo_connectors_base::interface::EntityDiscoverOptions;
use ansilo_connectors_native_postgres::{
    PostgresEntitySearcher, PostgresEntitySourceConfig, UnpooledClient,
};
use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
    web::catalog::*,
};
use ansilo_logging::error;
use axum::{extract::State, Json};
use hyper::StatusCode;
use itertools::Itertools;

use crate::HttpApiState;

pub(super) async fn handler(
    State(state): State<HttpApiState>,
) -> Result<Json<Catalog>, (StatusCode, &'static str)> {
    // First retrieve an admin connection to postgres
    let mut con = state.pools().admin().await.map_err(|e| {
        error!("{:?}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Connection error")
    })?;

    // Then discover all the table schema's from the "public" schema
    let entities = PostgresEntitySearcher::<UnpooledClient>::discover_async(
        &mut con,
        EntityDiscoverOptions::new("public.%", Default::default()),
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
                t.relname as table_name,
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
        .map(|e| Ok((e.get(0), serde_json::from_value(e.get(1))?)))
        .collect::<Result<Vec<(String, EntityConfig)>, _>>()
        .map_err(|e: serde_json::Error| {
            error!("{:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
        })?;

    // The original discovery may contain entries for the entities
    // discovered through through the above query, we want our
    // ones to take precedence so we combine them below
    let entities = foreign_entities
        .into_iter()
        .chain(entities.into_iter().map(|e| (e.id.clone(), e)))
        .unique_by(|(_, e)| e.id.clone());

    // Finally, map our entities to the data models we want to expose
    let entities = entities
        .map(|(t, e)| to_catalog(state.conf(), e, t))
        .collect::<Result<Vec<_>>>()
        .map_err(|e| {
            error!("{:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
        })?;

    Ok(Json(Catalog { entities }))
}

fn to_catalog(conf: &NodeConfig, e: EntityConfig, table_name: String) -> Result<CatalogEntity> {
    let source = conf
        .sources
        .iter()
        .find(|i| i.id == e.source.data_source)
        .and_then(|i| {
            if i.r#type.as_str() == "peer" {
                serde_yaml::from_value::<PostgresEntitySourceConfig>(e.source.options).ok()
            } else {
                None
            }
        })
        .and_then(|i| i.as_table().cloned())
        .and_then(|i| i.source)
        .unwrap_or_else(|| CatalogEntitySource::table(table_name));

    Ok(CatalogEntity {
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
        source,
    })
}

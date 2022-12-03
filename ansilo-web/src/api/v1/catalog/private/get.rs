use std::{collections::HashSet, sync::Arc};

use ansilo_connectors_base::interface::EntityDiscoverOptions;
use ansilo_connectors_native_postgres::{PostgresEntitySearcher, UnpooledClient};
use ansilo_core::{config::EntityConfig, err::Result, web::catalog::*};
use ansilo_logging::error;
use axum::{extract::State, Extension, Json};
use hyper::StatusCode;
use itertools::Itertools;

use crate::{
    api::v1::catalog::common::to_catalog,
    middleware::pg_auth::ClientAuthenticatedPostgresConnection, HttpApiState,
};

/// Gets the private data catalog of this node.
/// We define the private catalog as the tables and views outside of the "public"
/// schema which the authenticated user has access to.
pub(super) async fn handler(
    State(state): State<Arc<HttpApiState>>,
    Extension(con): Extension<ClientAuthenticatedPostgresConnection>,
) -> Result<Json<Catalog>, (StatusCode, &'static str)> {
    let con = con.0.lock().await;

    // Discover all the table schemas accessible to the user
    let entities = PostgresEntitySearcher::<UnpooledClient>::discover_async(
        &*con.client_async().await,
        EntityDiscoverOptions::new(
            "%",
            [
                ("include_schema_in_id".into(), "true".into()),
                ("exclude_internal".into(), "true".into()),
            ]
            .into_iter()
            .collect(),
        ),
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
    let foreign_entities = state
        .pools()
        .admin()
        .await
        .map_err(|e| {
            error!("{:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal error")
        })?
        .query(
            r#"
            SELECT
                s.nspname || '.' || t.relname as entity_id,
                __ansilo_private.get_entity_config(t.oid) as conf
            FROM pg_class t
            INNER JOIN pg_namespace s on t.relnamespace = s.oid
            WHERE t.relnamespace != 'public'::regnamespace
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

    // Filter to the entities the user has access to
    let lookup = entities.iter().map(|e| &e.id).collect::<HashSet<_>>();
    let foreign_entities = foreign_entities
        .into_iter()
        .filter(|(id, _)| lookup.contains(id))
        .collect::<Vec<_>>();

    // Override the entity id's with schema prefix
    let foreign_entities = foreign_entities.into_iter().map(|(id, mut e)| {
        e.id = id.clone();
        (id, e)
    });

    // The original discovery may contain entries for the entities
    // discovered through through the above query, we want our
    // ones to take precedence so we combine them below
    let entities = foreign_entities
        .chain(entities.into_iter().map(|e| (e.id.clone(), e)))
        .unique_by(|(_, e)| e.id.clone());

    // Exclude "public" and internal schemas
    let entities = entities.filter(|(i, _)| {
        !i.starts_with("public.")
            && !i.starts_with("information_schema.")
            && !i.starts_with("pg_catalog.")
            && !i.starts_with("ansilo_catalog.")
    });

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

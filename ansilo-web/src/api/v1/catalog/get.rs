use ansilo_connectors_base::interface::{EntityDiscoverOptions, EntitySearcher};
use ansilo_connectors_native_postgres::{PostgresConnection, PostgresEntitySearcher, PooledClient};
use ansilo_core::config::EntityConfig;
use axum::{extract::State, Json};
use serde::Serialize;

use crate::HttpApiState;

#[derive(Debug, Serialize)]
pub struct Catalog {
    entities: Vec<EntityConfig>,
}

pub(super) async fn handler(State(state): State<HttpApiState>) -> Json<Catalog> {
    let mut con = PostgresConnection::new(PooledClient(state.pools().admin().await.unwrap()));

    let entities = PostgresEntitySearcher::discover(
        &mut con,
        state.conf(),
        EntityDiscoverOptions::new("public".into(), Default::default()),
    )
    .unwrap();

    Json(Catalog { entities })
}

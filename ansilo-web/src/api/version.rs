use std::sync::Arc;

use axum::{extract::State, routing, Json, Router};

use crate::{HttpApiState, VersionInfo};

async fn handler(State(state): State<Arc<HttpApiState>>) -> Json<VersionInfo> {
    Json(state.version_info().clone())
}

pub(super) fn router() -> Router<Arc<HttpApiState>> {
    Router::new().route("/", routing::get(handler))
}

use std::sync::Arc;

use axum::{extract::State, routing, Json, Router};

use crate::{HttpApiState, VersionInfo};

async fn handler(State(state): State<HttpApiState>) -> Json<VersionInfo> {
    Json(state.version_info().clone())
}

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state).route("/", routing::get(handler))
}

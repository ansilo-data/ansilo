use std::sync::Arc;

use axum::Router;

use crate::HttpApiState;

mod healthcheck;
mod v1;
mod version;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state.clone())
        .nest("/v1", v1::router(state.clone()))
        .nest("/health", healthcheck::router(state.clone()))
        .nest("/version", version::router(state.clone()))
}

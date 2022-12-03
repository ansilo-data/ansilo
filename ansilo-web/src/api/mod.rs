use std::sync::Arc;

use axum::Router;

use crate::HttpApiState;

pub mod healthcheck;
pub mod v1;
pub mod version;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<Arc<HttpApiState>> {
    Router::new()
        .nest("/v1", v1::router(state.clone()))
        .nest("/health", healthcheck::router())
        .nest("/version", version::router())
}

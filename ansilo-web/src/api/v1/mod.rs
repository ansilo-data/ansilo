use std::sync::Arc;

use axum::Router;

use crate::HttpApiState;

pub mod auth;
pub mod catalog;
pub mod node;
pub mod query;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<Arc<HttpApiState>> {
    Router::new()
        .nest("/node", node::router())
        .nest("/catalog", catalog::router(state.clone()))
        .nest("/auth", auth::router())
        .nest("/query", query::router(state.clone()))
}

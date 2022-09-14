use std::sync::Arc;

use axum::Router;

use crate::HttpApiState;

pub mod auth;
pub mod catalog;
pub mod query;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state.clone())
        .nest("/catalog", catalog::router(state.clone()))
        .nest("/auth", auth::router(state.clone()))
        .nest("/query", query::router(state.clone()))
}

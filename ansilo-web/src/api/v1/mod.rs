use std::sync::Arc;

use axum::Router;

use crate::HttpApiState;

pub mod catalog;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state.clone()).nest("/catalog", catalog::router(state.clone()))
}

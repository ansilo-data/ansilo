use std::sync::Arc;

use axum::Router;

use crate::HttpApiState;

pub mod v1;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state.clone()).nest("/v1", v1::router(state.clone()))
}

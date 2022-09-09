use std::sync::Arc;

use axum::{routing, Router};

use crate::HttpApiState;

mod get;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state).route("/", routing::get(get::handler))
}

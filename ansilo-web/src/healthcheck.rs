use std::sync::Arc;

use axum::{routing, Router};

use crate::HttpApiState;

async fn handler() -> &'static str {
    // TODO: better healthchecks for subsytems?
    "Ok"
}

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state).route("/", routing::get(handler))
}

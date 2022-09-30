use std::sync::Arc;

use axum::{routing, Router};

use crate::HttpApiState;

mod common;
mod get;
mod private;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state.clone())
        .route("/", routing::get(get::handler))
        .nest("/private", private::router(state.clone()))
}

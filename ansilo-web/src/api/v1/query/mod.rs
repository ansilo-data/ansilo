use std::sync::Arc;

use axum::{routing, Router};

use crate::{middleware::pg_auth, HttpApiState};

pub mod post;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<HttpApiState> {
    Router::with_state_arc(state.clone())
        .route("/", routing::post(post::handler))
        .route_layer({
            let state = state.clone();
            axum::middleware::from_fn(move |req, next| pg_auth::auth(req, next, state.clone()))
        })
}

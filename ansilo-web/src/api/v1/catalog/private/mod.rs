use std::sync::Arc;

use axum::{routing, Router};

use crate::{middleware::pg_auth, HttpApiState};

pub mod get;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<Arc<HttpApiState>> {
    Router::new()
        .route("/", routing::get(get::handler))
        .route_layer({
            let state = state.clone();
            axum::middleware::from_fn(move |req, next| pg_auth::auth(req, next, state.clone()))
        })
}

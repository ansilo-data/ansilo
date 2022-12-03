use std::sync::Arc;

use axum::{routing, Router};

use crate::HttpApiState;

mod common;
mod get;
mod private;

pub(super) fn router(state: Arc<HttpApiState>) -> Router<Arc<HttpApiState>> {
    Router::new()
        .route("/", routing::get(get::handler))
        .nest("/private", private::router(state))
}

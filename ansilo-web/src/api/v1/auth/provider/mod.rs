use std::sync::Arc;

use axum::{routing, Router};

use crate::HttpApiState;

pub mod get;

pub(super) fn router() -> Router<Arc<HttpApiState>> {
    Router::new().route("/", routing::get(get::handler))
}

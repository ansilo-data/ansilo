use axum::{routing, Router};

async fn handler() -> &'static str {
    "Ok"
}

pub(super) fn router() -> Router {
    Router::new().route("/", routing::get(handler))
}

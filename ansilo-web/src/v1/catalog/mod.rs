use axum::{routing, Router};

mod get;

pub(super) fn router() -> Router {
    Router::new().route("/", routing::get(get::handler))
}

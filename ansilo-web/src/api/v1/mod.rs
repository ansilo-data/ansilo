use axum::Router;

mod catalog;

pub(super) fn router() -> Router {
    Router::new().nest("/catalog", catalog::router())
}

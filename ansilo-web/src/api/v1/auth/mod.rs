use std::sync::Arc;

use axum::Router;

use crate::HttpApiState;

pub mod provider;

pub(super) fn router() -> Router<Arc<HttpApiState>> {
    Router::new().nest("/provider", provider::router())
}

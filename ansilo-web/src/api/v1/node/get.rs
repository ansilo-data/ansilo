use ansilo_core::{err::Result, web::node::*};
use axum::{extract::State, Json};
use hyper::StatusCode;

use crate::HttpApiState;

// Unauthenticated endpoint to retrieve high-level node metadata
pub(super) async fn handler(
    State(state): State<HttpApiState>,
) -> Result<Json<NodeInfo>, (StatusCode, &'static str)> {
    Ok(Json(NodeInfo {
        name: state.conf().name.clone(),
        version: state.version_info().version.clone(),
    }))
}

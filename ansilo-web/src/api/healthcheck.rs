use std::{collections::HashMap, sync::Arc};

use ansilo_logging::warn;
use ansilo_util_health::HealthStatus;
use axum::{extract::State, routing, Json, Router};
use hyper::StatusCode;
use serde::{Deserialize, Serialize};

use crate::HttpApiState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub subsystems: HashMap<String, HealthStatus>,
}

async fn handler(
    State(state): State<Arc<HttpApiState>>,
) -> Result<(StatusCode, Json<HealthCheck>), (StatusCode, &'static str)> {
    let subsystems = state.health().check().map_err(|e| {
        warn!("Failed to get health: {:?}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to get health. This is a bad sign.",
        )
    })?;

    let healthy = subsystems.values().all(|h| h.healthy);

    Ok((
        if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(HealthCheck { subsystems }),
    ))
}

pub(super) fn router() -> Router<Arc<HttpApiState>> {
    Router::new().route("/", routing::get(handler))
}

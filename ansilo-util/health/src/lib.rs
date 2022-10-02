use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, RwLock},
};

use ansilo_core::{
    data::chrono::{DateTime, Utc},
    err::{Error, Result},
};
use ansilo_logging::{info, warn};
use serde::{Deserialize, Serialize};

/// Stores the health status of each subsystem
#[derive(Clone)]
pub struct Health {
    /// Mapping of the subsytem name to the healthy status
    state: Arc<RwLock<HashMap<String, HealthStatus>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthStatus {
    /// Is the system healthy?
    pub healthy: bool,
    /// When was it last checked?
    pub checked: DateTime<Utc>,
    /// When was it last healthy?
    pub last_healthy: Option<DateTime<Utc>>,
}

impl Health {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns a copy of the health state
    pub fn check(&self) -> Result<HashMap<String, HealthStatus>> {
        Ok(self
            .state
            .read()
            .map_err(|_| Error::msg("Failed to lock health state"))?
            .clone())
    }

    /// Updates the health status of a system
    pub fn update(&self, subsystem: &str, healthy: bool) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| Error::msg("Failed to lock health state"))?;

        let now = Utc::now();

        match state.entry(subsystem.into()) {
            Entry::Occupied(mut s) => {
                let s = s.get_mut();

                match (s.healthy, healthy) {
                    (true, false) => warn!("Subsystem '{subsystem}' changed to unhealthy"),
                    (false, true) => info!("Subsystem '{subsystem}' changed to healthy"),
                    _ => {}
                }

                s.healthy = healthy;
                if healthy {
                    s.last_healthy = Some(now)
                }
            }
            Entry::Vacant(s) => {
                s.insert(HealthStatus {
                    healthy,
                    checked: now,
                    last_healthy: if healthy { Some(now) } else { None },
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let health = Health::new();

        assert_eq!(health.check().unwrap(), HashMap::new());

        health.update("sys", true).unwrap();
        health.update("other", false).unwrap();

        let sys = health.check().unwrap().get("sys").cloned().unwrap();
        assert_eq!(sys.healthy, true);
        assert_eq!(sys.last_healthy.is_some(), true);

        let other = health.check().unwrap().get("other").cloned().unwrap();
        assert_eq!(other.healthy, false);
        assert_eq!(other.last_healthy.is_some(), false);

        health.update("other", true).unwrap();

        let other = health.check().unwrap().get("other").cloned().unwrap();
        assert_eq!(other.last_healthy.is_some(), true);
    }
}

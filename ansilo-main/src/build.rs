use std::{
    fs,
    path::PathBuf,
    time::{self, UNIX_EPOCH},
};

use ansilo_core::err::{Context, Result};
use chrono::TimeZone;
use serde::{Deserialize, Serialize};

use crate::conf;

/// Captures information about the build
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildInfo {
    /// When the build occurred in unix timestamp millis
    ts: u64,
}

impl BuildInfo {
    pub fn new() -> Self {
        Self {
            ts: time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Invalid system time")
                .as_millis() as u64,
        }
    }

    /// When the build occurred
    pub fn built_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.timestamp_millis(self.ts as _)
    }

    /// Stores the build info at the path specified in the node config
    pub fn store(&self) -> Result<()> {
        let path = Self::path();
        let json = serde_json::to_vec(self).context("Failed to serialize build info")?;

        fs::write(path, json).context("Failed to write build info")?;

        Ok(())
    }

    /// Stores the build info at the path specified in the node config
    pub fn fetch() -> Result<Option<Self>> {
        let path = Self::path();

        if !path.exists() {
            return Ok(None);
        }

        let info = serde_json::from_slice(
            fs::read(path)
                .context("Failed to read build info file")?
                .as_slice(),
        )
        .context("Failed to deserialize build info file")?;

        Ok(Some(info))
    }

    fn path() -> PathBuf {
        conf()
            .postgres
            .clone()
            .unwrap_or_default()
            .build_info_path
            .unwrap_or("/etc/ansilo/build-info.json".into())
    }
}

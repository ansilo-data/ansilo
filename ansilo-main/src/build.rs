use std::{
    fs,
    path::PathBuf,
    time::{self, UNIX_EPOCH},
};

use ansilo_core::err::{Context, Result};
use ansilo_logging::info;
use ansilo_pg::PostgresInstance;
use chrono::TimeZone;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use crate::conf::*;

/// Initialises the postgres database
pub fn build(conf: &'static AppConf, handle: Handle) -> Result<PostgresInstance> {
    info!("Running build...");

    // Initialize postgres via initdb
    let mut postgres =
        PostgresInstance::configure(&conf.pg, handle).context("Failed to initialise postgres")?;

    // Connect to it
    let mut con = postgres
        .connections()
        .admin()
        .context("Failed to connect to postgres")?;

    // Run sql init scripts
    let init_sql_path = conf
        .node
        .postgres
        .clone()
        .unwrap_or_default()
        .init_sql_path
        .unwrap_or("/etc/ansilo/sql/*.sql".into());

    info!("Running scripts {}", init_sql_path.display());

    for script in glob::glob(init_sql_path.to_str().context("Invalid init sql path")?)
        .context("Failed to glob init sql path")?
    {
        let script = script.context("Failed to read sql file")?;

        info!("Running {}", script.display());
        let sql = fs::read_to_string(&script)
            .with_context(|| format!("Failed to read sql file: {}", script.display()))?;
        con.batch_execute(&sql)
            .with_context(|| format!("Failed to execute sql script: {}", script.display()))?;
    }

    BuildInfo::new().store(conf)?;
    info!("Build complete...");

    Ok(postgres)
}

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
    pub fn store(&self, conf: &AppConf) -> Result<()> {
        let path = Self::path(conf);
        let json = serde_json::to_vec(self).context("Failed to serialize build info")?;

        fs::write(path, json).context("Failed to write build info")?;

        Ok(())
    }

    /// Stores the build info at the path specified in the node config
    pub fn fetch(conf: &AppConf) -> Result<Option<Self>> {
        let path = Self::path(conf);

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

    fn path(conf: &AppConf) -> PathBuf {
        conf.node
            .postgres
            .clone()
            .unwrap_or_default()
            .build_info_path
            .unwrap_or("/var/run/ansilo/build-info.json".into())
    }
}

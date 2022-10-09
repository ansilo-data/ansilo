use std::{
    fs,
    path::PathBuf,
    time::{self, UNIX_EPOCH},
};

use ansilo_auth::Authenticator;
use ansilo_core::{
    build::ansilo_version,
    config::BuildStageMode,
    err::{Context, Result},
};
use ansilo_logging::info;
use ansilo_pg::{handler::PostgresConnectionHandler, PostgresInstance};
use ansilo_web::VersionInfo;
use chrono::TimeZone;
use serde::{Deserialize, Serialize};

use crate::conf::*;

/// Initialises the postgres database
pub async fn build(
    conf: &'static AppConf,
    auth: Authenticator,
) -> Result<(PostgresInstance, BuildInfo)> {
    info!("Building database (mode: buildtime)...");

    // Initialize postgres via initdb
    let mut postgres = PostgresInstance::configure(&conf.pg)
        .await
        .context("Failed to initialise postgres")?;

    let handler = PostgresConnectionHandler::new(auth, postgres.connections().clone());

    run_build_stages(conf, BuildStageMode::Build, &handler).await?;

    let build_info = BuildInfo::new();
    build_info.store(conf)?;
    info!("Build complete...");

    Ok((postgres, build_info))
}

/// Runs the runtime build scripts
pub async fn runtime_build(
    conf: &'static AppConf,
    handler: &PostgresConnectionHandler,
) -> Result<()> {
    info!("Building database (mode: runtime)...");

    run_build_stages(conf, BuildStageMode::Runtime, handler).await?;

    info!("Runtime build complete...");

    Ok(())
}

async fn run_build_stages(
    conf: &AppConf,
    mode: BuildStageMode,
    handler: &PostgresConnectionHandler,
) -> Result<()> {
    let stages = conf
        .node
        .build
        .stages
        .iter()
        .filter(|s| s.mode == mode)
        .collect::<Vec<_>>();

    if stages.is_empty() {
        return Ok(());
    }

    // Connect to postgres as the default admin user
    let admin_con = handler
        .pool()
        .admin()
        .await
        .context("Failed to connect to postgres")?;

    Ok(for (idx, stage) in stages.iter().enumerate() {
        info!(
            "Running build stage {}...",
            stage.name.as_ref().unwrap_or(&(idx + 1).to_string())
        );

        // If this stage is configured to run as a service user, authenticate
        // as that service user
        let service_user_con = if let Some(service_user_id) = stage.service_user.as_ref() {
            Some(
                handler
                    .authenticate_as_service_user(service_user_id.clone())
                    .await
                    .context("Failed to connect to postgres")?,
            )
        } else {
            None
        };

        // Get a reference to the appropriate connection for this stage
        let con = service_user_con.as_ref().unwrap_or(&admin_con);

        let init_sql_path = stage.sql.clone();
        info!("Running scripts {}", init_sql_path.display());

        for script in glob::glob(init_sql_path.to_str().context("Invalid init sql path")?)
            .context("Failed to glob init sql path")?
        {
            let script = script.context("Failed to read sql file")?;

            info!("Running {}", script.display());
            let sql = fs::read_to_string(&script)
                .with_context(|| format!("Failed to read sql file: {}", script.display()))?;
            con.batch_execute(&sql)
                .await
                .with_context(|| format!("Failed to execute sql script: {}", script.display()))?;
        }
    })
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

impl Into<VersionInfo> for &BuildInfo {
    fn into(self) -> VersionInfo {
        VersionInfo::new(ansilo_version(), self.built_at())
    }
}

use std::{collections::HashMap, os::raw::c_int, thread, time::Duration};

use crate::{
    args::Command,
    build::BuildInfo,
    handlers::{Http1ConnectionHandler, Http2ConnectionHandler, PostgresConnectionHandler},
};
use ansilo_connectors_all::{ConnectionPools, ConnectorEntityConfigs, Connectors};
use ansilo_core::err::{Context, Result};
use ansilo_logging::{info, warn};
use ansilo_pg::{fdw::server::FdwServer, PostgresInstance};
use ansilo_proxy::{conf::HandlerConf, server::ProxyServer};
use clap::Parser;
use nix::libc::SIGUSR1;
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};

pub mod args;
pub mod build;
pub mod conf;
pub mod dev;
pub mod handlers;

pub use ansilo_pg::fdw::log::RemoteQueryLog;

use build::*;
use conf::*;
use tokio::runtime::Runtime;

/// This struct represents a running instance of ansilo and its subsystems.
///
/// This is the entrypoint to build, start and manage the instance.
pub struct Ansilo {
    /// The command used to start the instance
    command: Command,
    /// The configuration used
    conf: &'static AppConf,
    /// Running subsystems
    subsystems: Option<Subsystems>,
    /// Remote query log
    log: RemoteQueryLog,
}

struct Subsystems {
    /// The tokio runtime
    runtime: Runtime,
    /// The postgres instance
    postgres: PostgresInstance,
    /// The FDW server
    fdw: FdwServer,
    /// The proxy server
    proxy: ProxyServer,
}

impl Ansilo {
    /// This is the entrypoint to booting Ansilo.
    /// Here, we start the initial launch sequence.
    pub fn main() {
        ansilo_logging::init_logging().unwrap();
        info!("Hi, thanks for using Ansilo!");

        Self::start(Command::parse(), None).unwrap().wait().unwrap();
    }

    /// Runs the supplied command
    pub fn start(command: Command, log: Option<RemoteQueryLog>) -> Result<Self> {
        let args = command.args();
        let log = log.unwrap_or_default();

        // Load configuration
        let config_path = args.config.clone().unwrap_or("/etc/ansilo/main.yml".into());
        // We are happy to let the app-wide config leak for the rest of the program
        let conf: &'static _ = Box::leak(Box::new(init_conf(&config_path)));

        let pools = Self::init_connectors(conf);

        info!("Starting fdw listener...");
        let fdw = FdwServer::start(
            &conf.node,
            conf.pg.fdw_socket_path.clone(),
            pools,
            log.clone(),
        )
        .context("Failed to start fdw server")?;

        let mut postgres = if let (Command::Run(_), false, Some(build_info)) =
            (&command, args.force_build, BuildInfo::fetch(conf)?)
        {
            info!("Build occurred at {}", build_info.built_at().to_rfc3339());
            info!("Starting postgres...");
            PostgresInstance::start(&conf.pg)?
        } else {
            build(conf)?
        };

        if command.is_build() {
            info!("Build complete...");
            return Ok(Self {
                command,
                conf,
                subsystems: None,
                log,
            });
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("ansilo-tokio-worker")
            .enable_all()
            .build()
            .context("Failed to create tokio runtime")?;

        info!("Starting proxy server...");
        let proxy_conf = Box::leak(Box::new(init_proxy_conf(
            conf,
            HandlerConf::new(
                PostgresConnectionHandler::new(conf, postgres.connections().clone()),
                Http2ConnectionHandler::new(postgres.connections().clone()),
                Http1ConnectionHandler::new(postgres.connections().clone()),
            ),
        )));

        let mut proxy = ProxyServer::new(proxy_conf);
        runtime
            .block_on(proxy.start())
            .context("Failed to start proxy server")?;

        info!("Start up complete...");
        Ok(Self {
            command,
            conf,
            subsystems: Some(Subsystems {
                runtime,
                postgres,
                fdw,
                proxy,
            }),
            log,
        })
    }

    /// Gets the app config
    pub fn conf(&self) -> &AppConf {
        &self.conf
    }

    /// Gets the remote query log
    pub fn log(&self) -> &RemoteQueryLog {
        &self.log
    }

    /// Waits for instance to terminate
    pub fn wait(mut self) -> Result<()> {
        if self.command.is_build() {
            return Ok(());
        }

        if self.command.is_dev() {
            thread::spawn(|| {
                dev::signal_on_config_update(self.conf);
            });
        }

        let mut sigs =
            Signals::new(&[SIGINT, SIGTERM, SIGUSR1]).context("Failed to attach signal handler")?;
        let sig = sigs.forever().next().unwrap();

        // TODO: better handling if critical threads fail
        info!(
            "Received {}",
            match sig {
                SIGINT => "SIGINT".into(),
                SIGTERM => "SIGTERM".into(),
                SIGUSR1 => "SIGUSR1".into(),
                _ => format!("unkown signal {}", sig),
            }
        );

        self.terminate_mut(Some(sig))?;

        Ok(())
    }

    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut(None)
    }

    fn terminate_mut(&mut self, sig: Option<c_int>) -> Result<()> {
        let subsystems = match self.subsystems.take() {
            Some(s) => s,
            None => return Ok(()),
        };

        info!("Terminating...");
        if let Err(err) = subsystems.proxy.terminate() {
            warn!("Failed to terminate proxy server: {:?}", err);
        }
        if let Err(err) = subsystems.postgres.terminate() {
            warn!("Failed to terminate postgres: {:?}", err);
        }
        if let Err(err) = subsystems.fdw.terminate() {
            warn!("Failed to terminate fdw server: {:?}", err);
        }

        subsystems.runtime.shutdown_timeout(Duration::from_secs(3));

        info!("Shutdown sequence complete");

        // If we are running in dev-mode, restart the process
        if self.command.is_dev() && sig == Some(SIGUSR1) {
            dev::restart();
        }

        Ok(())
    }

    fn init_connectors(
        conf: &'static AppConf,
    ) -> HashMap<String, (ConnectionPools, ConnectorEntityConfigs)> {
        info!("Initializing connectors...");
        let pools = conf
            .node
            .sources
            .iter()
            .map(|i| {
                info!("Initializing connector: {}", i.id);
                let connector = Connectors::from_type(&i.r#type)
                    .with_context(|| format!("Unknown connector type: {}", i.r#type))
                    .unwrap();
                let options = connector
                    .parse_options(i.options.clone())
                    .context("Failed to parse options")
                    .unwrap();

                let pool = connector
                    .create_connection_pool(&conf.node, &i.id, options)
                    .context("Failed to create connection pool")
                    .unwrap();

                (i.id.clone(), pool)
            })
            .collect();
        pools
    }
}

impl Drop for Ansilo {
    fn drop(&mut self) {
        if let Err(err) = self.terminate_mut(None) {
            warn!("Error occurred while shutting down: {:?}", err);
        }
    }
}

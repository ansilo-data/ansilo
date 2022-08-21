use std::{collections::HashMap, thread};

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

use build::*;
use conf::*;

/// This is the entrypoint to booting Ansilo.
/// Here, we initial launch sequence.
pub fn main() {
    ansilo_logging::init_logging().unwrap();
    info!("Hi, thanks for using Ansilo!");

    run(Command::parse()).unwrap();
}

/// Runs the supplied command
pub fn run(command: Command) -> Result<()> {
    let args = command.args();

    // Load configuration
    let config_path = args.config.clone().unwrap_or("/etc/ansilo/main.yml".into());
    init_conf(&config_path);

    let pools = init_connectors();

    info!("Starting fdw listener...");
    let fdw_server = FdwServer::start(conf(), pg_conf().fdw_socket_path.clone(), pools)
        .context("Failed to start fdw server")?;

    let mut postgres = if let (Command::Run(_), Some(build_info)) = (&command, BuildInfo::fetch()?)
    {
        info!("Build occurred at {}", build_info.built_at().to_rfc3339());
        info!("Starting postgres...");
        PostgresInstance::start(&pg_conf())?
    } else {
        build()?
    };

    if command.is_build() {
        info!("Build complete...");
        return Ok(());
    }

    info!("Starting proxy server...");
    let conf = init_proxy_conf(HandlerConf::new(
        PostgresConnectionHandler::new(postgres.connections().clone()),
        Http2ConnectionHandler::new(postgres.connections().clone()),
        Http1ConnectionHandler::new(postgres.connections().clone()),
    ));

    let mut proxy = ProxyServer::new(conf);
    proxy.start().context("Failed to start proxy server")?;

    info!("Start up complete...");

    if command.is_dev() {
        thread::spawn(|| {
            dev::signal_on_config_update();
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

    info!("Terminating...");
    if let Err(err) = proxy.terminate() {
        warn!("Failed to terminate proxy server: {}", err);
    }
    if let Err(err) = postgres.terminate() {
        warn!("Failed to terminate postgres: {}", err);
    }
    if let Err(err) = fdw_server.terminate() {
        warn!("Failed to terminate fdw server: {}", err);
    }

    info!("Shutdown sequence complete");

    // If we are running in dev-mode, restart the process
    if command.is_dev() && sig == SIGUSR1 {
        dev::restart();
    }

    Ok(())
}

fn init_connectors() -> HashMap<String, (ConnectionPools, ConnectorEntityConfigs)> {
    info!("Initializing connectors...");
    let pools = conf()
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
                .create_connection_pool(conf(), &i.id, options)
                .context("Failed to create connection pool")
                .unwrap();

            (i.id.clone(), pool)
        })
        .collect();
    pools
}

use std::fs;

use crate::{args::Command, build::BuildInfo};
use ansilo_config::loader::ConfigLoader;
use ansilo_connectors_all::Connectors;
use ansilo_core::{
    config::NodeConfig,
    err::{Context, Result},
};
use ansilo_logging::info;
use ansilo_pg::{conf::PostgresConf, fdw::server::FdwServer, PostgresInstance};
use clap::Parser;
use once_cell::sync::OnceCell;
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};

mod args;
pub mod build;

/// We store our node configuration in a global static variable
static NODE_CONFIG: OnceCell<NodeConfig> = OnceCell::new();

pub fn conf() -> &'static NodeConfig {
    NODE_CONFIG
        .get()
        .expect("Tried to retrieve node config before initialised")
}

/// This is the entrypoint to booting Ansilo.
/// Here, we initial launch sequence.
fn main() {
    ansilo_logging::init_logging().unwrap();
    info!("Hi, thanks for using Ansilo!");

    // Parse arguments
    let command = Command::parse();
    let args = command.args();

    // Load configuration
    info!("Loading configuration...");
    NODE_CONFIG.get_or_init(|| {
        let config_path = args.config.clone().unwrap_or("/etc/ansilo/main.yml".into());
        let config_loader = ConfigLoader::new();

        config_loader
            .load(&config_path)
            .context("Failed to load configuration")
            .unwrap()
    });

    run(command).unwrap()
}

/// Runs postgres and the fdw server
fn run(command: Command) -> Result<()> {
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

    info!("Starting fdw listener...");
    let fdw_server = FdwServer::start(conf(), pg_conf().fdw_socket_path.clone(), pools)
        .context("Failed to start fdw server")?;

    let postgres = if let (Command::Run(_), Some(build_info)) = (&command, BuildInfo::fetch()?) {
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

    // TODO: Create postgres proxy

    info!("Start up complete...");

    // TODO: dev mode to restart on file change
    // Now we wait for any signals on the main thread that
    // indicates we should terminate
    let mut sigs = Signals::new(&[SIGINT, SIGTERM]).context("Failed to attach signal handler")?;
    for sig in sigs.forever() {
        info!(
            "Received {}",
            match sig {
                SIGINT => "SIGINT".into(),
                SIGTERM => "SIGTERM".into(),
                _ => format!("unkown signal {}", sig),
            }
        );
        break;
    }

    info!("Terminating...");
    postgres
        .terminate()
        .context("Failed to terminate postgres")?;
    fdw_server
        .terminate()
        .context("Failed to terminate fdw server")?;

    info!("Graceful shutdown complete");
    Ok(())
}

/// Initialises the postgres database
fn build() -> Result<PostgresInstance> {
    info!("Running build...");
    let conf = conf();
    let pg_conf = pg_conf();

    let mut postgres =
        PostgresInstance::configure(&pg_conf).context("Failed to initialise postgres")?;
    let mut con = postgres
        .connections()
        .admin()
        .context("Failed to connect to postgres")?;

    let init_sql_path = conf
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
        let sql = fs::read_to_string(script).context("Failed to read sql file")?;
        con.batch_execute(&sql).context("Failed to execute sql")?;
    }

    BuildInfo::new().store()?;
    info!("Build complete...");

    Ok(postgres)
}

fn pg_conf() -> PostgresConf {
    let pg_conf = conf().postgres.clone().unwrap_or_default();

    PostgresConf {
        install_dir: pg_conf
            .install_dir
            .unwrap_or("/usr/lib/postgresql/14/".into()),
        postgres_conf_path: Some(
            pg_conf
                .config_path
                .unwrap_or("/etc/postgresql/14/main/postgresql.conf".into()),
        ),
        data_dir: pg_conf
            .data_dir
            .unwrap_or("/var/run/postgresql/ansilo/data".into()),
        socket_dir_path: pg_conf
            .listen_socket_dir_path
            .unwrap_or("/var/run/postgresql/ansilo/".into()),
        fdw_socket_path: pg_conf
            .fdw_socket_path
            .unwrap_or("/var/run/postgresql/ansilo/fdw.sock".into()),
    }
}

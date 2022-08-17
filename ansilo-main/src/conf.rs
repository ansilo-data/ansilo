use std::path::{Path, PathBuf};

use ansilo_config::loader::ConfigLoader;
use ansilo_core::{config::NodeConfig, err::Context};
use ansilo_logging::info;
use ansilo_pg::conf::PostgresConf;
use once_cell::sync::OnceCell;

/// We store our node configuration in a global static variable
static NODE_CONFIG: OnceCell<NodeConfig> = OnceCell::new();
static NODE_CONFIG_PATH: OnceCell<PathBuf> = OnceCell::new();

/// Initialises the node global config state
pub fn init_conf(config_path: &Path) {
    info!("Loading configuration...");
    NODE_CONFIG.get_or_init(|| {
        let config_loader = ConfigLoader::new();

        config_loader
            .load(&config_path)
            .context("Failed to load configuration")
            .unwrap()
    });

    NODE_CONFIG_PATH.get_or_init(|| config_path.to_path_buf());
}

/// Gets the global node configuration
pub fn conf() -> &'static NodeConfig {
    NODE_CONFIG
        .get()
        .expect("Tried to retrieve node config before initialised")
}

/// Gets the global node config path
pub fn conf_path() -> &'static Path {
    NODE_CONFIG_PATH
        .get()
        .expect("Tried to retrieve node config path before initialised")
}

/// Gets the postgres configuration for this instance
pub fn pg_conf() -> PostgresConf {
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

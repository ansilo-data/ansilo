use std::{
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
};

use ansilo_config::loader::ConfigLoader;
use ansilo_core::{config::NodeConfig, err::Context};
use ansilo_logging::info;
use ansilo_pg::conf::PostgresConf;
use ansilo_proxy::conf::{HandlerConf, ProxyConf, TlsConf};
use once_cell::sync::OnceCell;

/// Container for the application config
pub struct AppConf {
    /// Node configuration from main config file
    pub node: NodeConfig,
    /// Path to config file
    pub path: PathBuf,
    /// Postgres configuration
    pub pg: PostgresConf,
}

/// Initialises the node global config state
pub fn init_conf(config_path: &Path) -> AppConf {
    info!("Loading configuration...");
    let config_loader = ConfigLoader::new();

    let node = config_loader
        .load(&config_path)
        .context("Failed to load configuration")
        .unwrap();

    let pg = pg_conf(&node);

    AppConf {
        node,
        path: config_path.into(),
        pg,
    }
}

/// Gets the postgres configuration for this instance
fn pg_conf(node: &NodeConfig) -> PostgresConf {
    let pg_conf = node.postgres.clone().unwrap_or_default();

    PostgresConf {
        install_dir: pg_conf
            .install_dir
            .unwrap_or("/usr/lib/postgresql/14/".into()),
        postgres_conf_path: pg_conf.config_path,
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

/// Initialises the proxy configuration
pub fn init_proxy_conf(conf: &AppConf, handlers: HandlerConf) -> ProxyConf {
    let networking = conf.node.networking.clone();

    ProxyConf {
        addrs: vec![(
            networking.bind.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            networking.port,
        )
            .into()],
        tls: networking.tls.map(|i| {
            TlsConf::new(&i.private_key, &i.certificate)
                .context("Failed to parse TLS configuration options")
                .unwrap()
        }),
        handlers,
    }
}

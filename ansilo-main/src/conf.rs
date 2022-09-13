use std::{
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
};

use ansilo_config::loader::ConfigLoader;
use ansilo_core::{config::NodeConfig, err::Context};
use ansilo_logging::info;
use ansilo_pg::{conf::PostgresConf, PG_ADMIN_USER};
use ansilo_proxy::conf::{HandlerConf, ProxyConf, TlsConf};
use ansilo_util_pg::query::{pg_quote_identifier, pg_str_literal};

use crate::args::Args;

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
pub fn init_conf(config_path: &Path, args: &Args) -> AppConf {
    info!("Loading configuration...");
    let config_loader = ConfigLoader::new();

    let node = config_loader
        .load(&config_path, args.config_args.iter().cloned().collect())
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
        //
        postgres_conf_path: pg_conf.config_path,
        //
        data_dir: pg_conf
            .data_dir
            .unwrap_or("/var/run/postgresql/ansilo/data".into()),
        //
        socket_dir_path: pg_conf
            .listen_socket_dir_path
            .unwrap_or("/var/run/postgresql/ansilo/".into()),
        //
        fdw_socket_path: pg_conf
            .fdw_socket_path
            .unwrap_or("/var/run/postgresql/ansilo/fdw.sock".into()),
        //
        app_users: node
            .auth
            .users
            .iter()
            .map(|i| i.username.clone())
            .collect::<Vec<_>>(),
        //
        init_db_sql: create_db_init_sql(node),
    }
}

fn create_db_init_sql(node: &NodeConfig) -> Vec<String> {
    // Run CREATE SERVER for each data source
    node.sources
        .iter()
        .map(|source| {
            let name = pg_quote_identifier(&source.id);
            let id = pg_str_literal(&source.id);
            format!(
                r#"
                CREATE SERVER {name}
                FOREIGN DATA WRAPPER ansilo_fdw
                OPTIONS (
                    data_source {id}
                );
                
                GRANT ALL ON FOREIGN SERVER {name} TO {PG_ADMIN_USER};
            "#
            )
        })
        .collect()
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

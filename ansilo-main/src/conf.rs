use std::{
    env,
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
    process::{Command, Stdio},
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
    ansilo_logging::error!("{:?}", create_db_init_sql(node));

    PostgresConf {
        install_dir: pg_conf
            .install_dir
            .or_else(|| env::var("ANSILO_PG_INSTALL_DIR").ok().map(PathBuf::from))
            .or_else(|| try_get_pg_install_dir())
            .unwrap_or("/usr/pgsql-14/".into()),
        //
        postgres_conf_path: pg_conf.config_path,
        //
        data_dir: pg_conf.data_dir.unwrap_or("/var/run/ansilo/data".into()),
        //
        socket_dir_path: pg_conf
            .listen_socket_dir_path
            .unwrap_or("/var/run/ansilo/".into()),
        //
        fdw_socket_path: pg_conf
            .fdw_socket_path
            .unwrap_or("/var/run/ansilo/fdw.sock".into()),
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

fn try_get_pg_install_dir() -> Option<PathBuf> {
    let output = Command::new("pg_config")
        .arg("--bindir")
        .stdin(Stdio::null())
        .stderr(Stdio::inherit())
        .stdout(Stdio::piped())
        .spawn()
        .ok()?
        .wait_with_output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let path = String::from_utf8(output.stdout).ok()?;

    let path: PathBuf = path.trim().parse().ok()?;

    Some(path.parent()?.to_path_buf())
}

fn create_db_init_sql(node: &NodeConfig) -> Vec<String> {
    [
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
            .collect::<Vec<_>>(),
        // Add descriptions of users
        node.auth
            .users
            .iter()
            .filter(|user| user.description.is_some())
            .map(|user| {
                let username = pg_quote_identifier(&user.username);
                let description = pg_str_literal(user.description.as_ref().unwrap());

                format!(
                    r#"
                    COMMENT ON ROLE {username} IS {description};
                "#
                )
            })
            .collect::<Vec<_>>(),
    ]
    .concat()
}

/// Initialises the proxy configuration
pub fn init_proxy_conf(conf: &AppConf, handlers: HandlerConf) -> ProxyConf {
    let networking = conf.node.networking.clone();

    ProxyConf {
        addrs: vec![(
            networking
                .bind
                .unwrap_or(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))),
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

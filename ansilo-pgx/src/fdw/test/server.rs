use std::{fs, os::unix::net::UnixStream, path::PathBuf, thread, time::Duration};

use ansilo_connectors_all::ConnectionPools;
use ansilo_core::config::NodeConfig;
use ansilo_pg::fdw::server::FdwServer;
use lazy_static::lazy_static;

lazy_static! {
    static ref NODE_CONFIG: NodeConfig = NodeConfig::default();
}

pub(crate) fn start_fdw_server(pool: ConnectionPools, socket_path: impl Into<String>) {
    let path = PathBuf::from(socket_path.into());
    fs::create_dir_all(path.parent().unwrap().clone()).unwrap();

    let server = FdwServer::start(
        &NODE_CONFIG,
        path.clone(),
        [("memory".to_string(), pool)].into_iter().collect(),
    )
    .unwrap();

    loop {
        if UnixStream::connect(&path).is_ok() {
            break;
        }

        thread::sleep(Duration::from_millis(10));
    }

    // Don't drop the server or it will terminate
    Box::leak(Box::new(server));
}

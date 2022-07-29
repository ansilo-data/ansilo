use std::{fs, path::PathBuf, thread, time::Duration};

use ansilo_connectors::interface::container::ConnectionPools;
use ansilo_pg::fdw::server::FdwServer;

pub(crate) fn start_fdw_server(pool: ConnectionPools, socket_path: impl Into<String>) -> FdwServer {
    let path = PathBuf::from(socket_path.into());
    fs::create_dir_all(path.parent().unwrap().clone()).unwrap();

    let server =
        FdwServer::start(path, [("memory".to_string(), pool)].into_iter().collect()).unwrap();
    thread::sleep(Duration::from_millis(10));

    server
}

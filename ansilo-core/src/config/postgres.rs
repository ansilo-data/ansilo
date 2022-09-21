use std::path::PathBuf;

use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// Postgres install directory
    pub install_dir: Option<PathBuf>,
    /// The postgres configuration file
    pub config_path: Option<PathBuf>,
    /// The postgres data directory
    pub data_dir: Option<PathBuf>,
    /// The directory of the unix socket postgres listens on for connections
    /// The full path is in the format {dir}/.s.PGSQL.{port}
    pub listen_socket_dir_path: Option<PathBuf>,
    /// Path to the unix socket which ansilo listens on
    /// acting as the data source for the FDW
    pub fdw_socket_path: Option<PathBuf>,
    /// The path used to mark the postgres instance as initialised
    pub build_info_path: Option<PathBuf>,
}

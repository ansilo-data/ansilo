use std::path::PathBuf;

/// Configuration of the postgres installation
#[derive(Debug, Clone, PartialEq)]
pub struct PostgresConf {
    /// The install directory
    pub install_dir: PathBuf,
    /// The postgres configuration file
    pub postgres_conf_path: Option<PathBuf>,
    /// The postgres data directory
    pub data_dir: PathBuf,
    /// The superuser username
    pub superuser: String,
    /// The directory of the unix socket postgres listens on for connections
    /// The full path is in the format {dir}/.s.PGSQL.{port}
    pub socket_dir_path: PathBuf,
    /// The port postgres SQL listens on for connections
    /// NOTE: we disable listening over TCP/IP and only connect via unix sockets
    /// This is purely used to have a stable unix socket path for connecting
    pub port: u16,
    /// Path to the unix socket which ansilo listens on
    /// acting as the data source for the FDW
    pub fdw_socket_path: PathBuf,
}

impl PostgresConf {
    /// Gets the full path of the postgres unix socket
    pub fn pg_socket_path(&self) -> PathBuf {
        self.socket_dir_path
            .join(format!(".s.PGSQL.{}", self.port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_conf_socket_path() {
        let conf = PostgresConf {
            install_dir: PathBuf::from("/"),
            postgres_conf_path: None,
            data_dir: PathBuf::from("/"),
            socket_dir_path: PathBuf::from("/var/run/pg/"),
            port: 65432,
            fdw_socket_path: PathBuf::from("/"),
            superuser: "test".to_string(),
        };

        assert_eq!(
            conf.pg_socket_path(),
            PathBuf::from("/var/run/pg/.s.PGSQL.65432")
        )
    }
}

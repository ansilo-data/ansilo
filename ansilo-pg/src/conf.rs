use std::path::PathBuf;

use crate::PG_PORT;

/// Configuration of the postgres installation
#[derive(Debug, Clone, PartialEq)]
pub struct PostgresConf {
    /// The install directory
    pub install_dir: PathBuf,
    /// The postgres configuration file
    pub postgres_conf_path: Option<PathBuf>,
    /// The postgres data directory
    pub data_dir: PathBuf,
    /// The directory of the unix socket postgres listens on for connections
    /// The full path is in the format {dir}/.s.PGSQL.{port}
    pub socket_dir_path: PathBuf,
    /// Path to the unix socket which ansilo listens on
    /// acting as the data source for the FDW
    pub fdw_socket_path: PathBuf,
    /// Applicaton users which have been configured to authenticate as.
    pub app_users: Vec<String>,
    /// Additional queries to run on database initialisation
    /// Used to bootstrap any initial configuration
    pub init_db_sql: Vec<String>,
}

impl PostgresConf {
    /// Gets the full path of the postgres unix socket
    pub fn pg_socket_path(&self) -> PathBuf {
        self.socket_dir_path.join(format!(".s.PGSQL.{}", PG_PORT))
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
            fdw_socket_path: PathBuf::from("/"),
            app_users: vec![],
            init_db_sql: vec![],
        };

        assert_eq!(
            conf.pg_socket_path(),
            PathBuf::from("/var/run/pg/.s.PGSQL.5432")
        )
    }
}

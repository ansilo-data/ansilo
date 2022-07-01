use std::{process::{Command, ExitStatus}, time::Duration};

use ansilo_core::err::Result;
use ansilo_logging::info;
use nix::sys::signal::Signal;

use crate::{conf::PostgresConf, proc::ChildProc};

/// initdb creates a new postgres data director
#[derive(Debug)]
pub struct PostgresInitDb {
    /// The configuration used to init the database
    pub conf: PostgresConf,
    /// The child postgres process
    pub proc: ChildProc,
}

impl PostgresInitDb {
    /// Runs the initdb process
    pub fn init(conf: PostgresConf) -> Result<Self> {
        info!("Running initdb...");
        let mut cmd = Command::new(conf.install_dir.join("bin/initdb"));
        cmd.arg("-D")
            .arg(conf.data_dir.as_os_str())
            .arg("--encoding=UTF8")
            .arg("-U")
            .arg(conf.superuser.clone());

        Ok(Self {
            conf: conf.clone(),
            proc: ChildProc::new("initdb", Signal::SIGINT, Duration::from_secs(1), cmd)?,
        })
    }

    /// Waits for the process to exit and streams any stdout/stderr to the logs
    pub fn wait(&mut self) -> Result<ExitStatus> {
        self.proc.wait()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn test_pg_config() -> PostgresConf {
        PostgresConf {
            install_dir: PathBuf::from("/usr/lib/postgresql/14"),
            postgres_conf_path: PathBuf::from("not-used"),
            data_dir: PathBuf::from("/tmp/ansilo-pg-test-data/"),
            socket_dir_path: PathBuf::from("/tmp/"),
            port: 65432,
            fdw_socket_path: PathBuf::from("not-used"),
            superuser: "pgsuper".to_string(),
        }
    }

    // #[test]
    // fn test_initdb() {
    //         ansilo_logging::init();
    //         let mut server = PostgresServer::boot(test_pg_config()).unwrap();

    //         thread::sleep(Duration::from_millis(100));
    //         // assert still running
    //         assert_eq!(server.proc.try_wait().unwrap(), None);
    //         // assert listening on expected socket path
    //         assert!(server.conf.pg_socket_path().exists());
    //     }
}

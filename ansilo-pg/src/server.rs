use std::{
    process::{Command, ExitStatus},
    time::Duration,
};

use ansilo_core::err::{Context, Result};
use ansilo_logging::info;
use nix::sys::signal::Signal;

use crate::{conf::PostgresConf, proc::ChildProc};

/// An instance of postgres run as an ephemeral server
pub(crate) struct PostgresServer {
    /// The configuration used to start the server
    pub conf: PostgresConf,
    /// The child postgres process
    pub proc: ChildProc,
}

impl PostgresServer {
    /// Boots a postgres server instance
    pub fn boot(conf: PostgresConf) -> Result<Self> {
        info!("Booting postgres...");
        let mut cmd = Command::new(conf.install_dir.join("bin/postgres"));
        cmd.arg("-D")
            .arg(conf.data_dir.as_os_str())
            .arg("-c")
            .arg("listen_addresses=")
            .arg("-c")
            .arg(format!("port={}", conf.port))
            .arg("-c")
            .arg(format!(
                "unix_socket_directories={}",
                conf.socket_dir_path
                    .to_str()
                    .context("Failed to parse pg_socket_dir_path as utf8")?
            ))
            .env("ANSILO_PG_FDW_SOCKET_PATH", conf.fdw_socket_path.clone());

        let proc = ChildProc::new("[postgres]", Signal::SIGINT, Duration::from_secs(3), cmd)
            .context("Failed to start postgres server process")?;

        Ok(Self {
            conf: conf.clone(),
            proc,
        })
    }

    /// Waits for the process to exit and streams any stdout/stderr to the logs
    pub fn wait(&mut self) -> Result<ExitStatus> {
        self.proc.wait()
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, thread};

    use nix::{sys::signal::kill, unistd::Pid};

    use crate::initdb::PostgresInitDb;

    use super::*;

    fn test_pg_config() -> PostgresConf {
        PostgresConf {
            install_dir: PathBuf::from("/usr/lib/postgresql/14"),
            postgres_conf_path: None,
            data_dir: PathBuf::from("/tmp/ansilo-tests/pg-server"),
            socket_dir_path: PathBuf::from("/tmp/ansilo-tests/pg-server"),
            port: 65432,
            fdw_socket_path: PathBuf::from("not-used"),
            superuser: "pgsuper".to_string(),
        }
    }

    #[test]
    fn test_postgres_server_boot() {
        ansilo_logging::init_for_tests();
        let conf = test_pg_config();
        PostgresInitDb::reset(&conf).unwrap();
        PostgresInitDb::run(conf.clone()).unwrap().complete().unwrap();
        let mut server = PostgresServer::boot(conf.clone()).unwrap();
        let pid = server.proc.pid();

        let server_thread = thread::spawn(move || server.wait());
        thread::sleep(Duration::from_millis(100));

        // assert listening on expected socket path
        assert!(conf.pg_socket_path().exists());

        kill(Pid::from_raw(pid as _), Signal::SIGINT).unwrap();
        assert!(server_thread.join().unwrap().unwrap().success())
    }
}

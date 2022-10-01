use std::{
    process::{Command, ExitStatus},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use ansilo_core::err::{bail, Context, Error, Result};
use ansilo_logging::info;
use nix::sys::signal::Signal;

use crate::{conf::PostgresConf, proc::ChildProc, PG_PORT};

/// An instance of postgres run as an ephemeral server
pub(crate) struct PostgresServer {
    /// The pid of the postgres instance
    pub pid: u32,
    /// The thread waiting on the process
    pub thread: JoinHandle<Result<ExitStatus>>,
    /// Whether the db is ready to accept connections
    pub ready: Arc<AtomicBool>,
}

impl PostgresServer {
    /// Boots a postgres server instance
    pub fn boot(conf: &'static PostgresConf) -> Result<Self> {
        info!("Booting postgres...");
        let mut cmd = Command::new(conf.install_dir.join("bin/postgres"));
        cmd.arg("-D")
            .arg(conf.data_dir.as_os_str())
            .args(["-c", "listen_addresses="])
            .args(["-c".into(), format!("port={}", PG_PORT)])
            .args([
                "-c".into(),
                format!(
                    "data_directory={}",
                    conf.data_dir.to_str().context("Failed to parse data_dir")?
                ),
            ])
            .args([
                "-c".into(),
                format!(
                    "unix_socket_directories={}",
                    conf.socket_dir_path
                        .to_str()
                        .context("Failed to parse socket_dir_path as utf8")?
                ),
            ])
            .args(["-c", "log_destination=stderr"])
            .args(["-c", "logging_collector=off"])
            // Minimal logging prefix as our own env_logger add it's own metadata
            .args(["-c", "log_line_prefix=[%p] "])
            .args([
                "-c".into(),
                format!("log_min_messages={}", Self::get_log_level()),
            ])
            .env("ANSILO_PG_FDW_SOCKET_PATH", conf.fdw_socket_path.clone());

        let mut proc = ChildProc::new("[postgres]", Signal::SIGINT, Duration::from_secs(3), cmd)
            .context("Failed to start postgres server process")?;
        let output = proc.subscribe()?;
        let ready = Arc::new(AtomicBool::new(false));

        let pid = proc.pid();
        let thread = thread::spawn(move || proc.wait());

        Self::wait_for_ready(output, Arc::clone(&ready));

        Ok(Self { pid, thread, ready })
    }

    fn wait_for_ready(output: Receiver<String>, ready: Arc<AtomicBool>) {
        thread::spawn(move || {
            while let Ok(log) = output.recv() {
                if log.contains("ready to accept connections") {
                    ready.store(true, Ordering::SeqCst);
                    break;
                }
            }
        });
    }

    /// Sets the postgres minimum logging level based on
    /// the logging level from rust.
    ///
    /// @see https://www.postgresql.org/docs/current/runtime-config-logging.html#RUNTIME-CONFIG-SEVERITY-LEVELS
    fn get_log_level() -> &'static str {
        if ansilo_logging::test_mode() {
            return "DEBUG1";
        }

        match ansilo_logging::max_level() {
            ansilo_logging::LevelFilter::Off => "PANIC",
            ansilo_logging::LevelFilter::Error => "ERROR",
            ansilo_logging::LevelFilter::Warn => "WARNING",
            ansilo_logging::LevelFilter::Info => "INFO",
            ansilo_logging::LevelFilter::Debug => "DEBUG1",
            ansilo_logging::LevelFilter::Trace => "DEBUG5",
        }
    }

    /// Waits until postgres is running and listening for connections
    #[allow(unused)]
    pub fn block_until_ready(&self, timeout: Duration) -> Result<()> {
        self.block_until_ready_opts(timeout, || false)
    }

    /// Waits until postgres is running and listening for connections
    pub fn block_until_ready_opts(
        &self,
        timeout: Duration,
        terminate_cb: impl Fn() -> bool,
    ) -> Result<()> {
        let mut tries = timeout.as_millis() as u64 / 10;

        loop {
            if tries <= 0 {
                bail!("Timedout while waiting for postgres to start up");
            }

            if terminate_cb() {
                bail!("Termination requested")
            }

            if self.is_ready() {
                info!("Postgres is listening for connections");
                return Ok(());
            }

            thread::sleep(Duration::from_millis(10));
            tries -= 1;
        }
    }

    /// Checks whether the instance is ready
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Waits for the process to exit and streams any stdout/stderr to the logs
    pub fn wait(self) -> Result<ExitStatus> {
        self.thread
            .join()
            .map_err(|_| Error::msg("Failed to join pg thread"))?
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nix::{sys::signal::kill, unistd::Pid};

    use crate::initdb::PostgresInitDb;

    use super::*;

    fn test_pg_config() -> &'static PostgresConf {
        let conf = PostgresConf {
            install_dir: PathBuf::from(
                std::env::var("ANSILO_TEST_PG_DIR").unwrap_or("/usr/lib/postgresql/14".into()),
            ),
            postgres_conf_path: None,
            data_dir: PathBuf::from("/tmp/ansilo-tests/pg-server/data"),
            socket_dir_path: PathBuf::from("/tmp/ansilo-tests/pg-server"),
            fdw_socket_path: PathBuf::from("not-used"),
            app_users: vec![],
            init_db_sql: vec![],
        };
        Box::leak(Box::new(conf))
    }

    #[test]
    fn test_postgres_server_boot() {
        ansilo_logging::init_for_tests();
        let conf = test_pg_config();
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();
        let server = PostgresServer::boot(conf).unwrap();
        let pid = server.pid;

        assert_eq!(server.is_ready(), false);
        server.block_until_ready(Duration::from_secs(5)).unwrap();
        assert_eq!(server.is_ready(), true);

        // assert listening on expected socket path
        assert!(conf.pg_socket_path().exists());

        kill(Pid::from_raw(pid as _), Signal::SIGINT).unwrap();
        assert!(server.wait().unwrap().success());
    }
}

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use ansilo_core::err::{bail, Context, Result, Error};
use ansilo_logging::{error, info};
use nix::{sys::signal::Signal, unistd::Pid};

use crate::{conf::PostgresConf, server::PostgresServer};

/// Supervises the postgres process and restarts if it crashes
#[derive(Debug)]
pub(crate) struct PostgresServerManager {
    /// The thread performing the supervision
    thread: Option<JoinHandle<Result<()>>>,
    /// Shared state across with supervisor thread
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    /// Current pid of the running postgres process
    pid: AtomicU32,
    /// Should terminate?
    terminate: AtomicBool,
}

impl PostgresServerManager {
    pub fn new(conf: PostgresConf) -> Self {
        let state = Arc::new(State {
            pid: AtomicU32::new(0),
            terminate: AtomicBool::new(false),
        });
        let thread = {
            let state = state.clone();
            thread::spawn(move || Self::supervise(conf, state))
        };

        Self {
            thread: Some(thread),
            state,
        }
    }

    fn supervise(conf: PostgresConf, state: Arc<State>) -> Result<()> {
        while !state.terminate.load(Ordering::SeqCst) {
            info!("Booting postgres instance...");

            let mut server = PostgresServer::boot(conf.clone())?;
            state.pid.store(server.proc.pid(), Ordering::SeqCst);

            let result = server.wait().context("Failed to wait for postgres process");

            state.pid.store(0, Ordering::SeqCst);

            info!("Postgres terminated with status {}", result?);
            thread::sleep(Duration::from_secs(3));
        }

        Ok(())
    }

    /// Checks if postgres is currently running
    #[allow(unused)]
    pub fn running(&self) -> bool {
        self.state.pid.load(Ordering::SeqCst) != 0
    }

    /// Terminates the postgres instance and blocks until it has completed
    pub fn terminate(&mut self) -> Result<()> {
        if self.thread.is_none() {
            bail!("Instance already terminated")
        }

        let pid = self.state.pid.load(Ordering::SeqCst);

        if pid != 0 {
            info!(
                "Terminating the postgres instance running under pid {}",
                pid
            );
            nix::sys::signal::kill(Pid::from_raw(pid as _), Signal::SIGINT)
                .context("Failed to send SIGINT to postgres")?;
        } else {
            info!("Postgres instance is not running");
        }

        self.state.terminate.store(true, Ordering::SeqCst);
        self.thread
            .take()
            .unwrap()
            .join()
            .map_err(|_| Error::msg("Failed to join supervisor thread"))??;

        Ok(())
    }
}

impl Drop for PostgresServerManager {
    fn drop(&mut self) {
        if let Err(err) = self.terminate() {
            error!("Failed to terminate postgres instance: {}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nix::sys::signal::kill;

    use crate::{initdb::PostgresInitDb, test::assert_not_running};

    use super::*;

    fn test_pg_config(test_name: &'static str) -> PostgresConf {
        PostgresConf {
            install_dir: PathBuf::from("/usr/lib/postgresql/14"),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!("/tmp/ansilo-tests/manager/{}", test_name)),
            socket_dir_path: PathBuf::from(format!("/tmp/ansilo-tests/manager/{}", test_name)),
            fdw_socket_path: PathBuf::from("not-used"),
        }
    }

    #[test]
    fn test_postgres_manager_invalid_conf() {
        let conf = test_pg_config("invalid");

        let mut manager = PostgresServerManager::new(conf);
        thread::sleep(Duration::from_secs(1));

        // the postgres should have failed to boot and should now be sleeping
        assert_eq!(manager.running(), false);

        // terminate should still succeed
        manager.terminate().unwrap();
        assert!(manager.thread.is_none())
    }

    #[test]
    fn test_postgres_manager_running_then_drop() {
        let conf = test_pg_config("drop");
        PostgresInitDb::reset(&conf).unwrap();
        PostgresInitDb::run(conf.clone())
            .unwrap()
            .complete()
            .unwrap();

        let manager = PostgresServerManager::new(conf);
        thread::sleep(Duration::from_secs(1));

        let pid = manager.state.pid.load(Ordering::SeqCst);
        assert_eq!(manager.running(), true);

        drop(manager);

        assert_not_running(pid);
    }

    #[test]
    fn test_postgres_manager_running_then_terminate() {
        let conf = test_pg_config("terminate");
        PostgresInitDb::reset(&conf).unwrap();
        PostgresInitDb::run(conf.clone())
            .unwrap()
            .complete()
            .unwrap();

        let mut manager = PostgresServerManager::new(conf);
        thread::sleep(Duration::from_secs(1));

        let pid = manager.state.pid.load(Ordering::SeqCst);
        assert_eq!(manager.running(), true);

        manager.terminate().unwrap();

        assert_not_running(pid);
        assert!(manager.thread.is_none())
    }

    #[test]
    fn test_postgres_manager_restarts_postgres_on_crash() {
        ansilo_logging::init_for_tests();

        let conf = test_pg_config("restart");
        PostgresInitDb::reset(&conf).unwrap();
        PostgresInitDb::run(conf.clone())
            .unwrap()
            .complete()
            .unwrap();

        let manager = PostgresServerManager::new(conf);
        thread::sleep(Duration::from_millis(500));

        let pid = manager.state.pid.load(Ordering::SeqCst);
        assert_eq!(manager.running(), true);

        // simulate a crash by terminating it
        kill(Pid::from_raw(pid as _), Signal::SIGKILL).unwrap();
        thread::sleep(Duration::from_millis(100));

        // should not be running while manager sleeps
        assert_eq!(manager.running(), false);

        thread::sleep(Duration::from_secs(3));

        assert_eq!(manager.running(), true);
    }
}

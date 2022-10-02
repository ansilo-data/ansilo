use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use ansilo_core::err::{bail, Context, Error, Result};
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
    pub fn new(conf: &'static PostgresConf) -> Self {
        let state = Arc::new(State {
            pid: AtomicU32::new(0),
            terminate: AtomicBool::new(false),
        });
        let thread = {
            let state = state.clone();
            thread::Builder::new()
                .name("postgres-supervisor".into())
                .spawn(move || Self::supervise(conf, state))
                .unwrap()
        };

        Self {
            thread: Some(thread),
            state,
        }
    }

    fn supervise(conf: &'static PostgresConf, state: Arc<State>) -> Result<()> {
        loop {
            info!("Booting postgres instance...");

            let server = PostgresServer::boot(conf)?;

            if let Ok(_) =
                server.block_until_ready_opts(Duration::from_secs(30), || state.terminated())
            {
                state.pid.store(server.pid, Ordering::SeqCst);

                if state.terminated() {
                    break;
                }

                let result = server.wait().context("Failed to wait for postgres process");

                state.pid.store(0, Ordering::SeqCst);

                info!("Postgres terminated with status {}", result?);
            } else {
                drop(server);
            }

            if state.terminated() {
                break;
            }

            thread::sleep(Duration::from_secs(3));
        }

        Ok(())
    }

    /// Waits until the postgres server is ready for connections
    pub fn block_until_ready(&self, timeout: Duration) -> Result<()> {
        let mut tries = timeout.as_millis() as u64 / 100;

        loop {
            if tries <= 0 {
                bail!("Timedout while waiting for postgres to start up");
            }

            if self.running() {
                info!("Postgres is listening for connections");
                return Ok(());
            }

            thread::sleep(Duration::from_millis(100));
            tries -= 1;
        }
    }

    /// Checks if postgres is currently running
    #[allow(unused)]
    pub fn running(&self) -> bool {
        self.state.pid.load(Ordering::SeqCst) != 0
    }

    /// Terminates the postgres instance and blocks until it has completed
    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut()
    }

    fn terminate_mut(&mut self) -> Result<()> {
        if self.thread.is_none() {
            return Ok(());
        }

        // Set terminate flag to true before the kill
        // to prevent a race condition with the supervisor thread from
        // restarting postgres
        self.state.terminate.store(true, Ordering::SeqCst);

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

        self.thread
            .take()
            .unwrap()
            .join()
            .map_err(|_| Error::msg("Failed to join supervisor thread"))??;

        Ok(())
    }
}

impl State {
    fn terminated(&self) -> bool {
        self.terminate.load(Ordering::SeqCst)
    }
}

impl Drop for PostgresServerManager {
    fn drop(&mut self) {
        if self.thread.is_some() {
            if let Err(err) = self.terminate_mut() {
                error!("Failed to terminate postgres instance: {:?}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use ansilo_core::config::ResourceConfig;
    use nix::sys::signal::kill;

    use crate::{initdb::PostgresInitDb, test::assert_not_running};

    use super::*;

    fn test_pg_config(test_name: &'static str) -> &'static PostgresConf {
        let conf = PostgresConf {
            resources: ResourceConfig::default(),
            install_dir: PathBuf::from(
                std::env::var("ANSILO_TEST_PG_DIR").unwrap_or("/usr/lib/postgresql/14".into()),
            ),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!("/tmp/ansilo-tests/manager/{}", test_name)),
            socket_dir_path: PathBuf::from(format!("/tmp/ansilo-tests/manager/{}", test_name)),
            fdw_socket_path: PathBuf::from("not-used"),
            app_users: vec![],
            init_db_sql: vec![],
        };
        Box::leak(Box::new(conf))
    }

    #[test]
    fn test_postgres_manager_invalid_conf() {
        ansilo_logging::init_for_tests();
        let conf = test_pg_config("invalid");

        let manager = PostgresServerManager::new(conf);
        thread::sleep(Duration::from_secs(1));

        // the postgres should have failed to boot and should now be sleeping
        assert_eq!(manager.running(), false);

        // terminate should still succeed
        manager.terminate().unwrap();
    }

    #[test]
    fn test_postgres_manager_running_then_drop() {
        let conf = test_pg_config("drop");
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();

        let manager = PostgresServerManager::new(conf);
        manager.block_until_ready(Duration::from_secs(1)).unwrap();

        let pid = manager.state.pid.load(Ordering::SeqCst);
        assert_eq!(manager.running(), true);

        drop(manager);

        assert_not_running(pid);
    }

    #[test]
    fn test_postgres_manager_running_then_terminate() {
        let conf = test_pg_config("terminate");
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();

        let manager = PostgresServerManager::new(conf);
        manager.block_until_ready(Duration::from_secs(1)).unwrap();

        let pid = manager.state.pid.load(Ordering::SeqCst);
        assert_eq!(manager.running(), true);

        manager.terminate().unwrap();

        assert_not_running(pid);
    }

    #[test]
    fn test_postgres_manager_restarts_postgres_on_crash() {
        ansilo_logging::init_for_tests();

        let conf = test_pg_config("restart");
        PostgresInitDb::reset(conf).unwrap();
        PostgresInitDb::run(conf).unwrap().complete().unwrap();

        let manager = PostgresServerManager::new(conf);
        manager.block_until_ready(Duration::from_secs(1)).unwrap();

        let pid = manager.state.pid.load(Ordering::SeqCst);
        assert_eq!(manager.running(), true);

        // simulate a crash by terminating it
        kill(Pid::from_raw(pid as _), Signal::SIGKILL).unwrap();
        thread::sleep(Duration::from_millis(100));

        // should not be running while manager sleeps
        assert_eq!(manager.running(), false);

        thread::sleep(Duration::from_secs(10));

        assert_eq!(manager.running(), true);
    }
}

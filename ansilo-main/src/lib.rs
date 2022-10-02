use std::{
    collections::HashMap,
    os::raw::c_int,
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use crate::{args::Command, build::BuildInfo};
use ansilo_auth::Authenticator;
use ansilo_connectors_all::{
    ConnectionPools, ConnectorEntityConfigs, Connectors, InternalConnection,
};
use ansilo_core::err::{Context, Result};
use ansilo_jobs::JobScheduler;
use ansilo_logging::{error, info, trace, warn};
use ansilo_pg::{fdw::server::FdwServer, handler::PostgresConnectionHandler, PostgresInstance};
use ansilo_proxy::{conf::HandlerConf, server::ProxyServer};
use ansilo_util_health::Health;
use ansilo_web::{Http1ConnectionHandler, Http2ConnectionHandler, HttpApi, HttpApiState};
use clap::Parser;
use signal_hook::{
    consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1},
    iterator::Signals,
};

pub mod args;
pub mod build;
pub mod conf;
pub mod dev;

pub use ansilo_pg::fdw::log::RemoteQueryLog;

use build::*;
use conf::*;
use tokio::runtime::Runtime;

/// This struct represents a running instance of ansilo and its subsystems.
///
/// This is the entrypoint to build, start and manage the instance.
pub struct Ansilo {
    /// The command used to start the instance
    command: Command,
    /// The configuration used
    conf: &'static AppConf,
    /// Running subsystems
    subsystems: Option<Subsystems>,
    /// Remote query log
    log: RemoteQueryLog,
    /// Health status
    health: Health,
    /// Whether the instance has been terminated
    term: Arc<AtomicBool>,
}

pub struct Subsystems {
    /// The tokio runtime
    runtime: Runtime,
    /// The postgres instance
    postgres: PostgresInstance,
    /// The FDW server
    fdw: FdwServer,
    /// The proxy server
    proxy: ProxyServer,
    /// The authentication system
    authenticator: Authenticator,
    /// The http api
    http: HttpApi,
    /// The job scheduler
    scheduler: JobScheduler,
}

impl Ansilo {
    /// This is the entrypoint to booting Ansilo.
    /// Here, we start the initial launch sequence.
    pub fn main() {
        ansilo_logging::init_logging().unwrap();
        info!("Hi, thanks for using Ansilo!");

        let cmd = Command::parse();
        let boot = || Self::start(cmd.clone(), None).unwrap().wait().unwrap();

        // In dev mode we want to restart if the config is invalid
        // On error we wait for a signal to either terminate or restart
        // SIGHUP is triggered by our file inotify watcher.
        if cmd.is_dev() || cmd.is_dump_config() {
            if let Err(_) = panic::catch_unwind(boot) {
                error!("Error while booting ansilo, waiting for change before restart...");
                if SIGHUP == Self::wait_for_signal().unwrap() {
                    dev::restart();
                }
            }
        } else {
            boot()
        }
    }

    /// Runs the supplied command
    pub fn start(command: Command, log: Option<RemoteQueryLog>) -> Result<Self> {
        let args = command.args();
        let log = log.unwrap_or_default();

        // Load configuration
        let config_path = args.config();

        if command.is_dev() || command.is_dump_config() {
            let config_path = config_path.clone();
            thread::spawn(move || {
                dev::signal_on_config_update(&config_path);
            });
        }

        if command.is_dump_config() {
            dump_conf(&config_path, &args)?;
            std::process::exit(0);
        }

        // We are happy to let the app-wide config leak for the rest of the program
        let conf: &'static _ = Box::leak(Box::new(init_conf(&config_path, &args)?));

        if command.is_dev() {
            thread::spawn(|| {
                dev::signal_on_sql_update(conf);
            });
        }

        // Boot the tokio runtime
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("ansilo-tokio-worker")
            .enable_all()
            .build()
            .context("Failed to create tokio runtime")?;

        let pools = Self::init_connectors(conf)?;

        info!("Starting fdw listener...");
        let fdw = FdwServer::start(
            &conf.node,
            conf.pg.fdw_socket_path.clone(),
            pools,
            log.clone(),
        )
        .context("Failed to start fdw server")?;

        info!("Starting authenticator...");
        let authenticator = Authenticator::init(&conf.node.auth)?;

        let (mut postgres, build_info) = if let (Command::Run(_), false, Some(build_info)) =
            (&command, args.force_build, BuildInfo::fetch(conf)?)
        {
            info!("Build occurred at {}", build_info.built_at().to_rfc3339());
            info!("Starting postgres...");
            let pg = runtime.block_on(PostgresInstance::start(&conf.pg))?;
            (pg, build_info)
        } else {
            runtime.block_on(build(conf, authenticator.clone()))?
        };

        let health = Health::new();
        let term = Arc::new(AtomicBool::new(false));

        if command.is_build() {
            info!("Build complete...");
            return Ok(Self {
                command,
                conf,
                subsystems: None,
                log,
                health,
                term,
            });
        }

        let pg_con_handler =
            PostgresConnectionHandler::new(authenticator.clone(), postgres.connections().clone());

        info!("Starting http api...");
        let http = runtime.block_on(HttpApi::start(HttpApiState::new(
            &conf.node,
            postgres.connections().clone(),
            pg_con_handler.clone(),
            health.clone(),
            (&build_info).into(),
        )))?;

        info!("Starting proxy server...");
        let proxy_conf = Box::leak(Box::new(init_proxy_conf(
            conf,
            HandlerConf::new(
                pg_con_handler.clone(),
                Http2ConnectionHandler::new(http.handler()),
                Http1ConnectionHandler::new(http.handler()),
            ),
        )));

        let mut proxy = ProxyServer::new(proxy_conf);
        runtime
            .block_on(proxy.start())
            .context("Failed to start proxy server")?;

        info!("Staring job scheduler...");
        let mut scheduler =
            JobScheduler::new(&conf.node.jobs, runtime.handle().clone(), pg_con_handler);
        scheduler.start().context("Failed to start job scheduler")?;

        let instance = Self {
            command,
            conf,
            subsystems: Some(Subsystems {
                runtime,
                postgres,
                fdw,
                proxy,
                authenticator,
                http,
                scheduler,
            }),
            log,
            health,
            term,
        };

        instance.check_health();

        info!("Start up complete...");
        Ok(instance)
    }

    /// Gets the app config
    pub fn conf(&self) -> &AppConf {
        &self.conf
    }

    /// Gets the running subsystems
    pub fn subsystems(&self) -> Option<&Subsystems> {
        self.subsystems.as_ref()
    }

    /// Gets the remote query log
    pub fn log(&self) -> &RemoteQueryLog {
        &self.log
    }

    /// Waits for instance to terminate
    pub fn wait(mut self) -> Result<()> {
        if self.command.is_build() {
            return Ok(());
        }

        // Update service health every 30s
        self.check_health();
        let term = Arc::clone(&self.term);
        thread::spawn(move || {
            while !term.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_secs(30));
                let _ = nix::sys::signal::kill(nix::unistd::getpid(), nix::sys::signal::SIGUSR1);
            }
        });

        let sig = loop {
            let sig = Self::wait_for_signal()?;

            if sig == SIGUSR1 {
                self.check_health();
                continue;
            }

            break sig;
        };

        self.terminate_mut(Some(sig))?;

        Ok(())
    }

    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut(None)
    }

    fn terminate_mut(&mut self, sig: Option<c_int>) -> Result<()> {
        let subsystems = match self.subsystems.take() {
            Some(s) => s,
            None => return Ok(()),
        };

        self.term.store(true, Ordering::SeqCst);

        info!("Terminating...");
        if let Err(err) = subsystems.scheduler.terminate() {
            warn!("Failed to terminate job scheduler: {:?}", err);
        }
        if let Err(err) = subsystems.http.terminate() {
            warn!("Failed to terminate http api: {:?}", err);
        }
        if let Err(err) = subsystems.authenticator.terminate() {
            warn!("Failed to terminate authenticator: {:?}", err);
        }
        if let Err(err) = subsystems.proxy.terminate() {
            warn!("Failed to terminate proxy server: {:?}", err);
        }
        if let Err(err) = subsystems.postgres.terminate() {
            warn!("Failed to terminate postgres: {:?}", err);
        }
        if let Err(err) = subsystems.fdw.terminate() {
            warn!("Failed to terminate fdw server: {:?}", err);
        }

        subsystems.runtime.shutdown_timeout(Duration::from_secs(3));

        info!("Shutdown sequence complete");

        // If we are running in dev-mode, restart the process
        if self.command.is_dev() && sig == Some(SIGUSR1) {
            dev::restart();
        }

        Ok(())
    }

    fn init_connectors(
        conf: &'static AppConf,
    ) -> Result<HashMap<String, (ConnectionPools, ConnectorEntityConfigs)>> {
        info!("Initializing connectors...");
        let mut pools: HashMap<_, _> = conf
            .node
            .sources
            .iter()
            .map(|i| {
                info!("Initializing connector: {}", i.id);
                let connector = Connectors::from_type(&i.r#type)
                    .with_context(|| format!("Unknown connector type: {}", i.r#type))?;
                let options = connector
                    .parse_options(i.options.clone())
                    .context("Failed to parse options")?;

                let pool = connector
                    .create_connection_pool(&conf.node, &i.id, options)
                    .context("Failed to create connection pool")?;

                Ok((i.id.clone(), pool))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        pools.insert(
            "internal".into(),
            (
                ConnectionPools::Internal(InternalConnection(&conf.node)),
                ConnectorEntityConfigs::Internal,
            ),
        );

        Ok(pools)
    }

    /// Updates the health of the each subsystem
    fn check_health(&self) {
        if let Some(ref subsystems) = self.subsystems {
            trace!("Updating system health status");

            let _ = self
                .health
                .update("Authenticator", subsystems.authenticator().healthy());
            let _ = self
                .health
                .update("Postgres", subsystems.postgres().healthy());
            let _ = self.health.update("Proxy", subsystems.proxy().healthy());
            let _ = self.health.update("FDW", subsystems.fdw().healthy());
            let _ = self.health.update("HTTP", subsystems.http().healthy());
            let _ = self
                .health
                .update("Scheduler", subsystems.scheduler().healthy());
        }
    }

    fn wait_for_signal() -> Result<i32> {
        let mut sigs = Signals::new(&[SIGINT, SIGQUIT, SIGTERM, SIGHUP, SIGUSR1])
            .context("Failed to attach signal handler")?;
        let sig = sigs.forever().next().unwrap();

        info!(
            "Received {}",
            match sig {
                SIGINT => "SIGINT".into(),
                SIGQUIT => "SIGQUIT".into(),
                SIGTERM => "SIGTERM".into(),
                SIGHUP => "SIGHUP".into(),
                SIGUSR1 => return Ok(sig),
                _ => format!("unknown signal {}", sig),
            }
        );

        Ok(sig)
    }
}

impl Drop for Ansilo {
    fn drop(&mut self) {
        if let Err(err) = self.terminate_mut(None) {
            warn!("Error occurred while shutting down: {:?}", err);
        }
    }
}

impl Subsystems {
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn postgres(&self) -> &PostgresInstance {
        &self.postgres
    }

    pub fn fdw(&self) -> &FdwServer {
        &self.fdw
    }

    pub fn proxy(&self) -> &ProxyServer {
        &self.proxy
    }

    pub fn authenticator(&self) -> &Authenticator {
        &self.authenticator
    }

    pub fn http(&self) -> &HttpApi {
        &self.http
    }

    pub fn scheduler(&self) -> &JobScheduler {
        &self.scheduler
    }
}

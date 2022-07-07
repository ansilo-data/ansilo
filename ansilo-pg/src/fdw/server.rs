use std::{
    collections::HashMap,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
};

use ansilo_connectors::{
    common::entity::ConnectorEntityConfig,
    interface::{
        container::{ConnectionPools, Connections, Connectors},
        *,
    },
    jdbc_oracle::OracleJdbcConnector,
};
use ansilo_core::{
    config::NodeConfig,
    err::{bail, Context, Result},
};
use ansilo_logging::error;

use super::{channel::IpcServerChannel, connection::FdwConnection};

// /// TODO: organise
// pub struct AppState {
//     /// The ansilo app config
//     conf: &'static NodeConfig,
//     /// The instance connection pools
//     pools: HashMap<String, ConnectionPools>,
// }

// impl AppState {
//     fn connection(&mut self, data_source_id: &str) -> Result<Connections> {
//         if !self.pools.contains_key(data_source_id) {
//             let source_conf = self
//                 .conf
//                 .sources
//                 .iter()
//                 .find(|i| i.id == data_source_id)
//                 .unwrap();

//             let connector = source_conf.r#type.parse::<Connectors>()?;
//             let config = connector.parse_options(source_conf.options.clone())?;
//             let pool = connector.create_connection_pool(self.conf, data_source_id, config)?;

//             self.pools.insert(data_source_id.to_string(), pool);
//         }

//         let pool = self.pools.get_mut(data_source_id).unwrap();

//         pool.acquire()
//     }
// }

/// Handles connections back from postgres
pub struct FdwServer {
    /// Mapping of data source ids to their respective socket paths
    paths: HashMap<String, PathBuf>,
    /// Listener threads
    threads: Vec<JoinHandle<()>>,
}

impl FdwServer {
    /// Starts a new server instance listening at the specified path
    pub fn start(path: PathBuf, pools: HashMap<String, ConnectionPools>) -> Result<Self> {
        let paths = Self::create_paths(path.as_path(), &pools);
        let threads = Self::start_threads(&paths, &pools)?;

        Ok(Self { paths, threads })
    }

    /// Gets the mapping of data source ids to their paths
    pub fn paths(&self) -> &HashMap<String, PathBuf> {
        &self.paths
    }

    /// Waits for all listener threads complete
    pub fn wait(self) -> Result<()> {
        // TODO: Use mpsc channels to receive early terminations
        for thread in self.threads.into_iter() {
            if let Err(_) = thread.join() {
                bail!("Error occurred while waiting for thread")
            }
        }

        Ok(())
    }

    fn create_paths(
        path: &Path,
        pools: &HashMap<String, ConnectionPools>,
    ) -> HashMap<String, PathBuf> {
        pools
            .iter()
            .map(|(id, _)| (id.to_owned(), path.join(id).join(".sock")))
            .collect()
    }

    fn start_threads(
        paths: &HashMap<String, PathBuf>,
        pools: &HashMap<String, ConnectionPools>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let mut threads = vec![];

        for (id, path) in paths.iter() {
            let pool = pools
                .get(id)
                .context("Failed to find connection pool with id")?
                .clone();
            let path = path.clone();

            threads.push(thread::spawn(move || {
                let res = FdwServer::listen(&path, pool);

                if let Err(err) = res {
                    error!("FDW Listener error: {}", err);
                }
            }));
        }

        Ok(threads)
    }

    fn listen(path: &PathBuf, pool: ConnectionPools) -> Result<()> {
        match pool {
            ConnectionPools::OracleJdbc(pool, entities) => {
                FdwListener::<OracleJdbcConnector>::bind(path, pool, entities)?.listen()?
            }
        };

        Ok(())
    }
}

/// Handles connections from postgres, serving data from a connector
pub struct FdwListener<TConnector: Connector> {
    /// The unix socket the server listens on
    listener: UnixListener,
    /// The instance connection pool attached to this server
    pool: TConnector::TConnectionPool,
    /// Connector entity config
    entities: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
}

impl<TConnector: Connector> FdwListener<TConnector> {
    /// Starts a server which listens
    pub fn bind(
        path: &Path,
        pool: TConnector::TConnectionPool,
        entities: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
    ) -> Result<Self> {
        let listener = UnixListener::bind(path).context("Failed to bind socket")?;

        Ok(Self {
            listener,
            pool,
            entities,
        })
    }

    /// Starts processing incoming connections
    pub fn listen(&mut self) -> Result<()> {
        for con in self.listener.incoming() {
            self.start(con.context("Failed to accept incoming connection")?)?;
        }

        Ok(())
    }

    /// Starts the thread responsible for executing the supplied connection
    fn start(&self, socket: UnixStream) -> Result<()> {
        let entities = self.entities.clone();
        let pool = self.pool.clone();

        let _ = thread::spawn(move || {
            let chan = IpcServerChannel::new(socket);
            let mut fdw_con = FdwConnection::<TConnector>::new(chan, entities, pool);

            if let Err(err) = fdw_con.process() {
                error!("Error while processing FDW connection: {}", err);
            }
        });

        Ok(())
    }
}

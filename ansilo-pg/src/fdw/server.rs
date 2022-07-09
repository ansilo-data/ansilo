use std::{
    collections::HashMap,
    fs,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
};

use ansilo_connectors::{
    common::entity::ConnectorEntityConfig,
    interface::{container::ConnectionPools, *},
    jdbc_oracle::OracleJdbcConnector,
    memory::MemoryConnector,
};
use ansilo_core::err::{bail, Context, Result};
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
            .map(|(id, _)| (id.to_owned(), path.join(format!("{id}.sock"))))
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

            let _ = fs::remove_file(&path);
            let listener = UnixListener::bind(path)
                .with_context(|| format!("Failed to bind socket at {}", path.display()))?;

            threads.push(thread::spawn(move || {
                let res = FdwServer::listen(listener, pool);

                if let Err(err) = res {
                    error!("FDW Listener error: {}", err);
                }
            }));
        }

        Ok(threads)
    }

    fn listen(listener: UnixListener, pool: ConnectionPools) -> Result<()> {
        match pool {
            ConnectionPools::OracleJdbc(pool, entities) => {
                FdwListener::<OracleJdbcConnector>::bind(listener, pool, entities).listen()?
            }
            ConnectionPools::Memory(pool, entities) => {
                FdwListener::<MemoryConnector>::bind(listener, pool, entities).listen()?
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
        listener: UnixListener,
        pool: TConnector::TConnectionPool,
        entities: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
    ) -> Self {
        Self {
            listener,
            pool,
            entities,
        }
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

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use ansilo_connectors::{
        common::entity::EntitySource,
        memory::{MemoryConnectionConfig, MemoryConnectionPool, MemoryConnector},
    };
    use ansilo_core::{
        common::data::{DataType, DataValue},
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig, NodeConfig},
        sqlil,
    };

    use crate::fdw::{
        channel::IpcClientChannel,
        proto::{ClientMessage, ServerMessage},
    };

    use super::*;

    fn create_memory_connection_pool() -> (ConnectorEntityConfig<()>, MemoryConnectionPool) {
        let mut conf = MemoryConnectionConfig::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::minimal(
            "people",
            EntityVersionConfig::minimal(
                "1.0",
                vec![
                    EntityAttributeConfig::minimal("first_name", DataType::rust_string()),
                    EntityAttributeConfig::minimal("last_name", DataType::rust_string()),
                ],
                EntitySourceConfig::minimal(""),
            ),
            (),
        ));

        conf.set_data(
            "people",
            "1.0",
            vec![
                vec![DataValue::from("Mary"), DataValue::from("Jane")],
                vec![DataValue::from("John"), DataValue::from("Smith")],
                vec![DataValue::from("Gary"), DataValue::from("Gregson")],
            ],
        );

        let pool = MemoryConnector::create_connection_pool(conf, &NodeConfig::default(), &entities)
            .unwrap();

        (entities, pool)
    }

    fn create_server(test_name: &'static str) -> FdwServer {
        let (entities, pool) = create_memory_connection_pool();
        let pool = ConnectionPools::Memory(pool, entities);
        let path = PathBuf::from(format!("/tmp/ansilo/fdw_server/{test_name}"));
        fs::create_dir_all(path.clone()).unwrap();

        let server =
            FdwServer::start(path, [("memory".to_string(), pool)].into_iter().collect()).unwrap();
        thread::sleep(Duration::from_millis(10));

        server
    }

    fn create_client_ipc_channel(server: &FdwServer) -> IpcClientChannel {
        let path = server.paths().get("memory").unwrap();

        IpcClientChannel::new(UnixStream::connect(path).unwrap())
    }

    fn send_auth_token(client: &mut IpcClientChannel) {
        let res = client
            .send(ClientMessage::AuthDataSource("TOKEN".to_string()))
            .unwrap();
        assert_eq!(res, ServerMessage::AuthAccepted);
    }

    #[test]
    fn test_fdw_server_connect_and_estimate_size() {
        let server = create_server("estimate_size");
        let mut client = create_client_ipc_channel(&server);

        send_auth_token(&mut client);

        let res = client
            .send(ClientMessage::EstimateSize(sqlil::entity("people", "1.0")))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::EstimatedSizeResult(EntitySizeEstimate::new(Some(3), None))
        );

        client.close().unwrap();
    }
}

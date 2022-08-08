use std::{
    collections::HashMap,
    fs,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::Arc,
    thread::{self, JoinHandle},
};

use ansilo_connectors::{
    common::entity::ConnectorEntityConfig,
    interface::{container::ConnectionPools, *},
    jdbc_oracle::OracleJdbcConnector,
    memory::MemoryConnector,
};
use ansilo_core::err::{bail, Context, Result};
use ansilo_logging::{error, warn};

use super::{
    channel::IpcServerChannel,
    connection::FdwConnection,
    proto::{ClientMessage, ServerMessage},
};

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
    /// The path of the socket which the server is listening on
    path: PathBuf,
    /// Listener thread
    thread: JoinHandle<()>,
}

impl FdwServer {
    /// Starts a new server instance listening at the specified path
    pub fn start(path: PathBuf, pools: HashMap<String, ConnectionPools>) -> Result<Self> {
        let thread = Self::start_listening_thread(path.as_path(), pools)?;

        Ok(Self { path, thread })
    }

    /// Gets the mapping of data source ids to their paths
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Waits for the listener thread complete
    pub fn wait(self) -> Result<()> {
        if let Err(_) = self.thread.join() {
            bail!("Error occurred while waiting for listener thread")
        }

        Ok(())
    }

    fn start_listening_thread(
        path: &Path,
        pools: HashMap<String, ConnectionPools>,
    ) -> Result<JoinHandle<()>> {
        let _ = fs::remove_file(&path);
        let listener = UnixListener::bind(path)
            .with_context(|| format!("Failed to bind socket at {}", path.display()))?;

        let thread = thread::spawn(move || {
            let res = FdwListener::bind(listener, pools).listen();

            if let Err(err) = res {
                error!("FDW listener error: {}", err);
            }
        });

        Ok(thread)
    }
}

/// Handles connections from postgres, serving data from a connector
pub struct FdwListener {
    /// The unix socket the server listens on
    listener: UnixListener,
    /// The connection pools keyed by their data source id
    pools: Arc<HashMap<String, ConnectionPools>>,
}

impl FdwListener {
    /// Starts a server which listens
    pub fn bind(listener: UnixListener, pools: HashMap<String, ConnectionPools>) -> Self {
        Self {
            listener,
            pools: Arc::new(pools),
        }
    }

    /// Starts processing incoming connections
    pub fn listen(&mut self) -> Result<()> {
        for con in self.listener.incoming() {
            self.start(con.context("Failed to accept incoming connection")?)?;
        }

        Ok(())
    }

    /// Starts the thread responsible for processing the supplied connection
    fn start(&self, socket: UnixStream) -> Result<()> {
        let pool = Arc::clone(&self.pools);

        let _ = thread::spawn(move || {
            let mut chan = IpcServerChannel::new(socket);

            let pool = match Self::auth(&mut chan, pool) {
                Ok(pool) => pool,
                Err(err) => {
                    warn!("Failed to authenticate client: {}", err);
                    return;
                }
            };

            match pool {
                ConnectionPools::OracleJdbc(pool, entities) => {
                    Self::process::<OracleJdbcConnector>(chan, pool, entities)
                }
                ConnectionPools::Memory(pool, entities) => {
                    Self::process::<MemoryConnector>(chan, pool, entities)
                }
            };
        });

        Ok(())
    }

    fn auth(
        chan: &mut IpcServerChannel,
        pools: Arc<HashMap<String, ConnectionPools>>,
    ) -> Result<ConnectionPools> {
        chan.recv_with_return(|msg| {
            let auth = match msg {
                ClientMessage::AuthDataSource(auth) => auth,
                _ => bail!("Received unexpected message from client: {:?}", msg),
            };

            // TODO: auth token

            let pool = pools
                .get(&auth.data_source_id)
                .map(|i| i.clone())
                .with_context(|| {
                    format!(
                        "Failed to find data source with id: {}",
                        auth.data_source_id
                    )
                });

            let response = match pool {
                Ok(_) => ServerMessage::AuthAccepted,
                Err(_) => ServerMessage::Error("Unknown data source id".to_string()),
            };

            Ok((Some(response), pool))
        })?
    }

    fn process<TConnector: Connector>(
        chan: IpcServerChannel,
        pool: TConnector::TConnectionPool,
        entities: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
    ) {
        let mut fdw_con = FdwConnection::<TConnector>::new(chan, entities, pool);

        if let Err(err) = fdw_con.process() {
            error!("Error while processing FDW connection: {}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use ansilo_connectors::{
        common::entity::EntitySource,
        memory::{
            MemoryDatabase, MemoryConnectionPool, MemoryConnector,
            MemoryConnectorEntitySourceConfig,
        },
    };
    use ansilo_core::{
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig, NodeConfig},
        data::{DataType, DataValue},
        sqlil,
    };

    use crate::fdw::{
        channel::IpcClientChannel,
        proto::{AuthDataSource, ClientMessage, ServerMessage},
    };

    use super::*;

    fn create_memory_connection_pool() -> (ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>, MemoryConnectionPool) {
        let conf = MemoryDatabase::new();
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
            MemoryConnectorEntitySourceConfig::default(),
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
        fs::create_dir_all(path.parent().unwrap().clone()).unwrap();

        let server =
            FdwServer::start(path, [("memory".to_string(), pool)].into_iter().collect()).unwrap();
        thread::sleep(Duration::from_millis(10));

        server
    }

    fn create_client_ipc_channel(server: &FdwServer) -> IpcClientChannel {
        IpcClientChannel::new(UnixStream::connect(server.path()).unwrap())
    }

    fn send_auth_token(client: &mut IpcClientChannel, data_source_id: &'static str) {
        let res = client
            .send(ClientMessage::AuthDataSource(AuthDataSource::new(
                "TOKEN",
                data_source_id,
            )))
            .unwrap();
        assert_eq!(res, ServerMessage::AuthAccepted);
    }

    #[test]
    fn test_fdw_server_invalid_data_source_id() {
        let server = create_server("invalid_data_source_id");
        let mut client = create_client_ipc_channel(&server);

        let res = client
            .send(ClientMessage::AuthDataSource(AuthDataSource::new(
                "TOKEN",
                "invalid_id",
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Error("Unknown data source id".to_string())
        );

        let _ = client.close();
    }

    #[test]
    fn test_fdw_server_connect_and_estimate_size() {
        let server = create_server("estimate_size");
        let mut client = create_client_ipc_channel(&server);

        send_auth_token(&mut client, "memory");

        let res = client
            .send(ClientMessage::EstimateSize(sqlil::entity("people", "1.0")))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::EstimatedSizeResult(OperationCost::new(Some(3), None, None, None))
        );

        client.close().unwrap();
    }
}

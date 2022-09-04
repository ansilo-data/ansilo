use std::{
    collections::HashMap,
    fs,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
};

use ansilo_connectors_all::*;
use ansilo_connectors_base::{common::entity::ConnectorEntityConfig, interface::Connector};
use ansilo_core::{
    config::NodeConfig,
    err::{bail, Context, Result},
};
use ansilo_logging::{error, warn};

use super::{
    channel::IpcServerChannel,
    connection::FdwConnection,
    log::RemoteQueryLog,
    proto::{ClientMessage, ServerMessage},
};

/// Handles connections back from postgres
pub struct FdwServer {
    /// Global node configuration
    #[allow(unused)]
    nc: &'static NodeConfig,
    /// The path of the socket which the server is listening on
    path: PathBuf,
    /// Listener thread
    thread: Option<JoinHandle<()>>,
    /// Whether the server is terminated
    terminated: Arc<AtomicBool>,
}

impl FdwServer {
    /// Starts a new server instance listening at the specified path
    pub fn start(
        nc: &'static NodeConfig,
        path: PathBuf,
        pools: HashMap<String, (ConnectionPools, ConnectorEntityConfigs)>,
        log: RemoteQueryLog,
    ) -> Result<Self> {
        let (thread, terminated) = Self::start_listening_thread(nc, path.as_path(), pools, log)?;

        Ok(Self {
            nc,
            path,
            thread: Some(thread),
            terminated,
        })
    }

    /// Gets the mapping of data source ids to their paths
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Waits for the listener thread complete
    pub fn wait(&mut self) -> Result<()> {
        if let Err(_) = self.thread.take().unwrap().join() {
            bail!("Error occurred while waiting for listener thread")
        }

        Ok(())
    }

    /// Terminates the current server
    pub fn terminate(mut self) -> Result<()> {
        self.terminate_mut()
    }

    fn terminate_mut(&mut self) -> Result<()> {
        if self.terminated.load(Ordering::SeqCst) {
            return Ok(());
        }

        self.terminated.store(true, Ordering::SeqCst);

        // Run a throw-away thread to trigger a bind to the unix socket
        // in order to trigger its shutdown
        {
            let path = self.path.clone();
            thread::spawn(move || {
                if let Err(err) = UnixStream::connect(&path) {
                    warn!(
                        "Failed to connect to fdw unix socket during termination procedure: {:?}",
                        err
                    );
                }
            });
        }

        self.wait()
    }

    fn start_listening_thread(
        nc: &'static NodeConfig,
        path: &Path,
        pools: HashMap<String, (ConnectionPools, ConnectorEntityConfigs)>,
        log: RemoteQueryLog,
    ) -> Result<(JoinHandle<()>, Arc<AtomicBool>)> {
        let terminated = Arc::new(AtomicBool::new(false));

        let thread = {
            let _ = fs::remove_file(&path);
            fs::create_dir_all(path.parent().context("Failed to get path parent")?)
                .with_context(|| format!("Could not create parent path for {}", path.display()))?;
            let listener = UnixListener::bind(path)
                .with_context(|| format!("Failed to bind socket at {}", path.display()))?;
            let terminated = Arc::clone(&terminated);

            thread::spawn(move || {
                let res = FdwListener::bind(nc, listener, pools, terminated, log).listen();

                if let Err(err) = res {
                    error!("FDW listener error: {}", err);
                }
            })
        };

        Ok((thread, terminated))
    }
}

/// Handles connections from postgres, serving data from a connector
pub struct FdwListener {
    /// Global node configuration
    nc: &'static NodeConfig,
    /// The unix socket the server listens on
    listener: UnixListener,
    /// The connection pools and entity config keyed by their data source id.
    ///
    /// We wrap each list of entities in a RW lock as these may be
    /// added to when new entities are registered from a connection.
    pools: Arc<HashMap<String, (ConnectionPools, Arc<RwLockEntityConfigs>)>>,
    /// Whether the server is terminated
    terminated: Arc<AtomicBool>,
    /// Remote query log
    log: RemoteQueryLog,
}

impl FdwListener {
    /// Starts a server which listens
    pub fn bind(
        nc: &'static NodeConfig,
        listener: UnixListener,
        pools: HashMap<String, (ConnectionPools, ConnectorEntityConfigs)>,
        terminated: Arc<AtomicBool>,
        log: RemoteQueryLog,
    ) -> Self {
        Self {
            nc,
            listener,
            pools: Arc::new(
                pools
                    .into_iter()
                    .map(|(k, (p, e))| (k, (p, Arc::new(e.into()))))
                    .collect(),
            ),
            terminated,
            log,
        }
    }

    /// Starts processing incoming connections
    pub fn listen(&mut self) -> Result<()> {
        for con in self.listener.incoming() {
            if self.terminated.load(Ordering::SeqCst) {
                break;
            }

            self.start(con.context("Failed to accept incoming connection")?)?;
        }

        Ok(())
    }

    /// Starts the thread responsible for processing the supplied connection
    fn start(&self, socket: UnixStream) -> Result<()> {
        let pool = Arc::clone(&self.pools);
        let nc = self.nc;
        let log = self.log.clone();

        let _ = thread::spawn(move || {
            let mut chan = IpcServerChannel::new(socket);

            let (ds_id, pool, entities) = match Self::auth(&mut chan, pool) {
                Ok(pool) => pool,
                Err(err) => {
                    warn!("Failed to authenticate client: {:?}", err);
                    return;
                }
            };

            match (pool, &*entities) {
                (ConnectionPools::Jdbc(pool), RwLockEntityConfigs::OracleJdbc(entities)) => {
                    Self::process::<OracleJdbcConnector>(ds_id, nc, chan, pool, entities, log)
                }
                (ConnectionPools::Jdbc(pool), RwLockEntityConfigs::MysqlJdbc(entities)) => {
                    Self::process::<MysqlJdbcConnector>(ds_id, nc, chan, pool, entities, log)
                }
                (ConnectionPools::Memory(pool), RwLockEntityConfigs::Memory(entities)) => {
                    Self::process::<MemoryConnector>(ds_id, nc, chan, pool, entities, log)
                }
                _ => {
                    panic!("Unknown types or mismatch between pool and entities",)
                }
            };
        });

        Ok(())
    }

    fn auth(
        chan: &mut IpcServerChannel,
        pools: Arc<HashMap<String, (ConnectionPools, Arc<RwLockEntityConfigs>)>>,
    ) -> Result<(String, ConnectionPools, Arc<RwLockEntityConfigs>)> {
        chan.recv_with_return(|msg| {
            let auth = match msg {
                ClientMessage::AuthDataSource(auth) => auth,
                _ => bail!("Received unexpected message from client: {:?}", msg),
            };

            // TODO: auth token

            let pool = pools
                .get(&auth.data_source_id)
                .cloned()
                .map(|(pool, entities)| (auth.data_source_id.clone(), pool, entities))
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
        data_source_id: String,
        nc: &'static NodeConfig,
        chan: IpcServerChannel,
        pool: TConnector::TConnectionPool,
        entities: &RwLock<ConnectorEntityConfig<TConnector::TEntitySourceConfig>>,
        log: RemoteQueryLog,
    ) {
        let mut fdw_con =
            FdwConnection::<TConnector>::new(data_source_id, nc, chan, entities, pool, log);

        if let Err(err) = fdw_con.process() {
            error!("Error while processing FDW connection: {:?}", err);
        }
    }
}

impl Drop for FdwServer {
    fn drop(&mut self) {
        if let Err(err) = self.terminate_mut() {
            warn!("Error while terminating fdw server: {:?}", err)
        }
    }
}

pub enum RwLockEntityConfigs {
    OracleJdbc(
        RwLock<ConnectorEntityConfig<<OracleJdbcConnector as Connector>::TEntitySourceConfig>>,
    ),
    MysqlJdbc(
        RwLock<ConnectorEntityConfig<<MysqlJdbcConnector as Connector>::TEntitySourceConfig>>,
    ),
    Memory(RwLock<ConnectorEntityConfig<<MemoryConnector as Connector>::TEntitySourceConfig>>),
}

impl From<ConnectorEntityConfigs> for RwLockEntityConfigs {
    fn from(conf: ConnectorEntityConfigs) -> Self {
        match conf {
            ConnectorEntityConfigs::OracleJdbc(e) => Self::OracleJdbc(RwLock::new(e)),
            ConnectorEntityConfigs::MysqlJdbc(e) => Self::MysqlJdbc(RwLock::new(e)),
            ConnectorEntityConfigs::Memory(e) => Self::Memory(RwLock::new(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use ansilo_connectors_base::{common::entity::EntitySource, interface::OperationCost};
    use ansilo_connectors_memory::{
        MemoryConnectionPool, MemoryConnector, MemoryConnectorEntitySourceConfig, MemoryDatabase,
    };
    use ansilo_core::{
        config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
        data::{DataType, DataValue},
        sqlil,
    };
    use lazy_static::lazy_static;

    use crate::fdw::{
        channel::IpcClientChannel,
        proto::{AuthDataSource, ClientMessage, ServerMessage},
    };

    use super::*;

    lazy_static! {
        static ref NODE_CONFIG: NodeConfig = NodeConfig::default();
    }

    fn create_memory_connection_pool() -> (
        ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        MemoryConnectionPool,
    ) {
        let conf = MemoryDatabase::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::new(
            EntityConfig::minimal(
                "people",
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
            vec![
                vec![DataValue::from("Mary"), DataValue::from("Jane")],
                vec![DataValue::from("John"), DataValue::from("Smith")],
                vec![DataValue::from("Gary"), DataValue::from("Gregson")],
            ],
        );

        let pool = MemoryConnector::create_connection_pool(conf, &NODE_CONFIG, &entities).unwrap();

        (entities, pool)
    }

    fn create_server(test_name: &'static str) -> FdwServer {
        let (entities, pool) = create_memory_connection_pool();
        let pool = ConnectionPools::Memory(pool);
        let entities = ConnectorEntityConfigs::Memory(entities);
        let path = PathBuf::from(format!("/tmp/ansilo/fdw_server/{test_name}"));
        fs::create_dir_all(path.parent().unwrap().clone()).unwrap();

        let server = FdwServer::start(
            &NODE_CONFIG,
            path,
            [("memory".to_string(), (pool, entities))]
                .into_iter()
                .collect(),
            RemoteQueryLog::new(),
        )
        .unwrap();
        thread::sleep(Duration::from_millis(10));

        server
    }

    fn create_client_ipc_channel(server: &FdwServer) -> IpcClientChannel {
        IpcClientChannel::new(UnixStream::connect(server.path()).unwrap())
    }

    fn send_auth_token(client: &mut IpcClientChannel, data_source_id: &'static str) {
        let res = client
            .send(ClientMessage::AuthDataSource(AuthDataSource::new(
                None,
                data_source_id,
            )))
            .unwrap();
        assert_eq!(res, ServerMessage::AuthAccepted);
    }

    #[test]
    fn test_fdw_server_terminate() {
        let server = create_server("terminate");

        server.terminate().unwrap();
    }

    #[test]
    fn test_fdw_server_invalid_data_source_id() {
        let server = create_server("invalid_data_source_id");
        let mut client = create_client_ipc_channel(&server);

        let res = client
            .send(ClientMessage::AuthDataSource(AuthDataSource::new(
                None,
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
            .send(ClientMessage::EstimateSize(sqlil::entity("people")))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::EstimatedSizeResult(OperationCost::new(Some(3), None, None, None))
        );

        client.close().unwrap();
    }
}

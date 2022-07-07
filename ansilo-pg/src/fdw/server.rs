use std::{
    collections::HashMap,
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
};

use ansilo_connectors::{
    common::{
        data::{QueryHandleWrite, ResultSetRead},
        entity::ConnectorEntityConfig,
    },
    interface::{
        container::{ConnectionPools, Connections, Connectors},
        *,
    }, jdbc_oracle::OracleJdbcConnector,
};
use ansilo_core::{
    config::NodeConfig,
    err::{bail, Context, Error, Result},
    sqlil::{self, EntityVersionIdentifier},
};
use ansilo_logging::error;

use super::{
    bincode::bincode_conf,
    proto::{ClientMessage, ClientSelectMessage, ServerMessage, ServerSelectMessage},
};

/// TODO: organise
pub struct AppState {
    /// The ansilo app config
    conf: &'static NodeConfig,
    /// The instance connection pools
    pools: HashMap<String, ConnectionPools>,
}

impl AppState {
    fn connection(&mut self, data_source_id: &str) -> Result<Connections> {
        if !self.pools.contains_key(data_source_id) {
            let source_conf = self
                .conf
                .sources
                .iter()
                .find(|i| i.id == data_source_id)
                .unwrap();

            let connector = source_conf.r#type.parse::<Connectors>()?;
            let config = connector.parse_options(source_conf.options.clone())?;
            let pool = connector.create_connection_pool(self.conf, data_source_id, config)?;

            self.pools.insert(data_source_id.to_string(), pool);
        }

        let pool = self.pools.get_mut(data_source_id).unwrap();

        pool.acquire()
    }
}

/// Handles connections back from postgres
pub struct FdwServer {
    /// The ansilo app config
    conf: &'static NodeConfig,
    /// The connection pools indexed by their data source ids
    pools: HashMap<String, ConnectionPools>,
    /// Mapping of data source ids to their respective socket paths
    paths: HashMap<String, PathBuf>,
    /// Listener threads
    threads: Vec<JoinHandle<()>>,
}

impl FdwServer {
    /// Starts a new server instance listening at the specified path
    pub fn start(
        conf: &'static NodeConfig,
        path: PathBuf,
        pools: HashMap<String, ConnectionPools>,
    ) -> Result<Self> {
        let paths = Self::create_paths(path.as_path(), &pools);
        let threads = Self::start_threads(conf, &paths, &pools)?;

        Ok(Self {
            conf,
            paths,
            pools,
            threads,
        })
    }

    /// Gets the mapping of data source ids to their paths
    pub fn paths(&self) -> &HashMap<String, PathBuf> {
        &self.paths
    }

    /// Waits for all listener threads complete
    pub fn wait(self) -> Result<()> {
        for thread in self.threads.into_iter() {
            if let Err(_) = thread.join() {
                bail!("Error occurred while waiting for thread")
            }
        }

        Ok(())
    }

    fn create_paths(path: &Path, pools: &HashMap<String, ConnectionPools>) -> HashMap<String, PathBuf> {
        pools
            .iter()
            .map(|(id, _)| (id.to_owned(), path.join(id).join(".sock")))
            .collect()
    }

    fn start_threads(
        conf: &'static NodeConfig,
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
                let res = FdwServer::listen(conf, &path, pool);

                if let Err(err) = res {
                    error!("FDW Listener error: {}", err);
                }
            }));
        }

        Ok(threads)
    }

    fn listen(conf: &'static NodeConfig, path: &PathBuf, pool: ConnectionPools) -> Result<()> {
        match pool {
            ConnectionPools::OracleJdbc(pool, entities) => {
                FdwListener::<OracleJdbcConnector>::bind(conf, path, pool, entities)?.listen()?
            }
        };

        Ok(())
    }
}

/// Handles connections from postgres, serving data from a connector
pub struct FdwListener<TConnector: Connector> {
    /// The ansilo app config
    conf: &'static NodeConfig,
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
        conf: &'static NodeConfig,
        path: &Path,
        pool: TConnector::TConnectionPool,
        entities: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
    ) -> Result<Self> {
        let listener = UnixListener::bind(path).context("Failed to bind socket")?;

        Ok(Self {
            conf,
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
            let mut fdw_con = FdwConnection::<TConnector>::new(socket, entities, pool);

            if let Err(err) = fdw_con.process() {
                error!("Error while processing FDW connection: {}", err);
            }
        });

        Ok(())
    }
}

/// A single connection from the FDW
struct FdwConnection<TConnector: Connector> {
    /// The unix socket the server listens on
    socket: UnixStream,
    /// Entity config
    entities: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
    /// Connection pool
    pool: TConnector::TConnectionPool,
    /// Connection state
    connection: FdwConnectionState<TConnector>,
    /// Current query state
    query: FdwQueryState<TConnector>,
}

enum FdwConnectionState<TConnector: Connector> {
    New,
    Connected(TConnector::TConnection),
}

enum FdwQueryState<TConnector: Connector> {
    New,
    PlanningSelect(sqlil::Select),
    Prepared(QueryHandleWrite<TConnector::TQueryHandle>),
    Executed(ResultSetRead<TConnector::TResultSet>),
}

impl<TConnector: Connector> FdwConnection<TConnector> {
    fn new(
        socket: UnixStream,
        entities: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
        pool: TConnector::TConnectionPool,
    ) -> Self {
        Self {
            socket,
            entities,
            pool,
            connection: FdwConnectionState::New,
            query: FdwQueryState::New,
        }
    }

    /// Starts the message handler loop
    fn process(&mut self) -> Result<()> {
        let conf = bincode_conf();
        loop {
            let request =
                bincode::decode_from_std_read::<ClientMessage, _, _>(&mut self.socket, conf)
                    .context("Failed to decode message from postgres")?;

            let response = self.handle_message(request);

            let response = match response {
                Ok(response) => response,
                Err(err) => Some(ServerMessage::GenericError(format!("{}", err))),
            };

            let response = match response {
                Some(response) => response,
                None => break,
            };

            bincode::encode_into_std_write(response, &mut self.socket, conf)
                .context("Failed to send response message to postgres")?;
        }

        Ok(())
    }

    fn handle_message(&mut self, message: ClientMessage) -> Result<Option<ServerMessage>> {
        Ok(Some(match message {
            ClientMessage::AuthDataSource(_) => {
                // TODO: implement auth
                self.connect()?;
                ServerMessage::AuthAccepted
            }
            ClientMessage::EstimateSize(entity) => {
                ServerMessage::EstimatedSizeResult(self.estimate_size(&entity)?)
            }
            ClientMessage::Select(select) => {
                ServerMessage::Select(self.handle_select_message(select)?)
            }
            ClientMessage::Prepare => {
                self.prepare()?;
                ServerMessage::QueryPrepared
            }
            ClientMessage::WriteParams(data) => {
                self.write_params(data)?;
                ServerMessage::QueryParamsWritten
            }
            ClientMessage::Execute => {
                self.execute()?;
                ServerMessage::QueryExecuted
            }
            ClientMessage::Read(len) => {
                // TODO: remove copy
                let mut buff = vec![0u8; len as usize];
                let read = self.read(&mut buff[..])?;
                ServerMessage::ResultData(buff[..read].to_vec())
            }
            ClientMessage::Close => return Ok(None),
            ClientMessage::GenericError(err) => bail!("Error received from client: {}", err),
        }))
    }

    fn connect(&mut self) -> Result<()> {
        let con = self.pool.acquire()?;
        self.connection = FdwConnectionState::Connected(con);

        Ok(())
    }

    fn estimate_size(&mut self, entity: &EntityVersionIdentifier) -> Result<EntitySizeEstimate> {
        Ok(TConnector::TQueryPlanner::estimate_size(
            self.connection.get()?,
            self.entities
                .find(entity)
                .context("Failed to find entity with id")?,
        )?)
    }

    fn handle_select_message(
        &mut self,
        select: ClientSelectMessage,
    ) -> Result<ServerSelectMessage> {
        Ok(match select {
            ClientSelectMessage::Create(entity) => {
                ServerSelectMessage::Result(self.create_select(&entity)?)
            }
            ClientSelectMessage::Apply(op) => {
                ServerSelectMessage::Result(self.apply_select_operation(op)?)
            }
            ClientSelectMessage::Estimate(op) => {
                ServerSelectMessage::Result(self.estimate_select_operation(op)?)
            }
        })
    }

    fn create_select(&mut self, entity: &EntityVersionIdentifier) -> Result<QueryOperationResult> {
        let (cost, select) = TConnector::TQueryPlanner::create_base_select(
            self.connection.get()?,
            &self.entities,
            self.entities
                .find(entity)
                .context("Failed to find entity with id")?,
        )?;

        self.query = FdwQueryState::PlanningSelect(select);

        Ok(QueryOperationResult::PerformedRemotely(cost))
    }

    fn apply_select_operation(&mut self, op: SelectQueryOperation) -> Result<QueryOperationResult> {
        let select = self.query.select()?;

        let res = TConnector::TQueryPlanner::apply_select_operation(
            self.connection.get()?,
            &self.entities,
            select,
            op,
        )?;

        Ok(res)
    }

    fn estimate_select_operation(
        &mut self,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult> {
        let select = self.query.select()?;

        let res = TConnector::TQueryPlanner::apply_select_operation(
            self.connection.get()?,
            &self.entities,
            &mut select.clone(),
            op,
        )?;

        Ok(res)
    }

    fn prepare(&mut self) -> Result<()> {
        let select = self.query.select()?;
        let connection = self.connection.get()?;

        let query =
            TConnector::TQueryCompiler::compile_select(connection, &self.entities, select.clone())?;
        let handle = connection.prepare(query)?;

        self.query = FdwQueryState::Prepared(QueryHandleWrite(handle));

        Ok(())
    }

    fn write_params(&mut self, data: Vec<u8>) -> Result<()> {
        let handle = self.query.query_handle()?;

        handle
            .write_all(data.as_slice())
            .context("Failed to write to query handle")?;

        Ok(())
    }

    fn execute(&mut self) -> Result<()> {
        let handle = self.query.query_handle()?;

        let result_set = handle.0.execute()?;

        self.query = FdwQueryState::Executed(ResultSetRead(result_set));
        Ok(())
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        let result_set = self.query.result_set()?;

        let read = result_set
            .read(buff)
            .context("Failed to read from result set")?;

        Ok(read)
    }
}

impl<TConnector: Connector> FdwQueryState<TConnector> {
    fn select(&mut self) -> Result<&mut sqlil::Select> {
        Ok(match self {
            FdwQueryState::PlanningSelect(select) => select,
            _ => bail!("Unexpected query state"),
        })
    }

    fn query_handle(&mut self) -> Result<&mut QueryHandleWrite<TConnector::TQueryHandle>> {
        Ok(match self {
            FdwQueryState::Prepared(handle) => handle,
            _ => bail!("Unexpected query state"),
        })
    }

    fn result_set(&mut self) -> Result<&mut ResultSetRead<TConnector::TResultSet>> {
        Ok(match self {
            FdwQueryState::Executed(result_set) => result_set,
            _ => bail!("Unexpected query state"),
        })
    }
}

impl<TConnector: Connector> FdwConnectionState<TConnector> {
    fn get(&mut self) -> Result<&mut TConnector::TConnection> {
        Ok(match self {
            FdwConnectionState::Connected(c) => c,
            _ => bail!("Unexpected connection state"),
        })
    }
}

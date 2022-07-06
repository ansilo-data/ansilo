use std::{
    collections::HashMap,
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
};

use ansilo_connectors::{
    common::{
        data::{QueryHandleWrite, ResultSetRead},
        entity::ConnectorEntityConfig,
    },
    interface::{
        container::{ConnectionPools, Connections, Connectors, QueryHandles, ResultSets},
        *,
    },
};
use ansilo_core::{
    config::NodeConfig,
    err::{bail, Context, Error, Result},
    sqlil::{self, EntityVersionIdentifier},
};

use super::proto::{ClientMessage, ClientSelectMessage, ServerMessage, ServerSelectMessage};

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
            let pool = connector.create_connection_pool(config, self.conf)?;

            self.pools.insert(data_source_id.to_string(), pool);
        }

        let pool = self.pools.get_mut(data_source_id).unwrap();

        pool.acquire()
    }
}

/// Handles connections from postgres, serving data from our connectors
pub struct PostgresFdwServer {
    /// The ansilo app config
    conf: &'static NodeConfig,
    /// The unix socket the server listens on
    listener: UnixListener,
}

/// A single connection from the FDW
struct FdwConnection<TConnector: Connector> {
    /// The unix socket the server listens on
    socket: UnixStream,
    /// Entity config
    config: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
    /// Current connection to data source
    connection: TConnector::TConnection,
    /// Current connection state
    state: FdwConnectionState<TConnector>,
}

enum FdwConnectionState<TConnector: Connector> {
    New,
    PlanningSelect(sqlil::Select),
    Prepared(QueryHandleWrite<TConnector::TQueryHandle>),
    Executed(ResultSetRead<TConnector::TResultSet>),
}

impl<TConnector: Connector> FdwConnection<TConnector> {
    fn handle_message(&mut self, message: ClientMessage) -> Result<ServerMessage> {
        Ok(match message {
            // TODO:
            ClientMessage::AuthDataSource(_, _) => todo!(),
            // TODO:
            ClientMessage::EstimateSize(_) => todo!(),
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
            ClientMessage::GenericError(err) => bail!("Error received from client: {}", err),
        })
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
            &self.connection,
            &self.config,
            self.config
                .find(entity)
                .context("Failed to find entity with id")?,
        )?;

        self.state = FdwConnectionState::PlanningSelect(select);

        Ok(QueryOperationResult::PerformedRemotely(cost))
    }

    fn apply_select_operation(&mut self, op: SelectQueryOperation) -> Result<QueryOperationResult> {
        let select = self.state.select()?;

        let res = TConnector::TQueryPlanner::apply_select_operation(
            &self.connection,
            &self.config,
            select,
            op,
        )?;

        Ok(res)
    }

    fn estimate_select_operation(
        &mut self,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult> {
        let select = self.state.select()?;

        let res = TConnector::TQueryPlanner::apply_select_operation(
            &self.connection,
            &self.config,
            &mut select.clone(),
            op,
        )?;

        Ok(res)
    }

    fn prepare(&mut self) -> Result<()> {
        let select = self.state.select()?;

        let query = TConnector::TQueryCompiler::compile_select(
            &self.connection,
            &self.config,
            select.clone(),
        )?;
        let handle = self.connection.prepare(query)?;

        self.state = FdwConnectionState::Prepared(QueryHandleWrite(handle));

        Ok(())
    }

    fn write_params(&mut self, data: Vec<u8>) -> Result<()> {
        let handle = self.state.query_handle()?;

        handle
            .write_all(data.as_slice())
            .context("Failed to write to query handle")?;

        Ok(())
    }

    fn execute(&mut self) -> Result<()> {
        let handle = self.state.query_handle()?;

        let result_set = handle.0.execute()?;

        self.state = FdwConnectionState::Executed(ResultSetRead(result_set));
        Ok(())
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        let result_set = self.state.result_set()?;

        let read = result_set
            .read(buff)
            .context("Failed to read from result set")?;

        Ok(read)
    }
}

impl<TConnector: Connector> FdwConnectionState<TConnector> {
    fn select(&mut self) -> Result<&mut sqlil::Select> {
        Ok(match self {
            FdwConnectionState::PlanningSelect(select) => select,
            _ => bail!("Unexpected connection state"),
        })
    }

    fn query_handle(&mut self) -> Result<&mut QueryHandleWrite<TConnector::TQueryHandle>> {
        Ok(match self {
            FdwConnectionState::Prepared(handle) => handle,
            _ => bail!("Unexpected connection state"),
        })
    }

    fn result_set(&mut self) -> Result<&mut ResultSetRead<TConnector::TResultSet>> {
        Ok(match self {
            FdwConnectionState::Executed(result_set) => result_set,
            _ => bail!("Unexpected connection state"),
        })
    }
}

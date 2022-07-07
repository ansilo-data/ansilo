use std::io::{Read, Write};

use ansilo_connectors::{
    common::{
        data::{QueryHandleWrite, ResultSetRead},
        entity::ConnectorEntityConfig,
    },
    interface::*,
};
use ansilo_core::{
    err::{bail, Context, Result},
    sqlil::{self, EntityVersionIdentifier},
};

use super::{
    channel::IpcServerChannel,
    proto::{ClientMessage, ClientSelectMessage, ServerMessage, ServerSelectMessage},
};

/// A single connection from the FDW
pub(crate) struct FdwConnection<TConnector: Connector> {
    /// The unix socket the server listens on
    chan: Option<IpcServerChannel>,
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
    pub(crate) fn new(
        chan: IpcServerChannel,
        entities: ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
        pool: TConnector::TConnectionPool,
    ) -> Self {
        Self {
            chan: Some(chan),
            entities,
            pool,
            connection: FdwConnectionState::New,
            query: FdwQueryState::New,
        }
    }

    /// Starts the message handler loop
    pub(crate) fn process(&mut self) -> Result<()> {
        let mut chan = self.chan.take().context("Channel already used")?;

        loop {
            let res = chan.recv(|request| {
                let response = self.handle_message(request);

                let response = match response {
                    Ok(response) => response,
                    Err(err) => Some(ServerMessage::GenericError(format!("{}", err))),
                };

                Ok(response)
            })?;

            if res.is_none() {
                break;
            }
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

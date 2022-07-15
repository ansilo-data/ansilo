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
use ansilo_logging::warn;

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
            ClientMessage::Execute => ServerMessage::QueryExecuted(self.execute()?),
            ClientMessage::Read(len) => {
                // TODO: remove copy
                let mut buff = vec![0u8; len as usize];
                let read = self.read(&mut buff[..])?;
                ServerMessage::ResultData(buff[..read].to_vec())
            }
            ClientMessage::Close => return Ok(None),
            ClientMessage::GenericError(err) => bail!("Error received from client: {}", err),
            _ => {
                warn!("Received unexpected message from client: {:?}", message);
                ServerMessage::GenericError("Invalid message received".to_string())
            }
        }))
    }

    fn connect(&mut self) -> Result<()> {
        let con = self.pool.acquire()?;
        self.connection = FdwConnectionState::Connected(con);

        Ok(())
    }

    fn estimate_size(&mut self, entity: &EntityVersionIdentifier) -> Result<OperationCost> {
        self.connect()?;
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
        self.connect()?;
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

    fn execute(&mut self) -> Result<RowStructure> {
        let handle = self.query.query_handle()?;

        let result_set = handle.0.execute()?;
        let row_structure = result_set.get_structure()?;

        self.query = FdwQueryState::Executed(ResultSetRead(result_set));
        Ok(row_structure)
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

#[cfg(test)]
mod tests {
    use std::{
        io,
        thread::{self, JoinHandle},
    };

    use ansilo_connectors::{
        common::{data::DataReader, entity::EntitySource},
        memory::{MemoryConnectionConfig, MemoryConnectionPool, MemoryConnector},
    };
    use ansilo_core::{
        common::data::{DataType, DataValue},
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig, NodeConfig},
    };

    use crate::fdw::{
        channel::IpcClientChannel, proto::AuthDataSource, test::create_tmp_ipc_channel,
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

    fn create_mock_connection(name: &'static str) -> (JoinHandle<Result<()>>, IpcClientChannel) {
        let (entities, pool) = create_memory_connection_pool();

        let (client_chan, server_chan) = create_tmp_ipc_channel(name);

        let thread = thread::spawn(move || {
            let mut fdw = FdwConnection::<MemoryConnector>::new(server_chan, entities, pool);

            fdw.process()
        });

        (thread, client_chan)
    }

    #[test]
    fn test_fdw_connection_estimate_size() {
        let (thread, mut client) = create_mock_connection("connection_estimate_size");

        let res = client
            .send(ClientMessage::EstimateSize(sqlil::entity("people", "1.0")))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::EstimatedSizeResult(OperationCost::new(Some(3), None, None, None))
        );

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_estimate_size_unknown_entity() {
        let (thread, mut client) =
            create_mock_connection("connection_estimate_size_unknown_entity");

        let res = client
            .send(ClientMessage::EstimateSize(sqlil::entity("unknown", "1.0")))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::GenericError("Failed to find entity with id".to_string())
        );

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_select() {
        let (thread, mut client) = create_mock_connection("connection_select");

        let res = client
            .send(ClientMessage::Select(ClientSelectMessage::Create(
                sqlil::entity("people", "1.0"),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::new(Some(3), None, None, None))
            ))
        );

        let res = client
            .send(ClientMessage::Select(ClientSelectMessage::Apply(
                SelectQueryOperation::AddColumn((
                    "first_name".into(),
                    sqlil::Expr::attr("people", "1.0", "first_name"),
                )),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Select(ClientSelectMessage::Estimate(
                SelectQueryOperation::SetRowOffset(5),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client.send(ClientMessage::Prepare).unwrap();
        assert_eq!(res, ServerMessage::QueryPrepared);

        let res = client.send(ClientMessage::Execute).unwrap();
        let row_structure = RowStructure::new(vec![("first_name".into(), DataType::rust_string())]);
        assert_eq!(res, ServerMessage::QueryExecuted(row_structure.clone()));

        let res = client.send(ClientMessage::Read(1024)).unwrap();
        let data = match res {
            ServerMessage::ResultData(data) => data,
            _ => unreachable!("Unexpected response {:?}", res),
        };

        let mut result_data = DataReader::new(io::Cursor::new(data), row_structure.types());

        assert_eq!(
            result_data.read_data_value().unwrap(),
            Some(DataValue::from("Mary"))
        );
        assert_eq!(
            result_data.read_data_value().unwrap(),
            Some(DataValue::from("John"))
        );
        assert_eq!(
            result_data.read_data_value().unwrap(),
            Some(DataValue::from("Gary"))
        );
        assert_eq!(result_data.read_data_value().unwrap(), None);

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_execute_without_query() {
        let (thread, mut client) = create_mock_connection("connection_execute_without_auth");

        let res = client.send(ClientMessage::Execute).unwrap();

        assert_eq!(
            res,
            ServerMessage::GenericError("Unexpected query state".into())
        );

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_unexpected_message() {
        let (thread, mut client) = create_mock_connection("unexpected_message");

        let res = client
            .send(ClientMessage::AuthDataSource(AuthDataSource::new(
                "TOKEN",
                "DATA_SOURCE",
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::GenericError("Invalid message received".into())
        );

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }
}

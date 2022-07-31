use std::{
    fmt::Display,
    io::{Read, Write},
    mem,
};

use ansilo_connectors::{
    common::{
        data::{QueryHandleWrite, ResultSetRead},
        entity::{ConnectorEntityConfig, EntitySource},
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
    proto::{
        ClientDeleteMessage, ClientInsertMessage, ClientMessage, ClientSelectMessage,
        ClientUpdateMessage, ServerDeleteMessage, ServerInsertMessage, ServerMessage,
        ServerSelectMessage, ServerUpdateMessage,
    },
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
    Planning(sqlil::Query),
    Prepared(QueryHandleWrite<TConnector::TQueryHandle>),
    Executed(
        QueryHandleWrite<TConnector::TQueryHandle>,
        ResultSetRead<TConnector::TResultSet>,
    ),
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
            ClientMessage::Insert(insert) => {
                ServerMessage::Insert(self.handle_insert_message(insert)?)
            }
            ClientMessage::Update(update) => {
                ServerMessage::Update(self.handle_update_message(update)?)
            }
            ClientMessage::Delete(delete) => {
                ServerMessage::Delete(self.handle_delete_message(delete)?)
            }
            ClientMessage::Explain(verbose) => {
                ServerMessage::ExplainResult(self.explain_query(verbose)?)
            }
            ClientMessage::Prepare => {
                let structure = self.prepare()?;
                ServerMessage::QueryPrepared(structure)
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
            ClientMessage::RestartQuery => {
                self.restart_query()?;
                ServerMessage::QueryRestarted
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
            Self::get_entity_config(&self.entities, entity)?,
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
        })
    }

    fn create_select(&mut self, source: &sqlil::EntitySource) -> Result<QueryOperationResult> {
        self.connect()?;
        let (cost, select) = TConnector::TQueryPlanner::create_base_select(
            self.connection.get()?,
            &self.entities,
            Self::get_entity_config(&self.entities, &source.entity)?,
            source,
        )?;

        self.query = FdwQueryState::Planning(select.into());

        Ok(QueryOperationResult::PerformedRemotely(cost))
    }

    fn apply_select_operation(&mut self, op: SelectQueryOperation) -> Result<QueryOperationResult> {
        let select = self
            .query
            .current()?
            .as_select_mut()
            .context("Current query is not SELECT")?;

        let res = TConnector::TQueryPlanner::apply_select_operation(
            self.connection.get()?,
            &self.entities,
            select,
            op,
        )?;

        Ok(res)
    }

    fn handle_insert_message(
        &mut self,
        insert: ClientInsertMessage,
    ) -> Result<ServerInsertMessage> {
        Ok(match insert {
            ClientInsertMessage::Create(entity) => {
                ServerInsertMessage::Result(self.create_insert(&entity)?)
            }
            ClientInsertMessage::Apply(op) => {
                ServerInsertMessage::Result(self.apply_insert_operation(op)?)
            }
        })
    }

    fn create_insert(&mut self, target: &sqlil::EntitySource) -> Result<QueryOperationResult> {
        self.connect()?;
        let (cost, insert) = TConnector::TQueryPlanner::create_base_insert(
            self.connection.get()?,
            &self.entities,
            Self::get_entity_config(&self.entities, &target.entity)?,
            target,
        )?;

        self.query = FdwQueryState::Planning(insert.into());

        Ok(QueryOperationResult::PerformedRemotely(cost))
    }

    fn apply_insert_operation(&mut self, op: InsertQueryOperation) -> Result<QueryOperationResult> {
        let insert = self
            .query
            .current()?
            .as_insert_mut()
            .context("Current query is not INSERT")?;

        let res = TConnector::TQueryPlanner::apply_insert_operation(
            self.connection.get()?,
            &self.entities,
            insert,
            op,
        )?;

        Ok(res)
    }

    fn handle_update_message(
        &mut self,
        update: ClientUpdateMessage,
    ) -> Result<ServerUpdateMessage> {
        Ok(match update {
            ClientUpdateMessage::Create(entity) => {
                ServerUpdateMessage::Result(self.create_update(&entity)?)
            }
            ClientUpdateMessage::Apply(op) => {
                ServerUpdateMessage::Result(self.apply_update_operation(op)?)
            }
        })
    }

    fn create_update(&mut self, target: &sqlil::EntitySource) -> Result<QueryOperationResult> {
        self.connect()?;
        let (cost, update) = TConnector::TQueryPlanner::create_base_update(
            self.connection.get()?,
            &self.entities,
            Self::get_entity_config(&self.entities, &target.entity)?,
            target,
        )?;

        self.query = FdwQueryState::Planning(update.into());

        Ok(QueryOperationResult::PerformedRemotely(cost))
    }

    fn apply_update_operation(&mut self, op: UpdateQueryOperation) -> Result<QueryOperationResult> {
        let update = self
            .query
            .current()?
            .as_update_mut()
            .context("Current query is not INSERT")?;

        let res = TConnector::TQueryPlanner::apply_update_operation(
            self.connection.get()?,
            &self.entities,
            update,
            op,
        )?;

        Ok(res)
    }

    fn handle_delete_message(
        &mut self,
        delete: ClientDeleteMessage,
    ) -> Result<ServerDeleteMessage> {
        Ok(match delete {
            ClientDeleteMessage::Create(entity) => {
                ServerDeleteMessage::Result(self.create_delete(&entity)?)
            }
            ClientDeleteMessage::Apply(op) => {
                ServerDeleteMessage::Result(self.apply_delete_operation(op)?)
            }
        })
    }

    fn create_delete(&mut self, target: &sqlil::EntitySource) -> Result<QueryOperationResult> {
        self.connect()?;
        let (cost, delete) = TConnector::TQueryPlanner::create_base_delete(
            self.connection.get()?,
            &self.entities,
            Self::get_entity_config(&self.entities, &target.entity)?,
            target,
        )?;

        self.query = FdwQueryState::Planning(delete.into());

        Ok(QueryOperationResult::PerformedRemotely(cost))
    }

    fn apply_delete_operation(&mut self, op: DeleteQueryOperation) -> Result<QueryOperationResult> {
        let delete = self
            .query
            .current()?
            .as_delete_mut()
            .context("Current query is not DELETE")?;

        let res = TConnector::TQueryPlanner::apply_delete_operation(
            self.connection.get()?,
            &self.entities,
            delete,
            op,
        )?;

        Ok(res)
    }

    fn prepare(&mut self) -> Result<QueryInputStructure> {
        let query = self.query.current()?;
        let connection = self.connection.get()?;

        let query =
            TConnector::TQueryCompiler::compile_query(connection, &self.entities, query.clone())?;
        let handle = connection.prepare(query)?;

        let structure = handle.get_structure()?;
        self.query = FdwQueryState::Prepared(QueryHandleWrite(handle));

        Ok(structure)
    }

    fn write_params(&mut self, data: Vec<u8>) -> Result<()> {
        let handle = self.query.query_handle()?;

        handle
            .write_all(data.as_slice())
            .context("Failed to write to query handle")?;

        Ok(())
    }

    fn execute(&mut self) -> Result<RowStructure> {
        let query = mem::replace(&mut self.query, FdwQueryState::New);
        let mut handle = match query {
            FdwQueryState::Prepared(handle) => handle,
            _ => bail!(
                "Failed to execute query: expecting query state to be 'prepared' found {}",
                query
            ),
        };

        let result_set = handle.0.execute()?;
        let row_structure = result_set.get_structure()?;

        self.query = FdwQueryState::Executed(handle, ResultSetRead(result_set));
        Ok(row_structure)
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        let result_set = self.query.result_set()?;

        let read = result_set
            .read(buff)
            .context("Failed to read from result set")?;

        Ok(read)
    }

    fn restart_query(&mut self) -> Result<()> {
        let query = mem::replace(&mut self.query, FdwQueryState::New);

        self.query = match query {
            FdwQueryState::Executed(mut handle, _) => {
                handle.0.restart()?;
                FdwQueryState::Prepared(handle)
            }
            _ => bail!(
                "Failed to restart query: expecting query state to be 'executed' found {}",
                query
            ),
        };

        Ok(())
    }

    fn explain_query(&mut self, verbose: bool) -> Result<String> {
        let res = TConnector::TQueryPlanner::explain_query(
            self.connection.get()?,
            &self.entities,
            &*self.query.current()?,
            verbose,
        )?;

        let json = serde_json::to_string(&res).context("Failed to encode explain state to JSON")?;

        Ok(json)
    }

    fn get_entity_config<'a, 'b>(
        entities: &'a ConnectorEntityConfig<TConnector::TEntitySourceConfig>,
        entity: &'b EntityVersionIdentifier,
    ) -> Result<&'a EntitySource<TConnector::TEntitySourceConfig>> {
        entities
            .find(entity)
            .context("Failed to find entity with id")
    }
}

impl<TConnector: Connector> FdwQueryState<TConnector> {
    fn current(&mut self) -> Result<&mut sqlil::Query> {
        Ok(match self {
            FdwQueryState::Planning(query) => query,
            _ => bail!("Expecting query state to be 'planning' found {}", self),
        })
    }

    fn query_handle(&mut self) -> Result<&mut QueryHandleWrite<TConnector::TQueryHandle>> {
        Ok(match self {
            FdwQueryState::Prepared(handle) => handle,
            _ => bail!("Expecting query state to be 'prepared' found {}", self),
        })
    }

    fn result_set(&mut self) -> Result<&mut ResultSetRead<TConnector::TResultSet>> {
        Ok(match self {
            FdwQueryState::Executed(_, result_set) => result_set,
            _ => bail!("Expecting query state to be 'executed' found {}", self),
        })
    }
}

impl<TConnector: Connector> Display for FdwQueryState<TConnector> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            FdwQueryState::New => "new",
            FdwQueryState::Planning(_) => "planning",
            FdwQueryState::Prepared(_) => "prepared",
            FdwQueryState::Executed(_, _) => "executed",
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
        sync::Arc,
        thread::{self, JoinHandle},
    };

    use ansilo_connectors::{
        common::{data::DataReader, entity::EntitySource},
        memory::{
            MemoryConnectionConfig, MemoryConnectionPool, MemoryConnector,
            MemoryConnectorEntitySourceConfig,
        },
    };
    use ansilo_core::{
        config::{EntityAttributeConfig, EntitySourceConfig, EntityVersionConfig, NodeConfig},
        data::{DataType, DataValue},
    };

    use crate::fdw::{
        channel::IpcClientChannel, proto::AuthDataSource, test::create_tmp_ipc_channel,
    };

    use super::*;

    fn create_memory_connection_pool() -> (
        ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        MemoryConnectionPool,
    ) {
        let conf = MemoryConnectionConfig::new();
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

    fn create_mock_connection(
        name: &'static str,
    ) -> (
        JoinHandle<Result<Arc<MemoryConnectionConfig>>>,
        IpcClientChannel,
    ) {
        let (entities, pool) = create_memory_connection_pool();

        let (client_chan, server_chan) = create_tmp_ipc_channel(name);

        let thread = thread::spawn(move || {
            let mut fdw = FdwConnection::<MemoryConnector>::new(server_chan, entities, pool);

            fdw.process()?;

            Ok(fdw.pool.conf())
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
                sqlil::source("people", "1.0", "people"),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Select(ClientSelectMessage::Apply(
                SelectQueryOperation::AddColumn((
                    "first_name".into(),
                    sqlil::Expr::attr("people", "first_name"),
                )),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client.send(ClientMessage::Prepare).unwrap();
        assert_eq!(
            res,
            ServerMessage::QueryPrepared(QueryInputStructure::new(vec![]))
        );

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

        assert!(matches!(res, ServerMessage::GenericError(_)));

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

    #[test]
    fn test_fdw_connection_select_with_restart_query() {
        let (thread, mut client) = create_mock_connection("connection_select");

        let res = client
            .send(ClientMessage::Select(ClientSelectMessage::Create(
                sqlil::source("people", "1.0", "people"),
            )))
            .unwrap();

        assert!(matches!(res, ServerMessage::Select(_)));

        let res = client
            .send(ClientMessage::Select(ClientSelectMessage::Apply(
                SelectQueryOperation::AddColumn((
                    "first_name".into(),
                    sqlil::Expr::attr("people", "first_name"),
                )),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client.send(ClientMessage::Prepare).unwrap();
        assert_eq!(
            res,
            ServerMessage::QueryPrepared(QueryInputStructure::new(vec![]))
        );

        for _ in 1..3 {
            let res = client.send(ClientMessage::Execute).unwrap();
            let row_structure =
                RowStructure::new(vec![("first_name".into(), DataType::rust_string())]);
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

            let res = client.send(ClientMessage::RestartQuery).unwrap();
            assert_eq!(res, ServerMessage::QueryRestarted);
        }

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_explain_select() {
        let (thread, mut client) = create_mock_connection("connection_select_explain");

        let res = client
            .send(ClientMessage::Select(ClientSelectMessage::Create(
                sqlil::source("people", "1.0", "people"),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Select(ClientSelectMessage::Apply(
                SelectQueryOperation::AddColumn((
                    "first_name".into(),
                    sqlil::Expr::attr("people", "first_name"),
                )),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client.send(ClientMessage::Explain(true)).unwrap();

        let json = match res {
            ServerMessage::ExplainResult(res) => res,
            _ => panic!("Unexpected response from server: {:?}", res),
        };

        let _parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_insert() {
        let (thread, mut client) = create_mock_connection("connection_insert");

        let res = client
            .send(ClientMessage::Insert(ClientInsertMessage::Create(
                sqlil::source("people", "1.0", "people"),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Insert(ServerInsertMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Insert(ClientInsertMessage::Apply(
                InsertQueryOperation::AddColumn((
                    "first_name".into(),
                    sqlil::Expr::constant(DataValue::from("New")),
                )),
            )))
            .unwrap();

        let res = client
            .send(ClientMessage::Insert(ClientInsertMessage::Apply(
                InsertQueryOperation::AddColumn((
                    "last_name".into(),
                    sqlil::Expr::constant(DataValue::from("Man")),
                )),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Insert(ServerInsertMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client.send(ClientMessage::Prepare).unwrap();
        assert_eq!(
            res,
            ServerMessage::QueryPrepared(QueryInputStructure::new(vec![]))
        );

        let res = client.send(ClientMessage::Execute).unwrap();
        assert_eq!(res, ServerMessage::QueryExecuted(RowStructure::new(vec![])));

        client.close().unwrap();
        let entities = thread.join().unwrap().unwrap();

        // Assert row was actually inserted
        entities
            .with_data("people", "1.0", |rows| {
                assert_eq!(
                    rows.iter().last().unwrap().clone(),
                    vec![DataValue::from("New"), DataValue::from("Man")]
                );
            })
            .unwrap();
    }

    #[test]
    fn test_fdw_connection_update() {
        let (thread, mut client) = create_mock_connection("connection_update");

        let res = client
            .send(ClientMessage::Update(ClientUpdateMessage::Create(
                sqlil::source("people", "1.0", "people"),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Update(ServerUpdateMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Update(ClientUpdateMessage::Apply(
                UpdateQueryOperation::AddSet((
                    "first_name".into(),
                    sqlil::Expr::constant(DataValue::from("Updated")),
                )),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Update(ServerUpdateMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client.send(ClientMessage::Prepare).unwrap();
        assert_eq!(
            res,
            ServerMessage::QueryPrepared(QueryInputStructure::new(vec![]))
        );

        let res = client.send(ClientMessage::Execute).unwrap();
        assert_eq!(res, ServerMessage::QueryExecuted(RowStructure::new(vec![])));

        client.close().unwrap();
        let entities = thread.join().unwrap().unwrap();

        // Assert rows were all updated
        entities
            .with_data("people", "1.0", |rows| {
                assert_eq!(
                    rows,
                    &vec![
                        vec![DataValue::from("Updated"), DataValue::from("Jane")],
                        vec![DataValue::from("Updated"), DataValue::from("Smith")],
                        vec![DataValue::from("Updated"), DataValue::from("Gregson")],
                    ]
                );
            })
            .unwrap();
    }

    #[test]
    fn test_fdw_connection_delete() {
        let (thread, mut client) = create_mock_connection("connection_delete");

        let res = client
            .send(ClientMessage::Delete(ClientDeleteMessage::Create(
                sqlil::source("people", "1.0", "people"),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Delete(ServerDeleteMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Delete(ClientDeleteMessage::Apply(
                DeleteQueryOperation::AddWhere(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::attr("people", "first_name"),
                    sqlil::BinaryOpType::Equal,
                    sqlil::Expr::constant(DataValue::from("John")),
                ))),
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Delete(ServerDeleteMessage::Result(
                QueryOperationResult::PerformedRemotely(OperationCost::default())
            ))
        );

        let res = client.send(ClientMessage::Prepare).unwrap();
        assert_eq!(
            res,
            ServerMessage::QueryPrepared(QueryInputStructure::new(vec![]))
        );

        let res = client.send(ClientMessage::Execute).unwrap();
        assert_eq!(res, ServerMessage::QueryExecuted(RowStructure::new(vec![])));

        client.close().unwrap();
        let entities = thread.join().unwrap().unwrap();

        // Assert row was deleted
        entities
            .with_data("people", "1.0", |rows| {
                assert_eq!(
                    rows,
                    &vec![
                        vec![DataValue::from("Mary"), DataValue::from("Jane")],
                        vec![DataValue::from("Gary"), DataValue::from("Gregson")],
                    ]
                );
            })
            .unwrap();
    }
}

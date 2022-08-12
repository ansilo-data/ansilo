use std::{
    collections::HashMap,
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
    data::DataType,
    err::{bail, Context, Result},
    sqlil::{self, EntityVersionIdentifier},
};
use ansilo_logging::warn;

use super::{
    channel::IpcServerChannel,
    proto::{ClientMessage, ClientQueryMessage, QueryId, ServerMessage, ServerQueryMessage},
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
    /// Current query states
    queries: HashMap<QueryId, FdwQueryState<TConnector>>,
    /// Current query id counter
    query_id: QueryId,
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
            queries: HashMap::new(),
            query_id: 0,
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
                    Err(err) => Some(ServerMessage::Error(format!("{}", err))),
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
            ClientMessage::GetRowIds(entity) => {
                ServerMessage::RowIds(self.get_row_id_exprs(&entity)?)
            }
            ClientMessage::CreateQuery(entity, query_type) => {
                let (query_id, cost) = self.create_query(&entity, query_type)?;
                ServerMessage::QueryCreated(query_id, cost)
            }
            ClientMessage::Query(query_id, message) => {
                ServerMessage::Query(self.handle_query_message(query_id, message)?)
            }
            ClientMessage::BeginTransaction => self.begin_transaction()?,
            ClientMessage::RollbackTransaction => self.rollback_transaction()?,
            ClientMessage::CommitTransaction => self.commit_transaction()?,
            ClientMessage::Close => return Ok(None),
            ClientMessage::Error(err) => bail!("Error received from client: {}", err),
            _ => {
                warn!("Received unexpected message from client: {:?}", message);
                ServerMessage::Error("Invalid message received".to_string())
            }
        }))
    }

    fn handle_query_message(
        &mut self,
        query_id: u32,
        message: ClientQueryMessage,
    ) -> Result<ServerQueryMessage> {
        Ok(match message {
            ClientQueryMessage::Apply(op) => {
                ServerQueryMessage::OperationResult(self.apply_query_operation(query_id, op)?)
            }
            ClientQueryMessage::Explain(verbose) => {
                ServerQueryMessage::Explained(self.explain_query(query_id, verbose)?)
            }
            ClientQueryMessage::Prepare => {
                let structure = self.prepare(query_id)?;
                ServerQueryMessage::Prepared(structure)
            }
            ClientQueryMessage::WriteParams(data) => {
                self.write_params(query_id, data)?;
                ServerQueryMessage::ParamsWritten
            }
            ClientQueryMessage::Execute => ServerQueryMessage::Executed(self.execute(query_id)?),
            ClientQueryMessage::Read(len) => {
                // TODO: remove copy
                let mut buff = vec![0u8; len as usize];
                let read = self.read(query_id, &mut buff[..])?;
                ServerQueryMessage::ResultData(buff[..read].to_vec())
            }
            ClientQueryMessage::Restart => {
                self.restart_query(query_id)?;
                ServerQueryMessage::Restarted
            }
            ClientQueryMessage::Duplicate => {
                let new_id = self.duplicate_query(query_id)?;
                ServerQueryMessage::Duplicated(new_id)
            }
            ClientQueryMessage::Discard => {
                self.queries.remove(&query_id).context("Invalid query id")?;
                ServerQueryMessage::Discarded
            }
        })
    }

    fn connect(&mut self) -> Result<()> {
        if let FdwConnectionState::New = &self.connection {
            let con = self.pool.acquire()?;
            self.connection = FdwConnectionState::Connected(con);
        }

        Ok(())
    }

    fn query(
        queries: &mut HashMap<QueryId, FdwQueryState<TConnector>>,
        query_id: QueryId,
    ) -> Result<&mut FdwQueryState<TConnector>> {
        queries.get_mut(&query_id).context("Invalid query id")
    }

    fn estimate_size(&mut self, entity: &EntityVersionIdentifier) -> Result<OperationCost> {
        self.connect()?;
        Ok(TConnector::TQueryPlanner::estimate_size(
            self.connection.get()?,
            Self::get_entity_config(&self.entities, entity)?,
        )?)
    }

    fn get_row_id_exprs(
        &mut self,
        source: &sqlil::EntitySource,
    ) -> Result<Vec<(sqlil::Expr, DataType)>> {
        self.connect()?;
        let res = TConnector::TQueryPlanner::get_row_id_exprs(
            self.connection.get()?,
            &self.entities,
            Self::get_entity_config(&self.entities, &source.entity)?,
            source,
        )?;

        Ok(res)
    }

    fn create_query(
        &mut self,
        source: &sqlil::EntitySource,
        r#type: sqlil::QueryType,
    ) -> Result<(QueryId, OperationCost)> {
        self.connect()?;
        let (cost, query) = TConnector::TQueryPlanner::create_base_query(
            self.connection.get()?,
            &self.entities,
            Self::get_entity_config(&self.entities, &source.entity)?,
            source,
            r#type,
        )?;

        let query_id = self.query_id;
        self.queries
            .insert(query_id, FdwQueryState::Planning(query));
        self.query_id += 1;

        Ok((query_id, cost))
    }

    fn apply_query_operation(
        &mut self,
        query_id: QueryId,
        op: QueryOperation,
    ) -> Result<QueryOperationResult> {
        match op {
            QueryOperation::Select(op) => self.apply_select_operation(query_id, op),
            QueryOperation::Insert(op) => self.apply_insert_operation(query_id, op),
            QueryOperation::Update(op) => self.apply_update_operation(query_id, op),
            QueryOperation::Delete(op) => self.apply_delete_operation(query_id, op),
        }
    }

    fn apply_select_operation(
        &mut self,
        query_id: QueryId,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult> {
        let select = Self::query(&mut self.queries, query_id)?
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

    fn apply_insert_operation(
        &mut self,
        query_id: QueryId,
        op: InsertQueryOperation,
    ) -> Result<QueryOperationResult> {
        let insert = Self::query(&mut self.queries, query_id)?
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

    fn apply_update_operation(
        &mut self,
        query_id: QueryId,
        op: UpdateQueryOperation,
    ) -> Result<QueryOperationResult> {
        let update = Self::query(&mut self.queries, query_id)?
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

    fn apply_delete_operation(
        &mut self,
        query_id: QueryId,
        op: DeleteQueryOperation,
    ) -> Result<QueryOperationResult> {
        let delete = Self::query(&mut self.queries, query_id)?
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

    fn prepare(&mut self, query_id: QueryId) -> Result<QueryInputStructure> {
        let query = Self::query(&mut self.queries, query_id)?.current()?;
        let connection = self.connection.get()?;

        let query =
            TConnector::TQueryCompiler::compile_query(connection, &self.entities, query.clone())?;
        let handle = connection.prepare(query)?;

        let structure = handle.get_structure()?;
        *Self::query(&mut self.queries, query_id)? =
            FdwQueryState::Prepared(QueryHandleWrite(handle));

        Ok(structure)
    }

    fn write_params(&mut self, query_id: QueryId, data: Vec<u8>) -> Result<()> {
        let handle = Self::query(&mut self.queries, query_id)?.query_handle()?;

        handle
            .write_all(data.as_slice())
            .context("Failed to write to query handle")?;

        Ok(())
    }

    fn execute(&mut self, query_id: QueryId) -> Result<RowStructure> {
        let query = mem::replace(
            Self::query(&mut self.queries, query_id)?,
            FdwQueryState::New,
        );
        let mut handle = match query {
            FdwQueryState::Prepared(handle) => handle,
            _ => bail!(
                "Failed to execute query: expecting query state to be 'prepared' found {}",
                query
            ),
        };

        let result_set = handle.0.execute()?;
        let row_structure = result_set.get_structure()?;

        *Self::query(&mut self.queries, query_id)? =
            FdwQueryState::Executed(handle, ResultSetRead(result_set));
        Ok(row_structure)
    }

    fn read(&mut self, query_id: QueryId, buff: &mut [u8]) -> Result<usize> {
        let result_set = Self::query(&mut self.queries, query_id)?.result_set()?;

        let read = result_set
            .read(buff)
            .context("Failed to read from result set")?;

        Ok(read)
    }

    fn restart_query(&mut self, query_id: QueryId) -> Result<()> {
        let query = mem::replace(
            Self::query(&mut self.queries, query_id)?,
            FdwQueryState::New,
        );

        *Self::query(&mut self.queries, query_id)? = match query {
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

    fn explain_query(&mut self, query_id: QueryId, verbose: bool) -> Result<String> {
        let res = TConnector::TQueryPlanner::explain_query(
            self.connection.get()?,
            &self.entities,
            Self::query(&mut self.queries, query_id)?.current()?,
            verbose,
        )?;

        let json = serde_json::to_string(&res).context("Failed to encode explain state to JSON")?;

        Ok(json)
    }

    fn duplicate_query(&mut self, query_id: u32) -> Result<QueryId> {
        let cloned = match Self::query(&mut self.queries, query_id)? {
            FdwQueryState::New => FdwQueryState::New,
            FdwQueryState::Planning(state) => FdwQueryState::Planning(state.clone()),
            _ => bail!("Duplicating query is only valid for new or planning states"),
        };

        let query_id = self.query_id;
        self.queries.insert(query_id, cloned);
        self.query_id += 1;

        Ok(query_id)
    }

    fn with_transaction_manager(
        &mut self,
        cb: impl FnOnce(&mut TConnector::TTransactionManager) -> Result<ServerMessage>,
    ) -> Result<ServerMessage> {
        self.connect()?;
        let tm = match self.connection.get()?.transaction_manager() {
            Some(tm) => tm,
            None => return Ok(ServerMessage::TransactionsNotSupported),
        };

        cb(tm)
    }

    fn begin_transaction(&mut self) -> Result<ServerMessage> {
        self.with_transaction_manager(|tm| {
            tm.begin_transaction()?;
            Ok(ServerMessage::TransactionBegun)
        })
    }

    fn rollback_transaction(&mut self) -> Result<ServerMessage> {
        self.with_transaction_manager(|tm| {
            tm.rollback_transaction()?;
            Ok(ServerMessage::TransactionRolledBack)
        })
    }

    fn commit_transaction(&mut self) -> Result<ServerMessage> {
        self.with_transaction_manager(|tm| {
            tm.commit_transaction()?;
            Ok(ServerMessage::TransactionCommitted)
        })
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
            MemoryConnectionPool, MemoryConnector, MemoryConnectorEntitySourceConfig,
            MemoryDatabase, MemoryDatabaseConf,
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

    fn create_memory_connection_pool(
        db_conf: MemoryDatabaseConf,
    ) -> (
        ConnectorEntityConfig<MemoryConnectorEntitySourceConfig>,
        MemoryConnectionPool,
    ) {
        let data = MemoryDatabase::new();
        data.update_conf(move |conf| *conf = db_conf);
        let mut conf = ConnectorEntityConfig::new();

        conf.add(EntitySource::minimal(
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

        data.set_data(
            "people",
            "1.0",
            vec![
                vec![DataValue::from("Mary"), DataValue::from("Jane")],
                vec![DataValue::from("John"), DataValue::from("Smith")],
                vec![DataValue::from("Gary"), DataValue::from("Gregson")],
            ],
        );

        let pool =
            MemoryConnector::create_connection_pool(data, &NodeConfig::default(), &conf).unwrap();

        (conf, pool)
    }

    fn create_mock_connection_opts(
        name: &'static str,
        db_conf: MemoryDatabaseConf,
    ) -> (JoinHandle<Result<Arc<MemoryDatabase>>>, IpcClientChannel) {
        let (entities, pool) = create_memory_connection_pool(db_conf);

        let (client_chan, server_chan) = create_tmp_ipc_channel(name);

        let thread = thread::spawn(move || {
            let mut fdw = FdwConnection::<MemoryConnector>::new(server_chan, entities, pool);

            fdw.process()?;

            Ok(fdw.pool.conf())
        });

        (thread, client_chan)
    }

    fn create_mock_connection(
        name: &'static str,
    ) -> (JoinHandle<Result<Arc<MemoryDatabase>>>, IpcClientChannel) {
        create_mock_connection_opts(name, MemoryDatabaseConf::default())
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
            ServerMessage::Error("Failed to find entity with id".to_string())
        );

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_select() {
        let (thread, mut client) = create_mock_connection("connection_select");

        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Select,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(
                0,
                ClientQueryMessage::Apply(
                    SelectQueryOperation::AddColumn((
                        "first_name".into(),
                        sqlil::Expr::attr("people", "first_name"),
                    ))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::OperationResult(
                QueryOperationResult::Ok(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Prepare))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                vec![]
            )))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Execute))
            .unwrap();
        let row_structure = RowStructure::new(vec![("first_name".into(), DataType::rust_string())]);
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Executed(row_structure.clone()))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Read(1024)))
            .unwrap();
        let data = match res {
            ServerMessage::Query(ServerQueryMessage::ResultData(data)) => data,
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

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Execute))
            .unwrap();

        assert!(matches!(res, ServerMessage::Error(_)));

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

        assert_eq!(res, ServerMessage::Error("Invalid message received".into()));

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_select_with_restart_query() {
        let (thread, mut client) = create_mock_connection("connection_select");

        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Select,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(
                0,
                ClientQueryMessage::Apply(
                    SelectQueryOperation::AddColumn((
                        "first_name".into(),
                        sqlil::Expr::attr("people", "first_name"),
                    ))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::OperationResult(
                QueryOperationResult::Ok(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Prepare))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                vec![]
            )))
        );

        for _ in 1..3 {
            let res = client
                .send(ClientMessage::Query(0, ClientQueryMessage::Execute))
                .unwrap();
            let row_structure =
                RowStructure::new(vec![("first_name".into(), DataType::rust_string())]);
            assert_eq!(
                res,
                ServerMessage::Query(ServerQueryMessage::Executed(row_structure.clone()))
            );

            let res = client
                .send(ClientMessage::Query(0, ClientQueryMessage::Read(1024)))
                .unwrap();
            let data = match res {
                ServerMessage::Query(ServerQueryMessage::ResultData(data)) => data,
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

            let res = client
                .send(ClientMessage::Query(0, ClientQueryMessage::Restart))
                .unwrap();
            assert_eq!(res, ServerMessage::Query(ServerQueryMessage::Restarted));
        }

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_explain_select() {
        let (thread, mut client) = create_mock_connection("connection_select_explain");

        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Select,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(
                0,
                ClientQueryMessage::Apply(
                    SelectQueryOperation::AddColumn((
                        "first_name".into(),
                        sqlil::Expr::attr("people", "first_name"),
                    ))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::OperationResult(
                QueryOperationResult::Ok(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Explain(true)))
            .unwrap();

        let json = match res {
            ServerMessage::Query(ServerQueryMessage::Explained(res)) => res,
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
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Insert,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(
                0,
                ClientQueryMessage::Apply(
                    InsertQueryOperation::AddColumn((
                        "first_name".into(),
                        sqlil::Expr::constant(DataValue::from("New")),
                    ))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::OperationResult(
                QueryOperationResult::Ok(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Query(
                0,
                ClientQueryMessage::Apply(
                    InsertQueryOperation::AddColumn((
                        "last_name".into(),
                        sqlil::Expr::constant(DataValue::from("Man")),
                    ))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::OperationResult(
                QueryOperationResult::Ok(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Prepare))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                vec![]
            )))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Execute))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Executed(RowStructure::new(vec![])))
        );

        client.close().unwrap();
        let entities = thread.join().unwrap().unwrap();

        // Assert row was actually inserted
        let rows = entities.get_data("people", "1.0").unwrap();
        assert_eq!(
            rows.iter().last().unwrap().clone(),
            vec![DataValue::from("New"), DataValue::from("Man")]
        );
    }

    #[test]
    fn test_fdw_connection_update() {
        let (thread, mut client) = create_mock_connection("connection_update");

        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Update,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(
                0,
                ClientQueryMessage::Apply(
                    UpdateQueryOperation::AddSet((
                        "first_name".into(),
                        sqlil::Expr::constant(DataValue::from("Updated")),
                    ))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::OperationResult(
                QueryOperationResult::Ok(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Prepare))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                vec![]
            )))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Execute))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Executed(RowStructure::new(vec![])))
        );

        client.close().unwrap();
        let entities = thread.join().unwrap().unwrap();

        // Assert rows were all updated
        let rows = entities.get_data("people", "1.0").unwrap();
        assert_eq!(
            rows,
            vec![
                vec![DataValue::from("Updated"), DataValue::from("Jane")],
                vec![DataValue::from("Updated"), DataValue::from("Smith")],
                vec![DataValue::from("Updated"), DataValue::from("Gregson")],
            ]
        );
    }

    #[test]
    fn test_fdw_connection_delete() {
        let (thread, mut client) = create_mock_connection("connection_delete");

        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Delete,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(
                0,
                ClientQueryMessage::Apply(
                    DeleteQueryOperation::AddWhere(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                        sqlil::Expr::attr("people", "first_name"),
                        sqlil::BinaryOpType::Equal,
                        sqlil::Expr::constant(DataValue::from("John")),
                    )))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::OperationResult(
                QueryOperationResult::Ok(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Prepare))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                vec![]
            )))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Execute))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Executed(RowStructure::new(vec![])))
        );

        client.close().unwrap();
        let entities = thread.join().unwrap().unwrap();

        // Assert row was deleted
        let rows = entities.get_data("people", "1.0").unwrap();

        assert_eq!(
            rows,
            vec![
                vec![DataValue::from("Mary"), DataValue::from("Jane")],
                vec![DataValue::from("Gary"), DataValue::from("Gregson")],
            ]
        );
    }

    #[test]
    fn test_fdw_connection_get_row_ids_exprs() {
        let (thread, mut client) = create_mock_connection("connection_get_row_ids");

        let res = client
            .send(ClientMessage::GetRowIds(sqlil::source(
                "people", "1.0", "people",
            )))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::RowIds(vec![(
                sqlil::Expr::attr("people", "ROWIDX"),
                DataType::UInt64
            )])
        );

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_multiple_queries() {
        let (thread, mut client) = create_mock_connection("connection_multiple_queries");

        let queries = (0..5)
            .map(|i| {
                let res = client
                    .send(ClientMessage::CreateQuery(
                        sqlil::source("people", "1.0", "people"),
                        sqlil::QueryType::Select,
                    ))
                    .unwrap();

                assert_eq!(
                    res,
                    ServerMessage::QueryCreated(i, OperationCost::default())
                );

                i
            })
            .collect::<Vec<_>>();

        for query_id in queries.iter().cloned() {
            let res = client
                .send(ClientMessage::Query(
                    query_id,
                    ClientQueryMessage::Apply(
                        SelectQueryOperation::AddColumn((
                            "first_name".into(),
                            sqlil::Expr::attr("people", "first_name"),
                        ))
                        .into(),
                    ),
                ))
                .unwrap();

            assert_eq!(
                res,
                ServerMessage::Query(ServerQueryMessage::OperationResult(
                    QueryOperationResult::Ok(OperationCost::default())
                ))
            );
        }

        for query_id in queries.iter().cloned() {
            let res = client
                .send(ClientMessage::Query(query_id, ClientQueryMessage::Prepare))
                .unwrap();
            assert_eq!(
                res,
                ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                    vec![]
                )))
            );
        }

        for query_id in queries.iter().cloned() {
            for _ in 1..3 {
                let res = client
                    .send(ClientMessage::Query(query_id, ClientQueryMessage::Execute))
                    .unwrap();
                let row_structure =
                    RowStructure::new(vec![("first_name".into(), DataType::rust_string())]);
                assert_eq!(
                    res,
                    ServerMessage::Query(ServerQueryMessage::Executed(row_structure.clone()))
                );

                let res = client
                    .send(ClientMessage::Query(
                        query_id,
                        ClientQueryMessage::Read(1024),
                    ))
                    .unwrap();
                let data = match res {
                    ServerMessage::Query(ServerQueryMessage::ResultData(data)) => data,
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

                let res = client
                    .send(ClientMessage::Query(query_id, ClientQueryMessage::Restart))
                    .unwrap();
                assert_eq!(res, ServerMessage::Query(ServerQueryMessage::Restarted));
            }

            let res = client
                .send(ClientMessage::Query(query_id, ClientQueryMessage::Discard))
                .unwrap();
            match res {
                ServerMessage::Query(ServerQueryMessage::Discarded) => {}
                _ => unreachable!("Unexpected response {:?}", res),
            };
        }

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_invalid_query_id() {
        let (thread, mut client) = create_mock_connection("connection_invalid_query_id");

        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Select,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(
                123,
                ClientQueryMessage::Apply(
                    SelectQueryOperation::AddColumn((
                        "first_name".into(),
                        sqlil::Expr::attr("people", "first_name"),
                    ))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(res, ServerMessage::Error("Invalid query id".into()));

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_duplicate_query() {
        let (thread, mut client) = create_mock_connection("connection_duplicate_query");

        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Select,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(
                0,
                ClientQueryMessage::Apply(
                    SelectQueryOperation::AddColumn((
                        "first_name".into(),
                        sqlil::Expr::attr("people", "first_name"),
                    ))
                    .into(),
                ),
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::OperationResult(
                QueryOperationResult::Ok(OperationCost::default())
            ))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Duplicate))
            .unwrap();

        assert_eq!(res, ServerMessage::Query(ServerQueryMessage::Duplicated(1)));

        let res = client
            .send(ClientMessage::Query(1, ClientQueryMessage::Prepare))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                vec![]
            )))
        );

        let res = client
            .send(ClientMessage::Query(1, ClientQueryMessage::Execute))
            .unwrap();
        let row_structure = RowStructure::new(vec![("first_name".into(), DataType::rust_string())]);
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Executed(row_structure.clone()))
        );

        let res = client
            .send(ClientMessage::Query(1, ClientQueryMessage::Read(1024)))
            .unwrap();
        let data = match res {
            ServerMessage::Query(ServerQueryMessage::ResultData(data)) => data,
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
    fn test_fdw_connection_begin_transaction_when_not_supported() {
        let (thread, mut client) = create_mock_connection_opts(
            "connection_transaction_not_supported",
            MemoryDatabaseConf {
                transactions_enabled: false,
                row_locks_pretend: true,
            },
        );

        let res = client.send(ClientMessage::BeginTransaction).unwrap();

        assert_eq!(res, ServerMessage::TransactionsNotSupported);

        client.close().unwrap();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn test_fdw_connection_begin_transaction_rollback() {
        let (thread, mut client) = create_mock_connection("connection_transaction_rollback");

        let res = client.send(ClientMessage::BeginTransaction).unwrap();

        assert_eq!(res, ServerMessage::TransactionBegun);

        // Trigger DELETE FROM "people:1.0"
        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Delete,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Prepare))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                vec![]
            )))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Execute))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Executed(RowStructure::new(vec![])))
        );

        // Perform rollback

        let res = client.send(ClientMessage::RollbackTransaction).unwrap();
        assert_eq!(res, ServerMessage::TransactionRolledBack);

        client.close().unwrap();
        let entities = thread.join().unwrap().unwrap();

        // Assert delete was rolled back
        let rows = entities.get_data("people", "1.0").unwrap();
        assert_eq!(
            rows,
            vec![
                vec![DataValue::from("Mary"), DataValue::from("Jane")],
                vec![DataValue::from("John"), DataValue::from("Smith")],
                vec![DataValue::from("Gary"), DataValue::from("Gregson")],
            ],
        );
    }

    #[test]
    fn test_fdw_connection_begin_transaction_commit() {
        let (thread, mut client) = create_mock_connection("connection_transaction_rollback");

        let res = client.send(ClientMessage::BeginTransaction).unwrap();

        assert_eq!(res, ServerMessage::TransactionBegun);

        // Trigger DELETE FROM "people:1.0"
        let res = client
            .send(ClientMessage::CreateQuery(
                sqlil::source("people", "1.0", "people"),
                sqlil::QueryType::Delete,
            ))
            .unwrap();

        assert_eq!(
            res,
            ServerMessage::QueryCreated(0, OperationCost::default())
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Prepare))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Prepared(QueryInputStructure::new(
                vec![]
            )))
        );

        let res = client
            .send(ClientMessage::Query(0, ClientQueryMessage::Execute))
            .unwrap();
        assert_eq!(
            res,
            ServerMessage::Query(ServerQueryMessage::Executed(RowStructure::new(vec![])))
        );

        // Perform commit
        let res = client.send(ClientMessage::CommitTransaction).unwrap();
        assert_eq!(res, ServerMessage::TransactionCommitted);

        client.close().unwrap();
        let entities = thread.join().unwrap().unwrap();

        // Assert delete was committed
        let rows = entities.get_data("people", "1.0").unwrap();
        assert_eq!(rows, Vec::<Vec<DataValue>>::new());
    }
}

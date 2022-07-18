use std::{
    cmp,
    collections::HashMap,
    iter::Chain,
    os::unix::net::UnixStream,
    path::Path,
    slice::Iter,
    sync::{Arc, Mutex},
};

use ansilo_core::{
    common::data::DataValue,
    err::{bail, Context, Result},
    sqlil::{self, EntityVersionIdentifier},
};
use ansilo_pg::fdw::{
    channel::IpcClientChannel,
    data::{QueryHandle, QueryHandleWriter, ResultSet, ResultSetReader},
    proto::{
        AuthDataSource, ClientMessage, OperationCost, QueryInputStructure, RowStructure,
        SelectQueryOperation, ServerMessage,
    },
};
use pgx::pg_sys::{self, RestrictInfo};

use crate::sqlil::ConversionContext;

/// Context storage for the FDW stored in the fdw_private field
pub struct FdwContext {
    /// The connection state to ansilo
    pub connection: FdwConnection,
    /// The ID of the data source for this FDW connection
    pub data_source_id: String,
    /// The initial entity of fdw context
    pub entity: EntityVersionIdentifier,
    /// The current query handle writer
    pub query_writer: Option<QueryHandleWriter<FdwQueryHandle>>,
    /// The current result set reader
    pub result_set: Option<ResultSetReader<FdwResultSet>>,
}

#[derive(Clone)]
pub struct FdwQueryHandle {
    /// The connection state to ansilo
    pub connection: FdwConnection,
    /// The query input structure
    pub query_input: QueryInputStructure,
}

#[derive(Clone)]
pub struct FdwResultSet {
    /// The connection state to ansilo
    pub connection: FdwConnection,
    /// The result set output structure
    pub row_structure: RowStructure,
}

impl FdwContext {
    pub fn new(data_source_id: &str, entity: EntityVersionIdentifier) -> Self {
        Self {
            connection: FdwConnection::Disconnected,
            data_source_id: data_source_id.into(),
            entity,
            query_writer: None,
            result_set: None,
        }
    }

    pub fn connect(&mut self, path: &Path, auth: AuthDataSource) -> Result<()> {
        if auth.data_source_id != self.data_source_id {
            bail!("Data source ID mismatch");
        }

        self.connection = self.connection.connect(path, auth)?;

        Ok(())
    }

    pub fn send(&mut self, req: ClientMessage) -> Result<ServerMessage> {
        self.connection.send(req)
    }

    pub fn prepare_query(&mut self) -> Result<QueryInputStructure> {
        let response = self.send(ClientMessage::Prepare)?;

        let query_input = match response {
            ServerMessage::QueryPrepared(structure) => structure,
            _ => bail!("Unexpected response while preparing query"),
        };

        self.query_writer = Some(QueryHandleWriter::new(FdwQueryHandle {
            connection: self.connection.clone(),
            query_input: query_input.clone(),
        })?);

        Ok(query_input)
    }

    pub fn write_query_input(&mut self, data: Vec<DataValue>) -> Result<()> {
        let writer = self.query_writer.as_mut().context("Query not prepared")?;

        // This wont be to inefficient as it is being buffered
        // by an underlying BufWriter
        for val in data.into_iter() {
            writer.write_data_value(val)?;
        }

        Ok(())
    }

    pub fn execute_query(&mut self) -> Result<RowStructure> {
        let writer = self.query_writer.take().context("Query not prepared")?;
        let result_set = writer.inner()?.execute()?;
        let row_structure = result_set.row_structure.clone();

        self.result_set = Some(ResultSetReader::new(result_set)?);

        Ok(row_structure)
    }

    pub fn read_result_data(&mut self) -> Result<Option<DataValue>> {
        let reader = self.result_set.as_mut().context("Query not executed")?;

        reader.read_data_value()
    }

    pub fn restart_query(&mut self) -> Result<()> {
        let response = self.send(ClientMessage::RestartQuery)?;

        match response {
            ServerMessage::QueryRestarted => {}
            _ => bail!("Unexpected response while restarting query"),
        };

        let query_input = self
            .query_writer
            .take()
            .context("Query not prepared")?
            .get_structure()
            .clone();
        self.query_writer = Some(QueryHandleWriter::new(FdwQueryHandle {
            connection: self.connection.clone(),
            query_input: query_input,
        })?);

        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<()> {
        self.connection = self.connection.disconnect()?;

        Ok(())
    }
}

impl QueryHandle for FdwQueryHandle {
    type TResultSet = FdwResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(self.query_input.clone())
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        let response = self
            .connection
            .send(ClientMessage::WriteParams(buff.to_vec()))?;

        match response {
            ServerMessage::QueryParamsWritten => Ok(buff.len()),
            _ => bail!("Unexpected response while writing query params"),
        }
    }

    fn execute(&mut self) -> Result<Self::TResultSet> {
        let response = self.connection.send(ClientMessage::Execute)?;

        match response {
            ServerMessage::QueryExecuted(row_structure) => Ok(FdwResultSet {
                connection: self.connection.clone(),
                row_structure,
            }),
            _ => bail!("Unexpected response while writing executing query"),
        }
    }
}

impl ResultSet for FdwResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        Ok(self.row_structure.clone())
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        let response = self.connection.send(ClientMessage::Read(buff.len() as _))?;

        match response {
            ServerMessage::ResultData(data) => {
                let read = cmp::min(buff.len(), data.len());
                buff[..read].copy_from_slice(&data[..read]);
                Ok(read)
            }
            _ => bail!("Unexpected response while reading result data"),
        }
    }
}

/// Connection state of the FDW back to ansilo
#[derive(Clone)]
pub enum FdwConnection {
    Disconnected,
    Connected(Arc<FdwAuthenticatedConnection>),
}

impl FdwConnection {
    pub fn connect(&mut self, path: &Path, auth: AuthDataSource) -> Result<Self> {
        if let FdwConnection::Connected(_) = &self {
            bail!("Already connected");
        }

        let sock = UnixStream::connect(path)
            .with_context(|| format!("Failed to connect to socket {}", path.display()))?;
        let mut client = IpcClientChannel::new(sock);

        let response = client
            .send(ClientMessage::AuthDataSource(auth.clone()))
            .context("Failed to authenticate")?;

        match response {
            ServerMessage::AuthAccepted => {}
            _ => bail!(
                "Failed to authenticate: unexpected response received from server {:?}",
                response
            ),
        }

        Ok(FdwConnection::Connected(Arc::new(
            FdwAuthenticatedConnection::new(auth.data_source_id, client),
        )))
    }

    pub fn send(&mut self, req: ClientMessage) -> Result<ServerMessage> {
        let con = match &self {
            Self::Disconnected => bail!("Not connected to server"),
            Self::Connected(con) => Arc::clone(con),
        };

        let mut client = match con.client.lock() {
            Ok(c) => c,
            Err(_) => bail!("Failed to lock mutex"),
        };

        client.send(req)
    }

    pub fn disconnect(&mut self) -> Result<Self> {
        {
            let con = match &self {
                Self::Disconnected => bail!("Not connected to server"),
                Self::Connected(con) => Arc::clone(con),
            };

            let mut client = match con.client.lock() {
                Ok(c) => c,
                Err(_) => bail!("Failed to lock mutex"),
            };

            client.close().context("Failed to close connection")?;
        }

        Ok(FdwConnection::Disconnected)
    }
}

impl FdwAuthenticatedConnection {
    fn new(data_source_id: String, client: IpcClientChannel) -> Self {
        Self {
            data_source_id,
            client: Mutex::new(client),
        }
    }
}

pub struct FdwAuthenticatedConnection {
    /// The ID of the ansilo data source for the connection
    pub data_source_id: String,
    /// The IPC client used to communicate with ansilo
    pub client: Mutex<IpcClientChannel>,
}

/// Query-specific state for the FDW
#[derive(Clone, PartialEq)]
pub struct FdwQueryContext {
    /// The type-specific query state
    pub q: FdwQueryType,
    /// The query cost calculation
    pub cost: OperationCost,
    /// Conditions required to be evaluated locally
    pub local_conds: Vec<*mut RestrictInfo>,
    /// Conditions required to be evaluated remotely
    pub remote_conds: Vec<*mut RestrictInfo>,
    /// The conversion context used to track query parameters
    pub cvt: ConversionContext,
}

impl FdwQueryContext {
    pub fn select() -> Self {
        Self {
            q: FdwQueryType::Select(FdwSelectQuery::default()),
            cost: OperationCost::default(),
            local_conds: vec![],
            remote_conds: vec![],
            cvt: ConversionContext::new(),
        }
    }

    pub fn pushdown_safe(&self) -> bool {
        self.q.pushdown_safe()
    }

    pub fn as_select(&self) -> Option<&FdwSelectQuery> {
        match &self.q {
            FdwQueryType::Select(q) => Some(q),
        }
    }

    pub fn as_select_mut(&mut self) -> Option<&mut FdwSelectQuery> {
        match &mut self.q {
            FdwQueryType::Select(q) => Some(q),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FdwQueryType {
    Select(FdwSelectQuery),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct FdwSelectQuery {
    /// The conditions which are performed locally (can't be pushed down)
    pub local_ops: Vec<SelectQueryOperation>,
    /// The conditions which are able to be pushed down to the remote
    pub remote_ops: Vec<SelectQueryOperation>,
    /// The current column alias counter
    col_num: u32,
}

impl FdwQueryType {
    fn pushdown_safe(&self) -> bool {
        match self {
            FdwQueryType::Select(q) => q.local_ops.is_empty(),
        }
    }
}

impl FdwSelectQuery {
    pub(crate) fn all_ops(&self) -> Chain<Iter<SelectQueryOperation>, Iter<SelectQueryOperation>> {
        self.remote_ops.iter().chain(self.local_ops.iter())
    }

    pub(crate) fn new_column_alias(&mut self) -> String {
        let num = self.col_num;
        self.col_num += 1;
        format!("c{num}")
    }

    pub(crate) fn new_column(&mut self, expr: sqlil::Expr) -> SelectQueryOperation {
        SelectQueryOperation::AddColumn((self.new_column_alias(), expr))
    }
}

/// Context storage for the FDW stored in the fdw_private field
#[derive(Clone)]
pub struct FdwScanContext {
    /// The prepared query parameter expr's and their type oid's
    /// Each item is keyed by its parameter id
    pub param_exprs: Option<HashMap<u32, (*mut pg_sys::ExprState, pg_sys::Oid)>>,
    /// The resultant row structure after the query has been executed
    pub row_structure: Option<RowStructure>,
}

impl FdwScanContext {
    pub fn new() -> Self {
        Self {
            param_exprs: None,
            row_structure: None,
        }
    }
}

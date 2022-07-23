use std::{
    cmp,
    os::unix::net::UnixStream,
    path::Path,
    sync::{Arc, Mutex},
};

use ansilo_core::{
    data::DataValue,
    err::{anyhow, bail, Context, Error, Result},
    sqlil::EntityVersionIdentifier,
};
use ansilo_pg::fdw::{
    channel::IpcClientChannel,
    data::{QueryHandle, QueryHandleWriter, ResultSet, ResultSetReader},
    proto::{AuthDataSource, ClientMessage, QueryInputStructure, RowStructure, ServerMessage},
};

/// Context data for query planning
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

/// Connection state of the FDW back to ansilo
#[derive(Clone)]
pub enum FdwConnection {
    Disconnected,
    Connected(Arc<FdwAuthenticatedConnection>),
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
            _ => return Err(unexpected_response(response).context("Preparing query")),
        };

        self.query_writer = Some(QueryHandleWriter::new(FdwQueryHandle {
            connection: self.connection.clone(),
            query_input: query_input.clone(),
        })?);

        Ok(query_input)
    }

    pub fn write_query_input(&mut self, data: Vec<DataValue>) -> Result<()> {
        let writer = self.query_writer.as_mut().context("Query not prepared")?;

        // This wont be too inefficient as it is being buffered
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
            _ => return Err(unexpected_response(response).context("Failed to restart query")),
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

/// TODO[low]: the query handle and result set are agnostic enough to be migrated to ansilo-pg crate
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
            _ => return Err(unexpected_response(response).context("Failed to write query params")),
        }
    }

    fn execute(&mut self) -> Result<Self::TResultSet> {
        let response = self.connection.send(ClientMessage::Execute)?;

        match response {
            ServerMessage::QueryExecuted(row_structure) => Ok(FdwResultSet {
                connection: self.connection.clone(),
                row_structure,
            }),
            _ => return Err(unexpected_response(response).context("Failed to execute query")),
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
            _ => {
                return Err(unexpected_response(response).context("Failed to read from result set"))
            }
        }
    }
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
            _ => return Err(unexpected_response(response).context("Failed to authenticate")),
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

fn unexpected_response(response: ServerMessage) -> Error {
    if let ServerMessage::GenericError(message) = response {
        anyhow!("Error from server: {message}")
    } else {
        anyhow!("Unexpected response {:?}", response)
    }
}
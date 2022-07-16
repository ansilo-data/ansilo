use std::{
    iter::Chain,
    os::unix::net::UnixStream,
    path::Path,
    slice::Iter,
    sync::{Arc, Mutex},
};

use ansilo_core::{
    err::{bail, Context, Result},
    sqlil::{self, EntityVersionIdentifier},
};
use ansilo_pg::fdw::{
    channel::IpcClientChannel,
    proto::{AuthDataSource, ClientMessage, OperationCost, SelectQueryOperation, ServerMessage},
};
use pgx::pg_sys::RestrictInfo;

use crate::sqlil::ConversionContext;

/// Context storage for the FDW stored in the fdw_private field
#[derive(Clone)]
pub struct FdwContext {
    /// The connection state to ansilo
    pub connection: FdwConnection,
    /// The ID of the data source for this FDW connection
    pub data_source_id: String,
    /// The initial entity of fdw context
    pub entity: EntityVersionIdentifier,
}

impl FdwContext {
    pub fn new(data_source_id: &str, entity: EntityVersionIdentifier) -> Self {
        Self {
            connection: FdwConnection::Disconnected,
            data_source_id: data_source_id.into(),
            entity,
        }
    }

    pub fn connect(&mut self, path: &Path, auth: AuthDataSource) -> Result<()> {
        if auth.data_source_id != self.data_source_id {
            bail!("Data source ID mismatch");
        }

        if let FdwConnection::Connected(_) = &self.connection {
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

        self.connection = FdwConnection::Connected(Arc::new(FdwAuthenticatedConnection::new(
            auth.data_source_id,
            client,
        )));

        Ok(())
    }

    pub fn send(&mut self, req: ClientMessage) -> Result<ServerMessage> {
        let con = match &self.connection {
            FdwConnection::Disconnected => bail!("Not connected to server"),
            FdwConnection::Connected(con) => Arc::clone(con),
        };

        let mut client = match con.client.lock() {
            Ok(c) => c,
            Err(_) => bail!("Failed to lock mutex"),
        };

        client.send(req)
    }

    pub fn disconnect(&mut self) -> Result<()> {
        {
            let con = match &self.connection {
                FdwConnection::Disconnected => bail!("Not connected to server"),
                FdwConnection::Connected(con) => Arc::clone(con),
            };

            let mut client = match con.client.lock() {
                Ok(c) => c,
                Err(_) => bail!("Failed to lock mutex"),
            };

            client.close().context("Failed to close connection")?;
        }

        self.connection = FdwConnection::Disconnected;

        Ok(())
    }
}

/// Connection state of the FDW back to ansilo
#[derive(Clone)]
pub enum FdwConnection {
    Disconnected,
    Connected(Arc<FdwAuthenticatedConnection>),
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
    /// The conversion context used to track query parameters
    pub cvt: ConversionContext,
}

impl FdwQueryContext {
    pub fn select() -> Self {
        Self {
            q: FdwQueryType::Select(FdwSelectQuery::default()),
            cost: OperationCost::default(),
            local_conds: vec![],
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
        SelectQueryOperation::AddColumn((self.new_column_alias(expr), expr))
    }
}

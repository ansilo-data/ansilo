use std::{
    os::unix::net::UnixStream,
    path::Path,
    sync::{Arc, Mutex},
};

use ansilo_core::err::{bail, Context, Result};
use ansilo_pg::fdw::{
    channel::IpcClientChannel,
    proto::{AuthDataSource, ClientMessage, OperationCost, SelectQueryOperation, ServerMessage},
};

/// Context storage for the FDW stored in the fdw_private field
#[derive(Clone)]
pub struct FdwContext {
    /// The connection state to ansilo
    pub connection: FdwConnection,
}

impl FdwContext {
    pub fn new() -> Self {
        Self {
            connection: FdwConnection::Disconnected,
        }
    }

    pub fn connect(&mut self, path: &Path, auth: AuthDataSource) -> Result<()> {
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
#[derive(Debug, Clone, PartialEq)]
pub struct FdwQueryContext {
    /// The reason for failing to pushdown the query
    pushdown_failure_reason: Option<String>,
    /// The type-specific query state
    pub q: FdwQueryType,
    /// The query cost calculation
    pub cost: OperationCost,
}

impl FdwQueryContext {
    pub fn select() -> Self {
        Self {
            pushdown_failure_reason: None,
            q: FdwQueryType::Select(FdwSelectQuery::default()),
            cost: OperationCost::default(),
        }
    }

    pub fn pushdown_safe(&self) -> bool {
        self.pushdown_failure_reason.is_none() && self.q.pushdown_safe()
    }

    pub fn mark_pushdown_unsafe(&mut self, reason: impl Into<String>) {
        let _ = self.pushdown_failure_reason.insert(reason.into());
    }

    pub fn as_select(&mut self) -> Option<&mut FdwSelectQuery> {
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
}

impl FdwQueryType {
    fn pushdown_safe(&self) -> bool {
        match self {
            FdwQueryType::Select(q) => q.local_ops.is_empty(),
        }
    }
}

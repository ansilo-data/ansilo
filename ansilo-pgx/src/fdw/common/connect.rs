use std::{
    collections::HashMap,
    os::unix::net::UnixStream,
    sync::{Arc, Mutex, Weak},
};

use ansilo_core::err::{bail, Context, Result};
use ansilo_pg::fdw::{
    channel::IpcClientChannel,
    proto::{AuthDataSource, ClientMessage, ServerMessage},
};

use lazy_static::lazy_static;
use pgx::{
    pg_sys::{DefElem, GetForeignServer, GetForeignTable, Oid},
    *,
};

use crate::{
    auth::ctx::AuthContextState,
    fdw::ctx::{FdwContext, FdwGlobalContext},
    sqlil::get_entity_id_from_foreign_table,
};

use super::ServerOptions;

// We store a global hash map of all active connections present in the session.
// Each connection is unique per data source. This is important when we perform
// modification queries which rely on transactions or locking scoped to a single
// connection.
//
// We dont take special care to remove free'd weak references from the map as we
// assume the number of elements will be small and will be refreshed frequently.
lazy_static! {
    static ref ACTIVE_CONNECTIONS: Mutex<HashMap<String, Weak<FdwIpcConnection>>> =
        Mutex::new(HashMap::new());
}

/// An IPC connection to a data source.
pub struct FdwIpcConnection {
    /// The ID of the ansilo data source for the connection
    pub data_source_id: String,
    /// The IPC client used to communicate with ansilo
    pub client: Mutex<IpcClientChannel>,
}

impl FdwIpcConnection {
    pub fn new(data_source_id: impl Into<String>, client: IpcClientChannel) -> Self {
        Self {
            data_source_id: data_source_id.into(),
            client: Mutex::new(client),
        }
    }

    pub fn send(&self, req: ClientMessage) -> Result<ServerMessage> {
        let mut client = match self.client.lock() {
            Ok(c) => c,
            Err(_) => bail!("Failed to lock mutex"),
        };

        client.send(req)
    }
}

/// When dropped we try to issue a close request to the server
/// so resources can be gracefully cleaned up on the other side
/// of the connection.
impl Drop for FdwIpcConnection {
    fn drop(&mut self) {
        let mut client = match self.client.lock() {
            Ok(c) => c,
            Err(err) => {
                warning!("Failed to lock connection mutex: {:?}", err);
                return;
            }
        };

        if let Err(err) = client.close().context("Failed to close connection") {
            warning!("Failed to close connection: {:?}", err);
        }
    }
}

/// Returns a connection to the data source for the supplied foreign table
pub(crate) unsafe fn connect_table(foreign_table_oid: Oid) -> FdwContext {
    // Look up the foreign table from its relid
    let table = GetForeignTable(foreign_table_oid);

    if table.is_null() {
        panic!("Could not find table with oid: {}", foreign_table_oid);
    }

    // Find the corrosponding entity / version id from the table name
    let entity = get_entity_id_from_foreign_table(foreign_table_oid).unwrap();

    let con = get_server_connection((*table).serverid).unwrap();

    FdwContext::new(con, entity, foreign_table_oid)
}

/// Returns a connection to the data source for the supplied foreign server
pub unsafe fn connect_server(server_oid: Oid) -> FdwGlobalContext {
    FdwGlobalContext::new(get_server_connection(server_oid).unwrap())
}

/// Gets a connection to the data source for the supplied foreign server
unsafe fn get_server_connection(server_oid: Oid) -> Result<Arc<FdwIpcConnection>> {
    // Retrieves the foreign server for the table
    let server = GetForeignServer(server_oid);
    if server.is_null() {
        panic!("Could not find server with oid: {}", server_oid);
    }

    // Parse the options defined on the server, namely the data source id
    let opts = ServerOptions::parse(PgList::<DefElem>::from_pg((*server).options))
        .expect("Failed to parse server options");

    get_connection(opts)
}

/// Gets a connection to the data source for the supplied server options
/// If an existing connection is valid for the supplied data source it will
/// be reused.
unsafe fn get_connection(opts: ServerOptions) -> Result<Arc<FdwIpcConnection>> {
    let mut active = ACTIVE_CONNECTIONS
        .lock()
        .expect("Failed to lock active connections mutex");

    // Try find an existing connection if there is one
    if let Some(con) = active.get(&opts.data_source) {
        if let Some(con) = con.upgrade() {
            return Ok(con);
        }
    }

    // There is no active connection, let's create a new one
    // Connect to ansilo over a unix socket
    let sock = UnixStream::connect(&opts.socket)
        .with_context(|| format!("Failed to connect to socket {}", opts.socket.display()))?;
    let mut client = IpcClientChannel::new(sock);

    // Try authenticated using the current authentication token
    let auth = AuthDataSource::new(
        AuthContextState::get().map(|c| c.context),
        &opts.data_source,
    );
    let response = client
        .send(ClientMessage::AuthDataSource(auth.clone()))
        .context("Failed to authenticate")?;

    match response {
        ServerMessage::AuthAccepted => {}
        _ => bail!("Failed to authenticate: {:?}", response),
    }

    let con = Arc::new(FdwIpcConnection::new(opts.data_source.clone(), client));
    active.insert(opts.data_source.clone(), Arc::downgrade(&con));

    Ok(con)
}

use ansilo_connectors::interface::QueryOperationResult;
use ansilo_core::{sqlil::{EntityVersionIdentifier, self}};
use serde::{Serialize, Deserialize};

/// Protocol messages sent by postgres
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Send authentication token and the data source id
    AuthDataSource(String, String),
    /// Estimates the number of entities from the source
    EstimateSize(EntityVersionIdentifier),
    /// Operations for a SELECT query
    Select(ClientSelectMessage),
    /// Prepares the current query
    Prepare,
    /// Write params to query
    /// TODO[maybe]: Write this to a shared-memory segment to avoid copying
    WriteParams(Vec<u8>),
    /// Execute the current query with the supplied params
    Execute,
    /// Read up to the supplied number of bytes from the query
    Read(u32),
    /// Error occurred with message
    GenericError(String)
}

/// Operations for a SELECT query sent from postgres
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientSelectMessage {
    /// Creates a select query for the supplied entity
    Create(EntityVersionIdentifier),
    /// Add a column to the select query
    AddColumn((String, sqlil::Expr)),
    /// Add Join
    AddJoin(sqlil::Join),
    /// Add group by
    AddGroupBy(sqlil::Expr),
    /// Add order by
    AddOrderBy(sqlil::Ordering),
    /// Set row limit
    SetRowLimit(u64),
    /// Set rows to skip
    SetRowSkip(u64),
    /// Only perform the estimation and dont change the query
    EstimateOnly(Box<ClientSelectMessage>)
}

/// Protocol messages sent by ansilo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Token was accepted
    AuthAccepted,
    /// Token was rejected and error message
    TokenRejected(String),
    /// Select was created
    Select(ServerSelectMessage),
    /// Query params written
    QueryParamsWritten,
    /// The query was prepared
    QueryPrepared,
    /// The query was executed
    QueryExecuted,
    /// Rows returned by the query
    /// TODO[maybe]: Write this to a shared-memory segment to avoid copying
    QueryData(Vec<u8>),
    /// Error occurred with message
    GenericError(String)
}

/// Results for operations on SELECT queries from ansilo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerSelectMessage {
    /// The result of the query operation
    Result(QueryOperationResult)
}

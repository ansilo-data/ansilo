use ansilo_connectors::interface::{
    DeleteQueryOperation, InsertQueryOperation, UpdateQueryOperation,
};
pub use ansilo_connectors::interface::{
    OperationCost, QueryInputStructure, QueryOperationResult, RowStructure, SelectQueryOperation,
};

use ansilo_core::sqlil::{self, EntityVersionIdentifier};
use bincode::{Decode, Encode};

/// Protocol messages sent by postgres
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientMessage {
    /// Send authentication token
    AuthDataSource(AuthDataSource),
    /// Estimates the number of entities from the source
    EstimateSize(EntityVersionIdentifier),
    /// Operations for a SELECT query
    Select(ClientSelectMessage),
    /// Operations for an INSERT query
    Insert(ClientInsertMessage),
    /// Operations for an UPDATE query
    Update(ClientUpdateMessage),
    /// Operations for a DELETE query
    Delete(ClientDeleteMessage),
    /// Returns an explaination of the current query state for debugging purposes in JSON encoding
    /// The boolean flag determines if a more vebose output is requested
    Explain(bool),
    /// Prepares the current query
    Prepare,
    /// Write params to query
    /// TODO[maybe]: Write this to a shared-memory segment to avoid copying
    WriteParams(Vec<u8>),
    /// Execute the current query with the supplied params
    Execute,
    /// Read up to the supplied number of bytes from the query
    Read(u32),
    /// Discard the current result set and ready the query for new params and execution
    RestartQuery,
    /// Instruct the server to close the connection
    Close,
    /// Error occurred with message
    GenericError(String),
}

/// Message sent by the client to initialise the connection
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub struct AuthDataSource {
    /// The authentication token
    pub token: String,
    /// The data source id
    pub data_source_id: String,
}

impl AuthDataSource {
    pub fn new(token: impl Into<String>, data_source_id: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            data_source_id: data_source_id.into(),
        }
    }
}

/// Operations for a SELECT query sent from postgres
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientSelectMessage {
    /// Creates a select query for the supplied entity
    Create(sqlil::EntitySource),
    /// Applys the supplied operation to the select query
    Apply(SelectQueryOperation),
}

/// Operations for an INSERT query sent from postgres
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientInsertMessage {
    /// Creates an insert query for the supplied entity
    Create(sqlil::EntitySource),
    /// Applys the supplied operation to the insert query
    Apply(InsertQueryOperation),
}

/// Operations for an UPDATE query sent from postgres
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientUpdateMessage {
    /// Creates an update query for the supplied entity
    Create(sqlil::EntitySource),
    /// Applys the supplied operation to the update query
    Apply(UpdateQueryOperation),
}

/// Operations for a DELETE query sent from postgres
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientDeleteMessage {
    /// Creates a DELETE query for the supplied entity
    Create(sqlil::EntitySource),
    /// Applys the supplied operation to the delete query
    Apply(DeleteQueryOperation),
}

/// Protocol messages sent by ansilo
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerMessage {
    /// Token was accepted
    AuthAccepted,
    /// Estimated size result
    EstimatedSizeResult(OperationCost),
    /// Select query specific message
    Select(ServerSelectMessage),
    /// Insert query specific message
    Insert(ServerInsertMessage),
    /// Update query specific message
    Update(ServerUpdateMessage),
    /// Delete query specific message
    Delete(ServerDeleteMessage),
    /// The result of the query explaination as a JSON encoded string
    ExplainResult(String),
    /// The query was prepared
    QueryPrepared(QueryInputStructure),
    /// Query params written
    QueryParamsWritten,
    /// The query was executed
    QueryExecuted(RowStructure),
    /// Rows returned by the query
    /// TODO[maybe]: Write this to a shared-memory segment to avoid copying
    ResultData(Vec<u8>),
    /// Query restarted
    QueryRestarted,
    /// Error occurred with message
    GenericError(String),
}

/// Results for operations on SELECT queries from ansilo
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerSelectMessage {
    /// The result of the query operation
    Result(QueryOperationResult),
}

/// Results for operations on INSERT queries from ansilo
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerInsertMessage {
    /// The result of the query operation
    Result(QueryOperationResult),
}

/// Results for operations on UPDATE queries from ansilo
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerUpdateMessage {
    /// The result of the query operation
    Result(QueryOperationResult),
}

/// Results for operations on DELETE queries from ansilo
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerDeleteMessage {
    /// The result of the query operation
    Result(QueryOperationResult),
}

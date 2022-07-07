use ansilo_connectors::interface::{
    EntitySizeEstimate, QueryOperationResult, SelectQueryOperation,
};
use ansilo_core::sqlil::EntityVersionIdentifier;
use bincode::{Decode, Encode};

/// Protocol messages sent by postgres
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientMessage {
    /// Send authentication token
    AuthDataSource(String),
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
    /// Instruct the server to close the connection
    Close,
    /// Error occurred with message
    GenericError(String),
}

/// Operations for a SELECT query sent from postgres
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientSelectMessage {
    /// Creates a select query for the supplied entity
    Create(EntityVersionIdentifier),
    /// Add a column to the select query
    Apply(SelectQueryOperation),
    /// Only perform the estimation and dont change the query
    Estimate(SelectQueryOperation),
}

/// Protocol messages sent by ansilo
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerMessage {
    /// Token was accepted
    AuthAccepted,
    /// Estimated size result
    EstimatedSizeResult(EntitySizeEstimate),
    /// Select query specific message
    Select(ServerSelectMessage),
    /// Query params written
    QueryParamsWritten,
    /// The query was prepared
    QueryPrepared,
    /// The query was executed
    QueryExecuted,
    /// Rows returned by the query
    /// TODO[maybe]: Write this to a shared-memory segment to avoid copying
    ResultData(Vec<u8>),
    /// Error occurred with message
    GenericError(String),
}

/// Results for operations on SELECT queries from ansilo
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerSelectMessage {
    /// The result of the query operation
    Result(QueryOperationResult),
}

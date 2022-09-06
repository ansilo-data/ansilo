pub use ansilo_connectors_base::interface::{
    BulkInsertQueryOperation, DeleteQueryOperation, EntityDiscoverOptions, InsertQueryOperation,
    OperationCost, QueryInputStructure, QueryOperation, QueryOperationResult, RowStructure,
    SelectQueryOperation, UpdateQueryOperation,
};

use ansilo_core::{
    auth::AuthContext,
    config::EntityConfig,
    data::DataType,
    sqlil::{self, EntityId},
};
use bincode::{Decode, Encode};

pub type QueryId = u32;

/// Protocol messages sent by postgres
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientMessage {
    /// Send authentication token
    AuthDataSource(AuthDataSource),
    /// Registers the supplied entity for use within future queries
    RegisterEntity(EntityConfig),
    /// Discovers entities from the remote data source
    DiscoverEntities(EntityDiscoverOptions),
    /// Estimates the number of entities from the source
    EstimateSize(EntityId),
    /// Requests the row id expressions for the entity source
    GetRowIds(sqlil::EntitySource),
    /// Creates a new query
    CreateQuery(sqlil::EntitySource, sqlil::QueryType),
    /// Performs an action on the the specified query
    Query(QueryId, ClientQueryMessage),
    /// Begins a transaction on the remote connection
    BeginTransaction,
    /// Rolls back the current transaction on the remote server
    RollbackTransaction,
    /// Commit's the the transaction on the remote server
    CommitTransaction,
    /// Instruct the server to close the connection
    Close,
    /// Error occurred with message
    Error(String),
    /// Multiple requests to execute serially
    /// If an error occurs during the batch the following requests will not be executed
    Batch(Vec<Self>),
}

/// Protocol messages sent by postgres to operate on a query instance
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ClientQueryMessage {
    /// Applies the supplied operation to the current query
    Apply(QueryOperation),
    /// Returns an explaination of the current query state for debugging purposes in JSON encoding
    /// The boolean flag determines if a more vebose output is requested
    Explain(bool),
    /// Prepares the current query
    Prepare,
    /// Gets the maximum batch size for the current query.
    /// Currently only inserts are supported.
    GetMaxBatchSize,
    /// Write params to query
    /// TODO[maybe]: Write this to a shared-memory segment to avoid copying
    WriteParams(Vec<u8>),
    /// Execute the current query with the supplied params
    Execute,
    /// Read up to the supplied number of bytes from the query
    Read(u32),
    /// Discard the current result set and ready the query for new params and execution
    Restart,
    /// Copies the state of the query to a new query
    Duplicate,
    /// Instructs the server to remove the query instance
    Discard,
}

/// Message sent by the client to initialise the connection
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub struct AuthDataSource {
    /// The authentication context
    pub context: Option<AuthContext>,
    /// The data source id
    pub data_source_id: String,
}

impl AuthDataSource {
    pub fn new(context: Option<AuthContext>, data_source_id: impl Into<String>) -> Self {
        Self {
            context,
            data_source_id: data_source_id.into(),
        }
    }
}

/// Protocol responses sent by ansilo
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerMessage {
    /// Token was accepted
    AuthAccepted,
    /// Entities discovered from the data source
    DiscoveredEntitiesResult(Vec<EntityConfig>),
    /// The supplied entity was registered
    RegisteredEntity,
    /// Estimated size result
    EstimatedSizeResult(OperationCost),
    /// The returned row id expressions
    RowIds(Vec<(sqlil::Expr, DataType)>),
    /// The base query was created
    QueryCreated(QueryId, OperationCost),
    /// The responses from operations on a specific query
    Query(ServerQueryMessage),
    /// Transactions not supported against this data source
    TransactionsNotSupported,
    /// Transaction begun
    TransactionBegun,
    /// Transaction rolled back
    TransactionRolledBack,
    /// Transaction committed
    TransactionCommitted,
    /// Unknown entity error
    UnknownEntity(EntityId),
    /// Error occurred with message
    Error(String),
    /// Responses to a batch of requests
    Batch(Vec<Self>),
}

/// Protocol respones sent by ansilo in regards to a specific query
#[derive(Debug, PartialEq, Clone, Encode, Decode)]
pub enum ServerQueryMessage {
    /// Operation applied
    OperationResult(QueryOperationResult),
    /// The result of the query explaination as a JSON encoded string
    Explained(String),
    /// Returns the maximum batch size for this query
    /// (Currently only inserts are supported)
    MaxBatchSize(u32),
    /// The query was prepared
    Prepared(QueryInputStructure),
    /// Query params written
    ParamsWritten,
    /// The query was executed
    Executed(RowStructure),
    /// Rows returned by the query
    /// TODO[maybe]: Write this to a shared-memory segment to avoid copying
    ResultData(Vec<u8>),
    /// Query restarted
    Restarted,
    /// Query duplicated
    Duplicated(QueryId),
    /// Query removed
    Discarded,
}

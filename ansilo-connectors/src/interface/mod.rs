pub mod boxed;
pub mod container;

use ansilo_core::{
    config::{self, EntityVersionConfig, NodeConfig},
    data::DataType,
    err::Result,
    sqlil as sql,
};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::common::entity::{ConnectorEntityConfig, EntitySource};

/// TODO: transactions
/// TODO: insert / update
/// TODO: custom entity config

/// An ansilo connector
/// A common abstraction over a data sources
pub trait Connector {
    type TConnectionConfig: Clone + Send + 'static;
    type TEntitySourceConfig: Clone + Send + 'static;
    type TConnectionPool: ConnectionPool<TConnection = Self::TConnection>;
    type TConnection: Connection<TQuery = Self::TQuery, TQueryHandle = Self::TQueryHandle>;
    type TEntitySearcher: EntitySearcher<
        TConnection = Self::TConnection,
        TEntitySourceConfig = Self::TEntitySourceConfig,
    >;
    type TEntityValidator: EntityValidator<
        TConnection = Self::TConnection,
        TEntitySourceConfig = Self::TEntitySourceConfig,
    >;
    type TQueryPlanner: QueryPlanner<
        TConnection = Self::TConnection,
        TQuery = Self::TQuery,
        TEntitySourceConfig = Self::TEntitySourceConfig,
    >;
    type TQueryCompiler: QueryCompiler<
        TConnection = Self::TConnection,
        TQuery = Self::TQuery,
        TEntitySourceConfig = Self::TEntitySourceConfig,
    >;
    type TQueryHandle: QueryHandle<TResultSet = Self::TResultSet>;
    type TQuery;
    type TResultSet: ResultSet;

    /// The type of the connector, usually the name of the target platform, eg 'postgres'
    const TYPE: &'static str;

    /// Parses the supplied configuration yaml into the strongly typed Options
    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig>;

    /// Parses the supplied configuration yaml into the strongly typed Options
    fn parse_entity_source_options(options: config::Value) -> Result<Self::TEntitySourceConfig>;

    /// Gets a connection pool instance
    fn create_connection_pool(
        options: Self::TConnectionConfig,
        nc: &NodeConfig,
        entities: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
    ) -> Result<Self::TConnectionPool>;
}

/// Opens a connection to the target data source
pub trait ConnectionPool: Clone + Sized + Send + Sync + 'static {
    type TConnection: Connection;

    /// Acquires a connection to the target data source
    fn acquire(&mut self) -> Result<Self::TConnection>;
}

/// An open connection to a data source
pub trait Connection {
    type TQuery;
    type TQueryHandle: QueryHandle;

    /// Prepares the supplied query
    fn prepare(&self, query: Self::TQuery) -> Result<Self::TQueryHandle>;
}

/// Discovers entity schemas from the data source
pub trait EntitySearcher {
    type TConnection: Connection;
    type TEntitySourceConfig;

    /// Retrieves the list of entities from the target data source
    /// Typlically these entities will have their accessibility set to internal
    fn discover(
        connection: &Self::TConnection,
        nc: &NodeConfig,
    ) -> Result<Vec<EntitySource<Self::TEntitySourceConfig>>>;
}

/// Validates custom entity config
pub trait EntityValidator {
    type TConnection: Connection;
    type TEntitySourceConfig;

    /// Validate the supplied entity config
    fn validate(
        connection: &Self::TConnection,
        entity_version: &EntityVersionConfig,
        nc: &NodeConfig,
    ) -> Result<EntitySource<Self::TEntitySourceConfig>>;
}

/// Select planning operations
#[derive(Debug, PartialEq, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum SelectQueryOperation {
    AddColumn((String, sql::Expr)),
    AddWhere(sql::Expr),
    AddJoin(sql::Join),
    AddGroupBy(sql::Expr),
    AddOrderBy(sql::Ordering),
    SetRowLimit(u64),
    SetRowOffset(u64),
}

impl SelectQueryOperation {
    /// Returns `true` if the select query operation is [`AddColumn`].
    ///
    /// [`AddColumn`]: SelectQueryOperation::AddColumn
    #[must_use]
    pub fn is_add_column(&self) -> bool {
        matches!(self, Self::AddColumn(..))
    }

    /// Returns `true` if the select query operation is [`AddWhere`].
    ///
    /// [`AddWhere`]: SelectQueryOperation::AddWhere
    #[must_use]
    pub fn is_add_where(&self) -> bool {
        matches!(self, Self::AddWhere(..))
    }

    /// Returns `true` if the select query operation is [`AddJoin`].
    ///
    /// [`AddJoin`]: SelectQueryOperation::AddJoin
    #[must_use]
    pub fn is_add_join(&self) -> bool {
        matches!(self, Self::AddJoin(..))
    }

    /// Returns `true` if the select query operation is [`AddGroupBy`].
    ///
    /// [`AddGroupBy`]: SelectQueryOperation::AddGroupBy
    #[must_use]
    pub fn is_add_group_by(&self) -> bool {
        matches!(self, Self::AddGroupBy(..))
    }

    /// Returns `true` if the select query operation is [`AddOrderBy`].
    ///
    /// [`AddOrderBy`]: SelectQueryOperation::AddOrderBy
    #[must_use]
    pub fn is_add_order_by(&self) -> bool {
        matches!(self, Self::AddOrderBy(..))
    }

    /// Returns `true` if the select query operation is [`SetRowLimit`].
    ///
    /// [`SetRowLimit`]: SelectQueryOperation::SetRowLimit
    #[must_use]
    pub fn is_set_row_limit(&self) -> bool {
        matches!(self, Self::SetRowLimit(..))
    }

    /// Returns `true` if the select query operation is [`SetRowOffset`].
    ///
    /// [`SetRowOffset`]: SelectQueryOperation::SetRowOffset
    #[must_use]
    pub fn is_set_row_offset(&self) -> bool {
        matches!(self, Self::SetRowOffset(..))
    }
}

/// The query planner determines if SQLIL queries can be executed remotely
pub trait QueryPlanner {
    type TConnection: Connection;
    type TQuery;
    type TEntitySourceConfig: Clone;

    /// Gets an estimate of the number of rows for the entity
    fn estimate_size(
        connection: &Self::TConnection,
        entity: &EntitySource<Self::TEntitySourceConfig>,
    ) -> Result<OperationCost>;

    /// Creates a base query to select all rows from the entity
    fn create_base_select(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        entity: &EntitySource<Self::TEntitySourceConfig>,
    ) -> Result<(OperationCost, sql::Select)>;

    /// Adds the supplied expr to the query
    fn apply_select_operation(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        select: &mut sql::Select,
        op: SelectQueryOperation,
    ) -> Result<QueryOperationResult>;
}

/// The query compiler compiles SQLIL queries into a format that can be executed by the connector
pub trait QueryCompiler {
    type TConnection: Connection;
    type TQuery;
    type TEntitySourceConfig: Clone;

    /// Compiles the select into a connector-specific query object
    fn compile_select(
        connection: &Self::TConnection,
        conf: &ConnectorEntityConfig<Self::TEntitySourceConfig>,
        select: sql::Select,
    ) -> Result<Self::TQuery>;
}

/// A cost estimate for a query operation
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum QueryOperationResult {
    PerformedRemotely(OperationCost),
    PerformedLocally,
}

/// A cost estimate for a query operation
#[derive(Debug, Default, Clone, PartialEq, Encode, Decode)]
pub struct OperationCost {
    /// The estimated number of rows
    pub rows: Option<u64>,
    /// The estimated average width of each row in bytes
    pub row_width: Option<u32>,
    /// The relative cost factor of opening the connection for this operation
    pub connection_cost: Option<u64>,
    /// The relative cost factor of performing the operation
    pub total_cost: Option<u64>,
}

impl OperationCost {
    pub fn new(
        rows: Option<u64>,
        row_width: Option<u32>,
        connection_cost: Option<u64>,
        total_cost: Option<u64>,
    ) -> Self {
        Self {
            rows,
            row_width,
            connection_cost,
            total_cost,
        }
    }
}

/// A query which is executing
pub trait QueryHandle {
    type TResultSet: ResultSet;

    /// Gets the types of the input expected by the query
    fn get_structure(&self) -> Result<QueryInputStructure>;

    /// Writes query parameter data to the underlying query
    /// Returns the number of bytes written
    fn write(&mut self, buff: &[u8]) -> Result<usize>;

    /// Restarts the query, so new query parameters can be written
    fn restart(&mut self) -> Result<()>;

    /// Executes the supplied query
    fn execute(&mut self) -> Result<Self::TResultSet>;
}

/// The structure of data expected by a query
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct QueryInputStructure {
    /// The list of query parameter ids and their associated data types
    ///
    /// The parameters are to be written to the query in the order they appear in the vector.
    /// A parameter with the same id can appear multiple times.
    pub params: Vec<(u32, DataType)>,
}

impl QueryInputStructure {
    pub fn new(params: Vec<(u32, DataType)>) -> Self {
        Self { params }
    }

    pub fn types(&self) -> Vec<DataType> {
        self.params.iter().map(|(_, t)| t.clone()).collect()
    }
}

/// A result set from an executed query
pub trait ResultSet {
    /// Gets the row structure of the result set
    fn get_structure(&self) -> Result<RowStructure>;

    /// Reads row data from the result set into the supplied slice
    /// Returns the number of bytes read of 0 if no bytes are left to read
    fn read(&mut self, buff: &mut [u8]) -> Result<usize>;
}

/// The structure of a row
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct RowStructure {
    /// The list of named columns in the row with their corrosponding data types
    pub cols: Vec<(String, DataType)>,
}

impl RowStructure {
    pub fn new(cols: Vec<(String, DataType)>) -> Self {
        Self { cols }
    }

    pub fn types(&self) -> Vec<DataType> {
        self.cols.iter().map(|i| i.1.clone()).collect()
    }
}

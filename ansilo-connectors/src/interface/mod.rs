use ansilo_core::{
    common::data::DataType,
    config::{self, EntityVersionConfig, NodeConfig},
    err::Result,
    sqlil as sql,
};

use crate::common::entity::{ConnectorEntityConfig, EntitySource};

/// TODO: transactions
/// TODO: insert / update
/// TODO: custom entity config

/// An ansilo connector
/// A common abstraction over a data sources
pub trait Connector<'a> {
    type TConnectionConfig;
    type TEntitySourceConfig;
    type TConnectionOpener: ConnectionOpener<Self::TConnectionConfig, Self::TConnection>;
    type TConnection: Connection<'a, Self::TQuery, Self::TQueryHandle>;
    type TEntitySearcher: EntitySearcher<Self::TConnection, Self::TEntitySourceConfig>;
    type TEntityValidator: EntityValidator<Self::TConnection, Self::TEntitySourceConfig>;
    type TQueryPlanner: QueryPlanner<Self::TConnection, Self::TQuery, Self::TEntitySourceConfig>;
    type TQueryCompiler: QueryCompiler<Self::TConnection, Self::TQuery, Self::TEntitySourceConfig>;
    type TQueryHandle: QueryHandle<'a, Self::TResultSet>;
    type TQuery: 'a;
    type TResultSet: ResultSet<'a>;

    /// Gets the type of the connector, usually the name of the target platform, eg 'postgres'
    fn r#type() -> &'static str;

    /// Parses the supplied configuration yaml into the strongly typed Options
    fn parse_options(options: config::Value) -> Result<Self::TConnectionConfig>;

    /// Gets a connection opener instance
    fn create_connection_opener(options: &Self::TConnectionConfig) -> Result<Self::TConnectionOpener>;
}

/// Opens a connection to the target data source
pub trait ConnectionOpener<TConnectionConfig, TConnection> {
    /// Opens a connection to the target data source using the supplied options
    fn open(&self, options: TConnectionConfig, nc: &NodeConfig) -> Result<TConnection>;
}

/// An open connection to a data source
pub trait Connection<'a, TQuery, TQueryHandle> {
    /// Prepares the supplied query
    fn prepare(&'a self, query: TQuery) -> Result<TQueryHandle>;
}

/// Discovers entity schemas from the data source
pub trait EntitySearcher<TConnection, TEntitySourceConfig> {
    /// Retrieves the list of entities from the target data source
    /// Typlically these entities will have their accessibility set to internal
    fn discover(
        connection: &TConnection,
        nc: &NodeConfig,
    ) -> Result<Vec<EntitySource<TEntitySourceConfig>>>;
}

/// Validates custom entity config
pub trait EntityValidator<TConnection, TEntitySourceConfig> {
    /// Validate the supplied entity config
    fn validate(
        connection: &TConnection,
        entity_version: &EntityVersionConfig,
        nc: &NodeConfig,
    ) -> Result<EntitySource<TEntitySourceConfig>>;
}

/// The query planner determines if SQLIL queries can be executed remotely
pub trait QueryPlanner<TConnection, TQuery, TEntitySourceConfig> {
    /// Gets an estimate of the number of rows for the entity
    fn estimate_size(
        connection: &TConnection,
        entity: &EntitySource<TEntitySourceConfig>,
    ) -> Result<EntitySizeEstimate>;

    /// Creates a base query to select all rows from the entity
    fn create_base_select(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        entity: &EntitySource<TEntitySourceConfig>,
        select: &mut sql::Select,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied expr to the query
    fn add_col_expr(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        select: &mut sql::Select,
        expr: sql::Expr,
        alias: String,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied where clause
    fn add_where_clause(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        select: &mut sql::Select,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied join clause to the query
    fn add_join(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        select: &mut sql::Select,
        join: sql::Join,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied group by clause to the query
    fn add_group_by(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        select: &mut sql::Select,
        expr: sql::Expr,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied order by clause to the query
    fn add_order_by(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        select: &mut sql::Select,
        ordering: sql::Ordering,
    ) -> Result<QueryOperationResult>;

    /// Sets the number of rows to return
    fn set_row_limit(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        select: &mut sql::Select,
        row_limit: u64,
    ) -> Result<QueryOperationResult>;

    /// Sets the number of rows to skip
    fn set_rows_to_skip(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        select: &mut sql::Select,
        row_skip: u64,
    ) -> Result<QueryOperationResult>;
}

/// The query compiler compiles SQLIL queries into a format that can be executed by the connector
pub trait QueryCompiler<TConnection, TQuery, TEntitySourceConfig> {
    /// Compiles the select into a connector-specific query object
    fn compile_select(
        connection: &TConnection,
        conf: &ConnectorEntityConfig<TEntitySourceConfig>,
        select: sql::Select,
    ) -> Result<TQuery>;
}

/// A size estimate of the entity
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EntitySizeEstimate {
    /// The estimated number of rows
    pub rows: Option<u64>,
    /// The estimated average width of each row in bytes
    pub row_width: Option<u32>,
}

impl EntitySizeEstimate {
    pub fn new(rows: Option<u64>, row_width: Option<u32>) -> Self {
        Self { rows, row_width }
    }
}

/// A cost estimate for a query operation
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOperationResult {
    PerformedRemotely(OperationCost),
    PerformedLocally,
}

/// A cost estimate for a query operation
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OperationCost {
    /// The estimated number of rows
    pub rows: Option<u32>,
    /// The relative cost factor of opening the connection for this operation
    pub connection_cost: Option<u32>,
    /// The relative cost factor of performing the operation
    pub total_cost: Option<u32>,
}

impl OperationCost {
    pub fn new(rows: Option<u32>, connection_cost: Option<u32>, total_cost: Option<u32>) -> Self {
        Self {
            rows,
            connection_cost,
            total_cost,
        }
    }
}

/// A query which is executing
pub trait QueryHandle<'a, TResultSet> {
    /// Gets the types of the input expected by the query
    fn get_structure(&self) -> Result<QueryInputStructure>;

    /// Writes query parameter data to the underlying query
    /// Returns the number of bytes written
    fn write(&mut self, buff: &[u8]) -> Result<usize>;

    /// Executes the supplied query
    fn execute(&mut self) -> Result<TResultSet>;
}

/// The structure of data expected by a query
#[derive(Debug, Clone, PartialEq)]
pub struct QueryInputStructure {
    /// The data type of each query parameter
    pub params: Vec<DataType>,
}

impl QueryInputStructure {
    pub fn new(params: Vec<DataType>) -> Self {
        Self { params }
    }
}

/// A result set from an executed query
pub trait ResultSet<'a> {
    /// Gets the row structure of the result set
    fn get_structure(&self) -> Result<RowStructure>;

    /// Reads row data from the result set into the supplied slice
    /// Returns the number of bytes read of 0 if no bytes are left to read
    fn read(&mut self, buff: &mut [u8]) -> Result<usize>;
}

/// The structure of a row
#[derive(Debug, Clone, PartialEq)]
pub struct RowStructure {
    /// The list of named columns in the row with their corrosponding data types
    pub cols: Vec<(String, DataType)>,
}

impl RowStructure {
    pub fn new(cols: Vec<(String, DataType)>) -> Self {
        Self { cols }
    }
}

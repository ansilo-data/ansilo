use ansilo_core::{
    common::data::DataType,
    config::{self, EntityVersionConfig},
    err::Result,
    sqlil::{
        expr::Expr,
        select::{Join, Ordering, Select},
    },
};

/// TODO: transactions
/// TODO: insert / update
/// TODO: custom entity config

/// An ansilo connector
/// A common abstraction over a data sources
pub trait Connector<
    'a,
    TConnectionConfig,
    TConnectionOpener,
    TConnection,
    TEntitySearcher,
    TEntityValidator,
    TSourceConfig,
    TQueryPlanner,
    TQuery,
    TResultSet,
> where
    TConnectionOpener: ConnectionOpener<TConnectionConfig, TConnection>,
    TConnection: Connection<'a, TQuery, TResultSet>,
    TEntitySearcher: EntitySearcher<TConnection, TSourceConfig>,
    TEntityValidator: EntityValidator<TConnection, TSourceConfig>,
    TQueryPlanner: QueryPlanner<TConnection, TQuery, TSourceConfig>,
    TResultSet: ResultSet<'a>,
    TConnection: 'a,
    TResultSet: 'a,
{
    /// Gets the type of the connector, usually the name of the target platform, eg 'postgres'
    fn r#type() -> &'static str;

    /// Parses the supplied configuration yaml into the strongly typed Options
    fn parse_options(options: config::Value) -> Result<TConnectionConfig>;

    /// Gets a connection opener instance
    fn create_connection_opener(options: TConnectionConfig) -> Result<TConnectionOpener>;

    /// Gets the entity searcher for this data source
    fn create_entity_searcher() -> Result<TEntitySearcher>;

    /// Gets the entity searcher for this data source
    fn create_entity_validator() -> Result<TEntityValidator>;

    /// Gets the query planner for this data source
    fn create_query_planner() -> Result<TQueryPlanner>;
}

/// Metadata about an entity version
#[derive(Debug, Clone, PartialEq)]
pub struct EntityVersionMetadata<TSourceConfig>
where
    TSourceConfig: Sized,
{
    /// The entity config
    pub conf: EntityVersionConfig,
    /// The entity source config
    pub source_config: TSourceConfig,
}

/// Opens a connection to the target data source
pub trait ConnectionOpener<TConnectionConfig, TConnection> {
    /// Opens a connection to the target data source using the supplied options
    fn open(&self, options: TConnectionConfig) -> Result<TConnection>;
}

/// An open connection to a data source
pub trait Connection<'a, TQuery, TResultSet>
{
    /// Executes the supplied query
    fn execute(&'a self, query: TQuery) -> Result<TResultSet>;
}

/// Discovers entity schemas from the data source
pub trait EntitySearcher<TConnection, TSourceConfig> {
    /// Retrieves the list of entities from the target data source
    /// Typlically these entities will have their accessibility set to internal
    fn discover(
        &self,
        connection: &TConnection,
    ) -> Result<Vec<EntityVersionMetadata<TSourceConfig>>>;
}

/// Validates custom entity config
pub trait EntityValidator<TConnection, TSourceConfig> {
    /// Validate the supplied entity config
    fn validate(
        &self,
        connection: &TConnection,
        entity_version: EntityVersionConfig,
    ) -> Result<EntityVersionMetadata<TSourceConfig>>;
}

/// The query planner determines if SQLIL queries can be executed remotely
pub trait QueryPlanner<TConnection, TQuery, TSourceConfig> {
    /// Gets an estimate of the number of rows for the entity
    fn estimate_size(
        &self,
        connection: &TConnection,
        entity_version: EntityVersionMetadata<TSourceConfig>,
    ) -> Result<EntitySizeEstimate>;

    /// Creates a base query to select all rows from the entity
    fn create_base_select(
        &self,
        connection: &TConnection,
        entity: EntityVersionMetadata<TSourceConfig>,
        select: &mut Select,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied expr to the query
    fn add_col_expr(
        &self,
        connection: &TConnection,
        entity: EntityVersionMetadata<TSourceConfig>,
        select: &mut Select,
        expr: Expr,
        alias: String,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied where clause
    fn add_where_clause(
        &self,
        connection: &TConnection,
        select: &mut Select,
        expr: Expr,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied join clause to the query
    fn add_join(
        &self,
        connection: &TConnection,
        select: &mut Select,
        join: Join,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied group by clause to the query
    fn add_group_by(
        &self,
        connection: &TConnection,
        select: &mut Select,
        expr: Expr,
    ) -> Result<QueryOperationResult>;

    /// Adds the supplied order by clause to the query
    fn add_order_by(
        &self,
        connection: &TConnection,
        select: &mut Select,
        ordering: Ordering,
    ) -> Result<QueryOperationResult>;

    /// Sets the number of rows to return
    fn set_row_limit(
        &self,
        connection: &TConnection,
        select: &mut Select,
        row_limit: u64,
    ) -> Result<QueryOperationResult>;

    /// Sets the number of rows to skip
    fn set_rows_to_skip(
        &self,
        connection: &TConnection,
        select: &mut Select,
        row_skip: u64,
    ) -> Result<QueryOperationResult>;

    /// Convert the select into a connector-specific query object
    fn convert(&self, connection: &TConnection, select: &Select) -> Result<TQuery>;
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

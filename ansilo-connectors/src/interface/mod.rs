use ansilo_core::{config::{self, EntityConfig, EntityVersionConfig}, err::Result, sqlil::{select::{Select, Join, Ordering}, expr::Expr}};

type Entity = EntityConfig;
type EntityVersion = EntityVersionConfig;

/// TODO: transactions
/// TODO: insert / update
/// TODO: custom entity config

/// An ansilo connector
/// A common abstraction over a data sources
pub trait Connector<TOptions, TConnection, TEntitySearcher, TQueryPlanner, TQuery>
where TEntitySearcher : EntitySearcher<TConnection>,
TQueryPlanner : QueryPlanner<TQuery>

{
    /// Gets the type of the connector, usually the name of the target platform, eg 'postgres'
    fn r#type() -> &'static str;

    /// Parses the supplied configuration yaml into the strongly typed Options
    fn parse_options(options: config::Value) -> Result<TOptions>;

    /// Opens a connection to the target data source using the supplied options
    fn open_connection(options: TOptions) -> Result<TConnection>;

    /// Gets the entity searcher for this data source
    fn create_entity_searcher() -> Result<TEntitySearcher>;

    /// Gets the query planner for this data source
    fn create_query_planner() -> Result<TQueryPlanner>;
}

/// An open connection to a data source
pub trait Connection<TQuery> {
    /// Executes the supplied query
    fn execute(query: TQuery) -> Result<ResultSet>;
}

/// Discovers entity schemas from the data source
pub trait EntitySearcher<TConnection> {
    /// Retrieves the list of entities from the target data source
    /// Typlically these entities will have their accessibility set to internal
    fn discover(connection: &TConnection) -> Result<Vec<Entity>>;
}

/// The query planner determines if SQLIL queries can be executed remotely
pub trait QueryPlanner<TConnection, TQuery> {
    /// Gets an estimate of the number of rows for the entity
    fn estimate_size(connection: &TConnection, entity: EntityVersion) -> Result<EntitySizeEstimate>;

    /// Creates a base query to select all rows from the entity
    fn create_base_select(connection: &TConnection, entity: EntityVersion, select: &mut Select) -> Result<QueryOperationResult>;

    /// Adds the supplied expr to the query
    fn add_col_expr(connection: &TConnection, entity: EntityVersion, select: &mut Select, expr: Expr, alias: String) -> Result<QueryOperationResult>;

    /// Adds the supplied where clause
    fn add_where_clause(connection: &TConnection, select: &mut Select, expr: Expr) -> Result<QueryOperationResult>;

    /// Adds the supplied join clause to the query
    fn add_join(connection: &TConnection, select: &mut Select, join: Join) -> Result<QueryOperationResult>;

    /// Adds the supplied group by clause to the query
    fn add_group_by(connection: &TConnection, select: &mut Select, expr: Expr) -> Result<QueryOperationResult>;

    /// Adds the supplied order by clause to the query
    fn add_order_by(connection: &TConnection, select: &mut Select, ordering: Ordering) -> Result<QueryOperationResult>;

    /// Sets the number of rows to return
    fn set_row_limit(connection: &TConnection, select: &mut Select, row_limit: u64) -> Result<QueryOperationResult>;

    /// Sets the number of rows to skip
    fn set_rows_to_skip(connection: &TConnection, select: &mut Select, row_skip: u64) -> Result<QueryOperationResult>;
    
    /// Convert the select into a connector-specific query object
    fn convert(connection: &TConnection, select: &Select) -> Result<TQuery>;
}

/// A size estimate of the entity
#[derive(Debug, Clone, PartialEq)]
pub struct EntitySizeEstimate {
    /// The estimated number of rows
    pub rows: Option<u64>,
    /// The estimated average width of each row in bytes
    pub row_width: Option<u32>
}

/// A cost estimate for a query operation
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOperationResult {
    PerformedRemotely(OperationCost),
    PerformedLocally   
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

/// A result set from an executed query
pub trait ResultSet {
    /// TODO
    /// Gets the next record
    // fn next() -> Result<Option<Rex;
}
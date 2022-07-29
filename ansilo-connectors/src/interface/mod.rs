pub mod boxed;
pub mod container;

mod connection;
mod entity_searcher;
mod entity_validator;
mod query;
mod query_compiler;
mod query_planner;
mod result_set;

pub use connection::*;
pub use entity_searcher::*;
pub use entity_validator::*;
pub use query::*;
pub use query_compiler::*;
pub use query_planner::*;
pub use result_set::*;

use ansilo_core::{
    config::{self, NodeConfig},
    err::Result,
};

use crate::common::entity::ConnectorEntityConfig;

/// An ansilo connector
/// A common abstraction over a data source
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

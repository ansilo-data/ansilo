use std::collections::HashMap;

use crate::interface::*;

mod connection;
use ansilo_core::{config, err::Result};
pub use connection::*;
mod data;
pub use data::*;
mod result_set;
pub use result_set::*;
mod query;
pub use query::*;
mod jvm;
pub use jvm::*;

#[cfg(test)]
mod tests;

/// Base connector which relies on a JDBC driver for connecting to
/// the target data source
/// We bridge into a JVM running within the process to start the JDBC driver
pub trait JdbcConnector<
    'a,
    TConnectionConfig,
    TEntitySearcher,
    TEntityValidator,
    TEntitySourceConfig,
    TQueryPlanner,
    TQueryCompiler,
> where
    TConnectionConfig: JdbcConnectionConfig,
    TEntitySearcher: EntitySearcher<JdbcConnection<'a>, TEntitySourceConfig>,
    TEntityValidator: EntityValidator<JdbcConnection<'a>, TEntitySourceConfig>,
    TQueryPlanner: QueryPlanner<JdbcConnection<'a>, JdbcQuery, TEntitySourceConfig>,
    TQueryCompiler: QueryCompiler<JdbcConnection<'a>, JdbcQuery>,
{
    /// Gets the type of the connector, usually the name of the target platform, eg 'postgres'
    fn r#type() -> &'static str;

    /// Parses the supplied configuration yaml into the strongly typed Options
    fn parse_options(options: config::Value) -> Result<TConnectionConfig>;

    /// Gets a connection opener instance
    fn create_connection_opener(options: &TConnectionConfig) -> Result<JdbcConnectionOpener>;

    /// Gets the entity searcher for this data source
    fn create_entity_searcher() -> Result<TEntitySearcher>;

    /// Gets the entity searcher for this data source
    fn create_entity_validator() -> Result<TEntityValidator>;

    /// Gets the query planner for this data source
    fn create_query_planner() -> Result<TQueryPlanner>;

    /// Gets the compiler planner for this data source
    fn create_query_compiler() -> Result<TQueryCompiler>;
}

/// Blanket impl for Connector for all impl's JdbcConnector
impl<
        'a,
        TConnectionConfig,
        TEntitySearcher,
        TEntityValidator,
        TEntitySourceConfig,
        TQueryPlanner,
        TQueryCompiler,
        T: JdbcConnector<
            'a,
            TConnectionConfig,
            TEntitySearcher,
            TEntityValidator,
            TEntitySourceConfig,
            TQueryPlanner,
            TQueryCompiler,
        >,
    >
    Connector<
        'a,
        TConnectionConfig,
        JdbcConnectionOpener,
        JdbcConnection<'a>,
        TEntitySearcher,
        TEntityValidator,
        TEntitySourceConfig,
        TQueryPlanner,
        TQueryCompiler,
        JdbcQuery,
        JdbcPreparedQuery<'a>,
        JdbcResultSet<'a>,
    > for T
where
    TConnectionConfig: JdbcConnectionConfig,
    TEntitySearcher: EntitySearcher<JdbcConnection<'a>, TEntitySourceConfig>,
    TEntityValidator: EntityValidator<JdbcConnection<'a>, TEntitySourceConfig>,
    TQueryPlanner: QueryPlanner<JdbcConnection<'a>, JdbcQuery, TEntitySourceConfig>,
    TQueryCompiler: QueryCompiler<JdbcConnection<'a>, JdbcQuery>,
{
    fn r#type() -> &'static str {
        T::r#type()
    }

    fn parse_options(options: config::Value) -> Result<TConnectionConfig> {
        T::parse_options(options)
    }

    fn create_connection_opener(options: &TConnectionConfig) -> Result<JdbcConnectionOpener> {
        T::create_connection_opener(options)
    }

    fn create_entity_searcher() -> Result<TEntitySearcher> {
        T::create_entity_searcher()
    }

    fn create_entity_validator() -> Result<TEntityValidator> {
        T::create_entity_validator()
    }

    fn create_query_planner() -> Result<TQueryPlanner> {
        T::create_query_planner()
    }

    fn create_query_compiler() -> Result<TQueryCompiler> {
        T::create_query_compiler()
    }
}

/// JDBC connection config
pub trait JdbcConnectionConfig {
    /// Gets the JDBC connection URL
    fn get_jdbc_url(&self) -> String;

    /// Gets the connection props
    fn get_jdbc_props(&self) -> HashMap<String, String>;
}

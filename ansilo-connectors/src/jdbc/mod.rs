use std::collections::HashMap;

use crate::interface::*;

mod connection;
pub use connection::*;
mod result_set;
pub use result_set::*;
mod jvm;
pub use jvm::*;

/// Base connector which relies on a JDBC driver for connecting to
/// the target data source
/// We bridge into a JVM running within the process to start the JDBC driver
pub trait JdbcConnector<
    'a,
    TConnectionConfig,
    TEntitySearcher,
    TEntityValidator,
    TSourceConfig,
    TQueryPlanner,
>:
    Connector<'a,
    TConnectionConfig,
    JdbcConnectionOpener,
    JdbcConnection<'a>,
    TEntitySearcher,
    TEntityValidator,
    TSourceConfig,
    TQueryPlanner,
    JdbcQuery,
    JdbcResultSet,
> where
    TConnectionConfig: JdbcConnectionConfig,
    TEntitySearcher: EntitySearcher<JdbcConnection<'a>, TSourceConfig>,
    TEntityValidator: EntityValidator<JdbcConnection<'a>, TSourceConfig>,
    TQueryPlanner: QueryPlanner<JdbcConnection<'a>, JdbcQuery, TSourceConfig>,
{
}

/// JDBC connection config
pub trait JdbcConnectionConfig {
    /// Gets the JDBC connection URL
    fn get_jdbc_url(&self) -> String;

    /// Gets the connection props
    fn get_jdbc_props(&self) -> HashMap<String, String>;
}

/// JDBC query
#[derive(Debug, Clone, PartialEq)]
pub struct JdbcQuery {
    /// The query (likely SQL) as a string
    pub query: String,
    /// Any parameters which need to be bound to the query
    pub params: Vec<JdbcQueryParamData>,
}

impl JdbcQuery {
    pub fn new(query: String) -> Self {
        Self {
            query,
            params: vec![],
        }
    }
}

/// The JDBC query param data
#[derive(Debug, Clone, PartialEq)]
pub enum JdbcQueryParamData {
    String(String),
    Bool(bool),
    Byte(u8),
    Short(u16),
    Float(f64),
    Int(i32),
    Long(i64),
    Null,
    // TODO: date time, big decimal, etc
    // Timestamp(u64),
}

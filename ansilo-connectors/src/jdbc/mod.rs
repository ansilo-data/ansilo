use std::collections::HashMap;
use ansilo_core::common::data::{DataType, DataValue};
use jni::objects::GlobalRef;

use crate::interface::*;

mod connection;
pub use connection::*;
mod data;
pub use data::*;
mod result_set;
pub use result_set::*;
mod query;
pub use query::*;
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
    Connector<
    'a,
    TConnectionConfig,
    JdbcConnectionOpener,
    JdbcConnection<'a>,
    TEntitySearcher,
    TEntityValidator,
    TSourceConfig,
    TQueryPlanner,
    JdbcQuery,
    JdbcPreparedQuery<'a>,
    JdbcResultSet<'a>,
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


// Base connector which relies on a JDBC driver for connecting to
// the target data source
// We bridge into a JVM running within the process to start the JDBC driver

use std::{collections::HashMap, time::Duration};

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
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// JDBC connection config
pub trait JdbcConnectionConfig: Send + Sync + Clone {
    /// Gets the JDBC connection URL
    fn get_jdbc_url(&self) -> String;

    /// Gets the connection props
    fn get_jdbc_props(&self) -> HashMap<String, String>;

    /// Gets the connection pool config
    fn get_pool_config(&self) -> Option<JdbcConnectionPoolConfig>;

    /// Gets the java class name of the connection
    fn get_java_connection(&self) -> String {
        "com.ansilo.connectors.JdbcConnection".into()
    }
}

/// Options for pooling the JDBC connections
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JdbcConnectionPoolConfig {
    /// Minimum number of connections
    min_cons: u32,
    /// Maximum number of connections
    max_cons: u32,
    /// Maximum connection lifetime
    max_lifetime: Option<Duration>,
    /// How long a connection can remain idle before closing
    idle_timeout: Option<Duration>,
    /// Maximum connection timeout
    connect_timeout: Option<Duration>,
}

// Base connector which relies on a JDBC driver for connecting to
// the target data source
// We bridge into a JVM running within the process to start the JDBC driver

use std::collections::HashMap;

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

#[cfg(test)]
mod tests;

/// JDBC connection config
pub trait JdbcConnectionConfig {
    /// Gets the JDBC connection URL
    fn get_jdbc_url(&self) -> String;

    /// Gets the connection props
    fn get_jdbc_props(&self) -> HashMap<String, String>;
}

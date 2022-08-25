// Base connector which relies on a JDBC driver for connecting to
// the target data source
// We bridge into a JVM running within the process to start the JDBC driver

mod conf;
pub use conf::*;
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

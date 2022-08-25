use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};

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

    /// Gets the java class name of class that maps data values to the equivalent JDBC types
    fn get_java_jdbc_data_mapping(&self) -> String {
        "com.ansilo.connectors.mapping.JdbcDataMapping".into()
    }
}

/// Options for pooling the JDBC connections
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JdbcConnectionPoolConfig {
    /// Minimum number of connections
    pub min_cons: u32,
    /// Maximum number of connections
    pub max_cons: u32,
    /// Maximum connection lifetime
    pub max_lifetime: Option<Duration>,
    /// How long a connection can remain idle before closing
    pub idle_timeout: Option<Duration>,
    /// Maximum connection timeout
    pub connect_timeout: Option<Duration>,
}

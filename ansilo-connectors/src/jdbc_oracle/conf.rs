use std::collections::HashMap;

use ansilo_core::{
    config,
    err::{Context, Result},
};
use serde::{Deserialize, Serialize};

use crate::jdbc::JdbcConnectionConfig;

/// The connection config for the Oracle JDBC driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OracleJdbcConnectionConfig {
    pub jdbc_url: String,
    /// @see https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html
    pub properties: HashMap<String, String>,
}

impl JdbcConnectionConfig for OracleJdbcConnectionConfig {
    fn get_jdbc_url(&self) -> String {
        self.jdbc_url.clone()
    }

    fn get_jdbc_props(&self) -> HashMap<String, String> {
        self.properties.clone()
    }
}

impl OracleJdbcConnectionConfig {
    pub fn new(jdbc_url: String, properties: HashMap<String, String>) -> Self {
        Self {
            jdbc_url,
            properties,
        }
    }

    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse connection configuration options")
    }
}

/// Entity source config for Oracle JDBC driver
pub struct OracleJdbcEntitySourceConfig {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_jdbc_parse_connection_options() {
        let conf = config::parse_config(
            r#"
jdbc_url: "JDBC_URL"
properties:
  TEST_PROP: "TEST_PROP_VAL"
"#,
        )
        .unwrap();

        let parsed = OracleJdbcConnectionConfig::parse(conf).unwrap();

        assert_eq!(
            parsed,
            OracleJdbcConnectionConfig {
                jdbc_url: "JDBC_URL".to_string(),
                properties: {
                    let mut map = HashMap::new();
                    map.insert("TEST_PROP".to_string(), "TEST_PROP_VAL".to_string());
                    map
                }
            }
        );
    }
}

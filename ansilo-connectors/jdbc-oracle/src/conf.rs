use std::collections::HashMap;

use ansilo_core::{
    config,
    err::{Context, Result},
};
use serde::{Deserialize, Serialize};

use ansilo_connectors_base::common::entity::ConnectorEntityConfig;
use ansilo_connectors_jdbc_base::{JdbcConnectionConfig, JdbcConnectionPoolConfig};

/// The connection config for the Oracle JDBC driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OracleJdbcConnectionConfig {
    pub jdbc_url: String,
    /// @see https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html
    pub properties: HashMap<String, String>,
    pub pool: Option<JdbcConnectionPoolConfig>,
}

impl JdbcConnectionConfig for OracleJdbcConnectionConfig {
    fn get_jdbc_url(&self) -> String {
        self.jdbc_url.clone()
    }

    fn get_jdbc_props(&self) -> HashMap<String, String> {
        self.properties.clone()
    }

    fn get_pool_config(&self) -> Option<JdbcConnectionPoolConfig> {
        self.pool.clone()
    }

    fn get_java_jdbc_data_mapping(&self) -> String {
        "com.ansilo.connectors.oracle.mapping.OracleJdbcDataMapping".into()
    }
}

impl OracleJdbcConnectionConfig {
    pub fn new(
        jdbc_url: String,
        properties: HashMap<String, String>,
        pool: Option<JdbcConnectionPoolConfig>,
    ) -> Self {
        Self {
            jdbc_url,
            properties,
            pool,
        }
    }

    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse connection configuration options")
    }
}

/// Entity source config for Oracle JDBC driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OracleJdbcEntitySourceConfig {
    Table(OracleJdbcTableOptions),
}

impl OracleJdbcEntitySourceConfig {
    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse entity source configuration options")
    }
}

/// Entity source configuration for mapping an entity to a table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OracleJdbcTableOptions {
    /// The database name
    pub owner_name: Option<String>,
    /// The table name
    pub table_name: String,
    /// Mapping of attributes to their respective column names
    pub attribute_column_map: HashMap<String, String>,
}

impl OracleJdbcTableOptions {
    pub fn new(
        owner_name: Option<String>,
        table_name: String,
        attribute_column_map: HashMap<String, String>,
    ) -> Self {
        Self {
            owner_name,
            table_name,
            attribute_column_map,
        }
    }
}

pub type OracleJdbcConnectorEntityConfig = ConnectorEntityConfig<OracleJdbcEntitySourceConfig>;

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
                },
                pool: None
            }
        );
    }

    #[test]
    fn test_oracle_jdbc_parse_entity_table_options() {
        let conf = config::parse_config(
            r#"
type: "Table"
owner_name: "db"
table_name: "table"
attribute_column_map:
  a: b
  d: c
"#,
        )
        .unwrap();

        let parsed = OracleJdbcEntitySourceConfig::parse(conf).unwrap();

        assert_eq!(
            parsed,
            OracleJdbcEntitySourceConfig::Table(OracleJdbcTableOptions {
                owner_name: Some("db".to_string()),
                table_name: "table".to_string(),
                attribute_column_map: [
                    ("a".to_string(), "b".to_string()),
                    ("d".to_string(), "c".to_string()),
                ]
                .into_iter()
                .collect()
            })
        );
    }
}

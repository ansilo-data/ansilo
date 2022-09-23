use std::collections::HashMap;

use ansilo_core::{
    config,
    err::{Context, Result},
};
use serde::{Deserialize, Serialize};

use ansilo_connectors_base::common::entity::ConnectorEntityConfig;
use ansilo_connectors_jdbc_base::{JdbcConnectionConfig, JdbcConnectionPoolConfig};

/// The connection config for the Teradata JDBC driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeradataJdbcConnectionConfig {
    pub jdbc_url: String,
    /// @see https://docs.teradata.com/en/database/teradata/teradata-database/21/jajdb/teradata/jdbc/TeradataConnection.html
    pub properties: HashMap<String, String>,
    pub pool: Option<JdbcConnectionPoolConfig>,
}

impl JdbcConnectionConfig for TeradataJdbcConnectionConfig {
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
        "com.ansilo.connectors.teradata.mapping.TeradataJdbcDataMapping".into()
    }
}

impl TeradataJdbcConnectionConfig {
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

/// Entity source config for Teradata JDBC driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TeradataJdbcEntitySourceConfig {
    Table(TeradataJdbcTableOptions),
}

impl TeradataJdbcEntitySourceConfig {
    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse entity source configuration options")
    }
}

/// Entity source configuration for mapping an entity to a table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeradataJdbcTableOptions {
    /// The database name
    pub database_name: String,
    /// The table name
    pub table_name: String,
    /// Mapping of attributes to their respective column names
    pub attribute_column_map: HashMap<String, String>,
}

impl TeradataJdbcTableOptions {
    pub fn new(
        database_name: String,
        table_name: String,
        attribute_column_map: HashMap<String, String>,
    ) -> Self {
        Self {
            database_name,
            table_name,
            attribute_column_map,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeradataJdbcSelectQueryOptions {
    /// The select SQL query
    pub query: String,
    /// Mapping of attributes to their respective column names
    pub attribute_column_map: HashMap<String, String>,
}

impl TeradataJdbcSelectQueryOptions {
    pub fn new(query: String, attribute_column_map: HashMap<String, String>) -> Self {
        Self {
            query,
            attribute_column_map,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeradataJdbcModifyQueryOptions {
    /// The insert/update/delete SQL query
    pub query: String,
    /// List of entity attributes the are bound to the query as parameters
    pub attribute_parameter_list: Vec<String>,
}

impl TeradataJdbcModifyQueryOptions {
    pub fn new(query: String, attribute_parameter_list: Vec<String>) -> Self {
        Self {
            query,
            attribute_parameter_list,
        }
    }
}

pub type TeradataJdbcConnectorEntityConfig = ConnectorEntityConfig<TeradataJdbcEntitySourceConfig>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_teradata_jdbc_parse_connection_options() {
        let conf = config::parse_config(
            r#"
jdbc_url: "JDBC_URL"
properties:
  TEST_PROP: "TEST_PROP_VAL"
"#,
        )
        .unwrap();

        let parsed = TeradataJdbcConnectionConfig::parse(conf).unwrap();

        assert_eq!(
            parsed,
            TeradataJdbcConnectionConfig {
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
    fn test_teradata_jdbc_parse_entity_table_options() {
        let conf = config::parse_config(
            r#"
type: "Table"
database_name: "db"
table_name: "table"
attribute_column_map:
  a: b
  d: c
"#,
        )
        .unwrap();

        let parsed = TeradataJdbcEntitySourceConfig::parse(conf).unwrap();

        assert_eq!(
            parsed,
            TeradataJdbcEntitySourceConfig::Table(TeradataJdbcTableOptions {
                database_name: "db".to_string(),
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

use std::collections::HashMap;

use ansilo_core::{
    config,
    err::{Context, Result},
};
use serde::{Deserialize, Serialize};

use crate::{
    common::entity::ConnectorEntityConfig,
    jdbc::{JdbcConnectionConfig, JdbcConnectionPoolConfig},
};

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
pub enum OracleJdbcEntitySourceConfig {
    Table(OracleJdbcTableOptions),
    CustomQueries(OracleJdbcCustomQueryOptions),
}

/// Entity source configuration for mapping an entity to a table
pub struct OracleJdbcTableOptions {
    /// The database name
    pub database_name: Option<String>,
    /// The table name
    pub table_name: String,
    /// Mapping of attributes to their respective column names
    pub attribute_column_name_map: HashMap<String, String>,
}

impl OracleJdbcTableOptions {
    pub fn new(
        database_name: Option<String>,
        table_name: String,
        attribute_column_name_map: HashMap<String, String>,
    ) -> Self {
        Self {
            database_name,
            table_name,
            attribute_column_name_map,
        }
    }
}

/// Entity source configuration for mapping an entity to custom queries
pub struct OracleJdbcCustomQueryOptions {
    /// The select query used to read entities, none if select is not supported
    pub select_query: Option<OracleJdbcSelectQueryOptions>,
    /// The insert query used to create entities, none if insert is not supported
    pub insert_query: Option<OracleJdbcModifyQueryOptions>,
    /// The update query used to update existing entities, none if update is not supported
    pub update_query: Option<OracleJdbcModifyQueryOptions>,
    /// The delete query used to delete entities, none if delete is not supported
    pub delete_query: Option<OracleJdbcModifyQueryOptions>,
}

impl OracleJdbcCustomQueryOptions {
    pub fn new(
        select_query: Option<OracleJdbcSelectQueryOptions>,
        insert_query: Option<OracleJdbcModifyQueryOptions>,
        update_query: Option<OracleJdbcModifyQueryOptions>,
        delete_query: Option<OracleJdbcModifyQueryOptions>,
    ) -> Self {
        Self {
            select_query,
            insert_query,
            update_query,
            delete_query,
        }
    }
}

pub struct OracleJdbcSelectQueryOptions {
    /// The select SQL query
    pub query: String,
    /// Mapping of attributes to their respective column names
    pub attribute_column_name_map: HashMap<String, String>,
}

impl OracleJdbcSelectQueryOptions {
    pub fn new(query: String, attribute_column_name_map: HashMap<String, String>) -> Self {
        Self {
            query,
            attribute_column_name_map,
        }
    }
}

pub struct OracleJdbcModifyQueryOptions {
    /// The insert/update/delete SQL query
    pub query: String,
    /// List of entity attributes the are bound to the query as parameters
    pub attribute_parameter_list: Vec<String>,
}

impl OracleJdbcModifyQueryOptions {
    pub fn new(query: String, attribute_parameter_list: Vec<String>) -> Self {
        Self {
            query,
            attribute_parameter_list,
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
}

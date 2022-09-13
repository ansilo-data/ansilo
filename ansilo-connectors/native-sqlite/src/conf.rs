use std::collections::HashMap;

use ansilo_connectors_base::common::entity::ConnectorEntityConfig;
use ansilo_core::{
    config,
    err::{Context, Result},
};
use enum_as_inner::EnumAsInner;
use serde::{Deserialize, Serialize};

/// The connection config
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SqliteConnectionConfig {
    /// Path to the database file.
    /// Set to ":memory:" for an in-memory db.
    pub path: String,
}

impl SqliteConnectionConfig {
    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse connection configuration options")
    }
}

pub type SqliteConnectorEntityConfig = ConnectorEntityConfig<SqliteEntitySourceConfig>;

/// Entity source config for Sqlite driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, EnumAsInner)]
#[serde(tag = "type")]
pub enum SqliteEntitySourceConfig {
    Table(SqliteTableOptions),
}

impl SqliteEntitySourceConfig {
    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse entity source configuration options")
    }
}

/// Entity source configuration for mapping an entity to a table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqliteTableOptions {
    /// The table name
    pub table_name: String,
    /// Mapping of attributes to their respective column names
    pub attribute_column_map: HashMap<String, String>,
}

impl SqliteTableOptions {
    pub fn new(table_name: String, attribute_column_map: HashMap<String, String>) -> Self {
        Self {
            table_name,
            attribute_column_map,
        }
    }
}

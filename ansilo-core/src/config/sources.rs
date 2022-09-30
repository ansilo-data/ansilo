use serde::{Deserialize, Serialize};

/// Defines a data source
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    /// The ID of the data source
    pub id: String,
    /// The name of the data source
    pub name: Option<String>,
    /// The type of the data source. This is the type of the underlying platform defined
    /// by each connector.
    /// @see ansilo-connectors/*
    pub r#type: String,
    /// The type specific connection options for the data source
    pub options: serde_yaml::Value,
}

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
pub struct MongodbConnectionConfig {
    /// The connection string
    /// @see https://www.mongodb.com/docs/manual/reference/connection-string/
    pub url: String,
}

impl MongodbConnectionConfig {
    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse connection configuration options")
    }
}

pub type MongodbConnectorEntityConfig = ConnectorEntityConfig<MongodbEntitySourceConfig>;

/// Entity source config for Mongodb driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, EnumAsInner)]
#[serde(tag = "type")]
pub enum MongodbEntitySourceConfig {
    Collection(MongodbCollectionOptions),
}

impl MongodbEntitySourceConfig {
    pub fn parse(options: config::Value) -> Result<Self> {
        config::from_value::<Self>(options)
            .context("Failed to parse entity source configuration options")
    }
}

/// Entity source configuration for mapping an entity to a collection
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MongodbCollectionOptions {
    /// The db name
    pub database_name: String,
    /// The collection name
    pub collection_name: String,
}

impl MongodbCollectionOptions {
    pub fn new(database_name: String, collection_name: String) -> Self {
        Self {
            database_name,
            collection_name,
        }
    }
}

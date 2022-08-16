use std::collections::HashMap;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::data::DataType;

/// An entity is a typed and documented dataset to be exposed by this ansilo node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EntityConfig {
    /// The ID of the entity
    pub id: String,
    /// The name of the entity
    pub name: Option<String>,
    /// The description of the entity
    pub description: Option<String>,
    /// The tags attached to the entity for categorisation
    #[serde(default)]
    pub tags: Vec<TagValueConfig>,
    /// The list of attributes exposed by this entity
    pub attributes: Vec<EntityAttributeConfig>,
    /// The list of constraints (fk or unique) on this entity
    #[serde(default)]
    pub constraints: Vec<EntityConstraintConfig>,
    /// The source-specific config for reading or writing to this entity
    pub source: EntitySourceConfig,
}

impl EntityConfig {
    pub fn minimal(
        id: impl Into<String>,
        attrs: Vec<EntityAttributeConfig>,
        source: EntitySourceConfig,
    ) -> Self {
        let id = id.into();

        Self {
            id: id.clone(),
            name: None,
            description: None,
            tags: vec![],
            attributes: attrs,
            constraints: vec![],
            source,
        }
    }
}

/// A tag attached to an entity.
/// These are key-value pairs use for custom categorisation
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TagValueConfig {
    /// The tag key
    pub key: String,
    /// The tag value
    pub value: String,
}

/// An attribute of an entity
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EntityAttributeConfig {
    /// The ID of the attribute
    pub id: String,
    /// A description of the attribute
    pub description: Option<String>,
    /// The data type of the attribute
    pub r#type: DataType,
    /// Whether the attribute is part of the entity's primary key
    #[serde(default)]
    pub primary_key: bool,
    /// Whether the attribute is nullable
    #[serde(default)]
    pub nullable: bool,
}

impl EntityAttributeConfig {
    pub fn minimal(id: impl Into<String>, r#type: DataType) -> Self {
        Self {
            id: id.into(),
            description: None,
            r#type,
            primary_key: false,
            nullable: false,
        }
    }
}

/// A constraint on the entity
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum EntityConstraintConfig {
    ForeignKey(ForeignKeyConstraintConfig),
    Unique(UniqueConstraintConfig),
}

/// A foreign key constraint
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ForeignKeyConstraintConfig {
    /// Foreign entity ID
    pub target_entity_id: String,
    /// Mapping of local attribute names to target attribute names
    pub attribute_map: HashMap<String, String>,
}

/// A unique constraint config
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct UniqueConstraintConfig {
    /// List of local attributes within the unique constraint
    pub attributes: Vec<String>,
}

/// Defines the config used to read and write the entity
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EntitySourceConfig {
    /// The ID of the data source this entity is retrieved from
    pub data_source: String,
    /// The data source specific options for reading/writing to the entity
    pub options: serde_yaml::Value,
}

impl EntitySourceConfig {
    pub fn new(data_source: String, options: serde_yaml::Value) -> Self {
        Self {
            data_source,
            options,
        }
    }

    pub fn minimal(data_source: impl Into<String>) -> Self {
        Self {
            data_source: data_source.into(),
            options: serde_yaml::Value::Null,
        }
    }
}

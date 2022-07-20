use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::data::DataType;

/// An entity is a typed and documented dataset to be exposed by this ansilo node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EntityConfig {
    /// The ID of the entity
    pub id: String,
    /// The name of the entity
    pub name: String,
    /// The description of the entity
    pub description: String,
    /// The tags attached to the entity for categorisation
    pub tags: Vec<TagValueConfig>,
    /// The versions of the entity
    pub versions: Vec<EntityVersionConfig>,
    /// The accessility of the entity
    pub accessibility: EntityAccessiblity,
}

impl EntityConfig {
    pub fn minimal(id: impl Into<String>, versions: Vec<EntityVersionConfig>) -> Self {
        let id: String = id.into();

        Self {
            id: id.clone(),
            name: id,
            description: "".to_string(),
            tags: vec![],
            versions,
            accessibility: EntityAccessiblity::Internal,
        }
    }
}

/// A tag attached to an entity.
/// These are key-value pairs use for custom categorisation
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TagValueConfig {
    /// The tag key
    pub key: String,
    /// The tag value
    pub value: String,
}

/// A version of the entity schema
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EntityVersionConfig {
    /// The version number
    /// It is recommended to follow semantic versioning eg `{major}.{minor}.{patch}`
    pub version: String,
    /// The list of attributes exposed by this entity
    pub attributes: Vec<EntityAttributeConfig>,
    /// The list of constraints (fk or unique) on this entity
    pub constraints: Vec<EntityConstraintConfig>,
    /// The source-specific config for reading or writing to this entity
    pub source: EntitySourceConfig,
}

impl EntityVersionConfig {
    pub fn minimal(
        id: impl Into<String>,
        attrs: Vec<EntityAttributeConfig>,
        source: EntitySourceConfig,
    ) -> Self {
        Self {
            version: id.into(),
            attributes: attrs,
            constraints: vec![],
            source,
        }
    }
}

/// An attribute of an entity
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EntityAttributeConfig {
    /// The ID of the attribute
    pub id: String,
    /// A description of the attribute
    pub description: String,
    /// The data type of the attribute
    pub r#type: DataType,
    /// Whether the attribute is part of the entity's primary key
    pub primary_key: bool,
    /// Whether the attribute is nullable
    pub nullable: bool,
}

impl EntityAttributeConfig {
    pub fn minimal(id: impl Into<String>, r#type: DataType) -> Self {
        Self {
            id: id.into(),
            description: "".to_string(),
            r#type,
            primary_key: false,
            nullable: false,
        }
    }
}

/// A constraint on the entity
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum EntityConstraintConfig {
    ForeignKey(ForeignKeyConstraintConfig),
    Unique(UniqueConstraintConfig),
}

/// A foreign key constraint
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraintConfig {
    /// Foreign entity ID
    pub target_entity_id: String,
    /// Mapping of local attribute names to target attribute names
    pub attribute_map: HashMap<String, String>,
}

/// A unique constraint config
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct UniqueConstraintConfig {
    /// List of local attributes within the unique constraint
    pub attributes: Vec<String>,
}

/// Defines the config used to read and write the entity
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EntitySourceConfig {
    /// The ID of the data source this entity is retrieved from
    pub data_source_id: String,
    /// The data source specific options for reading/writing to the entity
    pub options: serde_yaml::Value,
}

impl EntitySourceConfig {
    pub fn minimal(data_source_id: impl Into<String>) -> Self {
        Self {
            data_source_id: data_source_id.into(),
            options: serde_yaml::Value::Null,
        }
    }
}

/// The levels of accessibility of an entity
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum EntityAccessiblity {
    /// The entity can only be accessed from the node itself
    Internal,
    /// The entity is accessible from other nodes
    Public,
}

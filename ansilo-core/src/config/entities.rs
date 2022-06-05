use std::collections::HashMap;

use serde::{Serialize, Deserialize};

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
}

/// A tag attached to an entity. 
/// These are key-value pairs use for custom categorisation
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TagValueConfig {
    /// The tag key
    pub key: String,
    /// The tag value
    pub value: String
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

/// An attribute of an entity
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EntityAttributeConfig {
    /// The name of the attribute
    pub name: String,
    /// A description of the attribute
    pub description: String,
    /// The data type of the attribute
    pub r#type: EntityAttributeType,
    /// Whether the attribute is part of the entity's primary key
    pub primary_key: bool,
    /// Whether the attribute is nullable
    pub nullable: bool,
}

/// Data type of values
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum EntityAttributeType {
    Varchar(VarcharOptions),
    Text(EncodingType),
    Binary,
    Boolean,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    FloatSingle,
    FloatDouble,
    Decimal(DecimalOptions),
    JSON,
    Date,
    Time,
    DateTime,
    DateTimeWithTZ,
    Uuid,
}

/// Options for the VARCHAR data type
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct VarcharOptions {
    /// Maximum length of the varchar data in bytes
    pub length: u32,
    /// The type of encoding of the varchar data
    pub encoding: EncodingType,
}

/// Types of encoding of textual data
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum EncodingType {
    Ascii,
    Utf8,
    Utf16,
    Utf32,
    Other
}

/// Decimal options
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DecimalOptions {
    /// The capacity of number of digits for the type
    pub precision: u16,
    /// The number of digits after the decimal point '.'
    pub scale: u16
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
    pub options: HashMap<String, String>
}

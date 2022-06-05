use serde::{Serialize, Deserialize};

/// Ansilo resource identifier (ARI)
/// Used to specify a single or multiple ansilo resources
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ARI {
    /// The network (currently supports "ansilo")
    pub network: String,
    /// The node
    pub node: String,
    /// Type
    pub r#type: ARIType
}

/// Type-specific ARI strings 
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ARIType {
    /// The entity ARI type
    Entity(EntityARI),
}

/// ARI parts for specifying entities
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EntityARI {
    /// The name of the entity
    pub entity: String,
    /// The attributes of the entity
    pub attributes: Vec<String>,
}
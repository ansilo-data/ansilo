use serde::{Deserialize, Serialize};

use crate::config::{EntityAttributeConfig, EntityConstraintConfig, TagValueConfig};

/// Model for exposing the data catalog of this instance.
/// As a convention, we define the data catalog as all tables and
/// views in the postgres "public" schema.
///
/// We dont want to all underlying config of the entity, only
/// the schema itself, the type of data source, and if this
/// entity is imported from a peer instance, we expose its lineage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    pub entities: Vec<CatalogEntity>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogEntity {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<TagValueConfig>,
    pub attributes: Vec<CatalogEntityAttribue>,
    pub constraints: Vec<EntityConstraintConfig>,
    pub source: CatalogEntitySource,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogEntitySource {
    /// The name of the table/view which this entity represents
    pub table_name: String,
    /// If this entity is imported from a peer node, we expose the URL
    /// of that node.
    /// This allows a lineage to be formed if data is exposed through
    /// "hops" along multiple nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// If this entity is imported from a peer node, we also expose
    /// the source provided by the peer. This is exposed recursively
    /// allowing the full lineage to appear.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Box<Self>>,
}

impl CatalogEntitySource {
    pub fn table(table_name: String) -> Self {
        Self {
            table_name,
            url: None,
            source: None,
        }
    }

    pub fn parent(table_name: String, url: String, source: Self) -> Self {
        Self {
            table_name,
            url: Some(url),
            source: Some(Box::new(source)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogEntityAttribue {
    #[serde(flatten)]
    pub attribute: EntityAttributeConfig,
    // TODO[future]: expose data lineage through querying information schema VIEW_COLUMN_USAGE
    // pub sources: Vec<String>
}

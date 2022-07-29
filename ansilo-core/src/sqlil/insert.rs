use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::{EntitySource, Expr};

/// A query for inserting rows into a data source
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Insert {
    /// The list of insert column expressions indexed by the column name
    pub cols: Vec<(String, Expr)>,
    /// The target entity
    pub target: EntitySource,
}

impl Insert {
    pub fn new(target: EntitySource) -> Self {
        Self {
            cols: vec![],
            target,
        }
    }

    /// Gets the entity sources from this query
    pub fn get_entity_sources(&self) -> impl Iterator<Item = &EntitySource> {
        [&self.target]
            .into_iter()
    }

    /// Gets an iterator of all expressions in the query
    pub fn exprs(&self) -> impl Iterator<Item = &Expr> + '_ {
        self.cols
            .iter()
            .map(|(_, e)| e)
    }
}
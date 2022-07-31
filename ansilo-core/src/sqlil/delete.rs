use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::{EntitySource, Expr};

/// A query for deleting rows from a data source
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Delete {
    /// The target entity
    pub target: EntitySource,
    /// The list of where clauses
    pub r#where: Vec<Expr>,
}

impl Delete {
    pub fn new(target: EntitySource) -> Self {
        Self {
            target,
            r#where: vec![],
        }
    }

    /// Gets the entity sources from this query
    pub fn get_entity_sources(&self) -> impl Iterator<Item = &EntitySource> {
        [&self.target].into_iter()
    }

    /// Gets an iterator of all expressions in the query
    pub fn exprs(&self) -> impl Iterator<Item = &Expr> + '_ {
        self.r#where.iter()
    }
}

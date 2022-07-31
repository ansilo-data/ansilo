use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::{EntitySource, Expr};

/// A query for updating rows from a data source
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Update {
    /// The list of set column expressions indexed by the column name
    pub cols: Vec<(String, Expr)>,
    /// The target entity
    pub target: EntitySource,
    /// The list of where clauses
    pub r#where: Vec<Expr>,
}

impl Update {
    pub fn new(target: EntitySource) -> Self {
        Self {
            cols: vec![],
            target,
            r#where: vec![],
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
            .chain(self.r#where.iter())
    }
}

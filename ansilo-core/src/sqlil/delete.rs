use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::{EntitySource, Expr, Ordering};

/// A query for deleting rows from a data source
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Delete {
    /// The target entity
    pub target: EntitySource,
    /// The list of where clauses
    pub r#where: Vec<Expr>,
    /// This list of ordering clauses
    pub order_bys: Vec<Ordering>,
    /// The number of rows to return
    pub row_limit: Option<u64>,
    /// The number of rows to skip
    pub row_skip: u64,
}

impl Delete {
    pub fn new(target: EntitySource) -> Self {
        Self {
            target,
            r#where: vec![],
            order_bys: vec![],
            row_limit: None,
            row_skip: 0,
        }
    }

    /// Gets the entity sources from this query
    pub fn get_entity_sources(&self) -> impl Iterator<Item = &EntitySource> {
        [&self.target]
            .into_iter()
    }

    /// Gets an iterator of all expressions in the query
    pub fn exprs(&self) -> impl Iterator<Item = &Expr> + '_ {
        self.r#where
            .iter()
            .chain(self.order_bys.iter().map(|i| &i.expr))
    }
}

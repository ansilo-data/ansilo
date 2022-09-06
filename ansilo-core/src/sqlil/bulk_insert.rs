use bincode::{Decode, Encode};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::{EntitySource, Expr};

/// A query for inserting multiple rows
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct BulkInsert {
    /// The list of columns for each row
    pub cols: Vec<String>,
    /// The flattened list of expressions
    pub values: Vec<Expr>,
    /// The target entity
    pub target: EntitySource,
}

impl BulkInsert {
    pub fn new(target: EntitySource) -> Self {
        Self {
            cols: vec![],
            values: vec![],
            target,
        }
    }

    /// Gets the entity sources from this query
    pub fn get_entity_sources(&self) -> impl Iterator<Item = &EntitySource> {
        [&self.target].into_iter()
    }

    /// Gets an iterator of the values grouped by row
    pub fn rows(&self) -> itertools::IntoChunks<std::slice::Iter<Expr>> {
        self.values.iter().chunks(self.cols.len())
    }

    /// Gets an iterator of all expressions in the query
    pub fn exprs(&self) -> impl Iterator<Item = &Expr> + '_ {
        self.values.iter()
    }
}

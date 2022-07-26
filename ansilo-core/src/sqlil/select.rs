use anyhow::{bail, Result};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::{
    expr::{EntityVersionIdentifier, Expr},
    EntitySource, Join, Ordering,
};

/// A query for retrieving rows from a data source
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Select {
    /// The list of column expressions indexed by their aliases
    pub cols: Vec<(String, Expr)>,
    /// The source FROM expression
    pub from: EntitySource,
    /// The join clauses
    pub joins: Vec<Join>,
    /// The list of where clauses
    pub r#where: Vec<Expr>,
    /// The list of grouping clauses
    pub group_bys: Vec<Expr>,
    /// This list of ordering clauses
    pub order_bys: Vec<Ordering>,
    /// The number of rows to return
    pub row_limit: Option<u64>,
    /// The number of rows to skip
    pub row_skip: u64,
}

impl Select {
    pub fn new(from: EntitySource) -> Self {
        Self {
            cols: vec![],
            from,
            joins: vec![],
            r#where: vec![],
            group_bys: vec![],
            order_bys: vec![],
            row_limit: None,
            row_skip: 0,
        }
    }

    /// Gets the source entity ID from the referenced alias
    pub fn get_entity(&self, alias: &str) -> Result<&EntityVersionIdentifier> {
        self.get_entity_source(alias).map(|s| &s.entity)
    }

    /// Gets the source entity from the referenced alias
    pub fn get_entity_source(&self, alias: &str) -> Result<&EntitySource> {
        if &self.from.alias == alias {
            return Ok(&self.from);
        }

        for join in self.joins.iter() {
            if &join.target.alias == alias {
                return Ok(&join.target);
            }
        }

        bail!("Failed to find alias \"{}\" in query", alias);
    }

    /// Gets an iterator of all expressions in the query
    pub fn exprs(&self) -> impl Iterator<Item = &Expr> + '_ {
        self.cols
            .iter()
            .map(|(_, e)| e)
            .chain(self.joins.iter().flat_map(|i| &i.conds))
            .chain(self.r#where.iter())
            .chain(self.group_bys.iter())
            .chain(self.order_bys.iter().map(|i| &i.expr))
    }
}

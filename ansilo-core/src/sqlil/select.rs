use std::collections::HashMap;

use super::expr::{EntityVersionIdentifier, Expr};

/// A SQLIL select query
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    /// The list of column expressions indexed by their aliases
    pub cols: HashMap<String, Expr>,
    /// The source FROM expression
    pub from: EntityVersionIdentifier,
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
    pub fn new(from: EntityVersionIdentifier) -> Self {
        Self {
            cols: HashMap::new(),
            from,
            joins: vec![],
            r#where: vec![],
            group_bys: vec![],
            order_bys: vec![],
            row_limit: None,
            row_skip: 0,
        }
    }
}

/// A join clause
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// Join type
    pub r#type: JoinType,
    /// The joined entity
    pub target: EntityVersionIdentifier,
    /// The joining conditions
    pub conds: Vec<Expr>,
}

/// Type of the join
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// An ordering expression
#[derive(Debug, Clone, PartialEq)]
pub struct Ordering {
    /// The type of ordering
    pub r#type: OrderingType,
    /// The ordering expression
    pub expr: Expr,
}

/// Type of ordering
#[derive(Debug, Clone, PartialEq)]
pub enum OrderingType {
    Asc,
    Desc,
}

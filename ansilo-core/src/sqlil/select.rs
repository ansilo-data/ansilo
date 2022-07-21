use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::expr::{EntityVersionIdentifier, Expr};

/// A SQLIL select query
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Select {
    /// The list of column expressions indexed by their aliases
    pub cols: Vec<(String, Expr)>,
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
}

/// A join clause
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Join {
    /// Join type
    pub r#type: JoinType,
    /// The joined entity
    pub target: EntityVersionIdentifier,
    /// The joining conditions
    pub conds: Vec<Expr>,
}

impl Join {
    pub fn new(r#type: JoinType, target: EntityVersionIdentifier, conds: Vec<Expr>) -> Self {
        Self {
            r#type,
            target,
            conds,
        }
    }
}

/// Type of the join
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// An ordering expression
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Ordering {
    /// The type of ordering
    pub r#type: OrderingType,
    /// The ordering expression
    pub expr: Expr,
}

impl Ordering {
    pub fn new(r#type: OrderingType, expr: Expr) -> Self {
        Self { r#type, expr }
    }
}

/// Type of ordering
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum OrderingType {
    Asc,
    Desc,
}

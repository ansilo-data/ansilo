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

impl JoinType {
    /// Returns `true` if the join type is [`Inner`].
    ///
    /// [`Inner`]: JoinType::Inner
    #[must_use]
    pub fn is_inner(&self) -> bool {
        matches!(self, Self::Inner)
    }

    /// Returns `true` if the join type is [`Left`].
    ///
    /// [`Left`]: JoinType::Left
    #[must_use]
    pub fn is_left(&self) -> bool {
        matches!(self, Self::Left)
    }

    /// Returns `true` if the join type is [`Right`].
    ///
    /// [`Right`]: JoinType::Right
    #[must_use]
    pub fn is_right(&self) -> bool {
        matches!(self, Self::Right)
    }

    /// Returns `true` if the join type is [`Full`].
    ///
    /// [`Full`]: JoinType::Full
    #[must_use]
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full)
    }
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

    pub fn asc(expr: Expr) -> Self {
        Self::new(OrderingType::Asc, expr)
    }

    pub fn desc(expr: Expr) -> Self {
        Self::new(OrderingType::Desc, expr)
    }
}

/// Type of ordering
#[derive(Debug, Clone, Copy, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum OrderingType {
    Asc,
    Desc,
}

impl OrderingType {
    /// Returns `true` if the ordering type is [`Asc`].
    ///
    /// [`Asc`]: OrderingType::Asc
    #[must_use]
    pub fn is_asc(&self) -> bool {
        matches!(self, Self::Asc)
    }

    /// Returns `true` if the ordering type is [`Desc`].
    ///
    /// [`Desc`]: OrderingType::Desc
    #[must_use]
    pub fn is_desc(&self) -> bool {
        matches!(self, Self::Desc)
    }
}

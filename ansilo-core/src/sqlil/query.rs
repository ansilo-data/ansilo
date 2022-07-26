use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::expr::{EntityVersionIdentifier, Expr};

/// The referenced entity and it's associated alias
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct EntitySource {
    /// The source entity
    pub entity: EntityVersionIdentifier,
    /// The alias of the source, referenced in expressions
    pub alias: String,
}

impl EntitySource {
    pub fn new(entity: EntityVersionIdentifier, alias: impl Into<String>) -> Self {
        Self {
            entity,
            alias: alias.into(),
        }
    }
}

/// Creates a new entity source
pub fn source(
    entity_id: impl Into<String>,
    version_id: impl Into<String>,
    alias: impl Into<String>,
) -> EntitySource {
    EntitySource::new(EntityVersionIdentifier::new(entity_id, version_id), alias)
}

/// A join clause
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Join {
    /// Join type
    pub r#type: JoinType,
    /// The joined entity
    pub target: EntitySource,
    /// The joining conditions
    pub conds: Vec<Expr>,
}

impl Join {
    pub fn new(r#type: JoinType, target: EntitySource, conds: Vec<Expr>) -> Self {
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

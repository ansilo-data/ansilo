use anyhow::{Context, Result};
use bincode::{Decode, Encode};
use enum_as_inner::EnumAsInner;
use serde::{Deserialize, Serialize};

use super::{
    expr::{EntityId, Expr},
    BulkInsert, Delete, Insert, Select, Update,
};

/// A query to be executed against a data source
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, EnumAsInner)]
pub enum Query {
    Select(Select),
    Insert(Insert),
    BulkInsert(BulkInsert),
    Update(Update),
    Delete(Delete),
}

/// The type of queries supported
#[derive(Debug, Clone, Copy, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum QueryType {
    Select,
    Insert,
    BulkInsert,
    Update,
    Delete,
}

impl Query {
    /// Gets the type of the query
    pub fn r#type(&self) -> QueryType {
        match self {
            Query::Select(_) => QueryType::Select,
            Query::Insert(_) => QueryType::Insert,
            Query::BulkInsert(_) => QueryType::BulkInsert,
            Query::Update(_) => QueryType::Update,
            Query::Delete(_) => QueryType::Delete,
        }
    }

    /// Gets the entity sources from this query
    pub fn get_entity_sources(&self) -> impl Iterator<Item = &EntitySource> {
        match self {
            Query::Select(q) => q.get_entity_sources().collect::<Vec<_>>().into_iter(),
            Query::Insert(q) => q.get_entity_sources().collect::<Vec<_>>().into_iter(),
            Query::BulkInsert(q) => q.get_entity_sources().collect::<Vec<_>>().into_iter(),
            Query::Update(q) => q.get_entity_sources().collect::<Vec<_>>().into_iter(),
            Query::Delete(q) => q.get_entity_sources().collect::<Vec<_>>().into_iter(),
        }
    }

    /// Gets the source entity ID from the referenced alias
    pub fn get_entity(&self, alias: &str) -> Result<&EntityId> {
        self.get_entity_source(alias).map(|s| &s.entity)
    }

    /// Gets the source entity from the referenced alias
    pub fn get_entity_source(&self, alias: &str) -> Result<&EntitySource> {
        self.get_entity_sources()
            .find(|i| i.alias == alias)
            .with_context(|| format!("Failed to find alias \"{}\" in query", alias))
    }

    /// Gets the expr's in the query
    pub fn exprs(&self) -> impl Iterator<Item = &Expr> + '_ {
        match self {
            Query::Select(q) => q.exprs().collect::<Vec<_>>().into_iter(),
            Query::Insert(q) => q.exprs().collect::<Vec<_>>().into_iter(),
            Query::BulkInsert(q) => q.exprs().collect::<Vec<_>>().into_iter(),
            Query::Update(q) => q.exprs().collect::<Vec<_>>().into_iter(),
            Query::Delete(q) => q.exprs().collect::<Vec<_>>().into_iter(),
        }
    }

    /// Get's the queries WHERE conditions
    pub fn r#where(&self) -> &Vec<Expr> {
        match self {
            Query::Select(q) => &q.r#where,
            Query::Update(q) => &q.r#where,
            Query::Delete(q) => &q.r#where,
            Query::Insert(_) => unimplemented!(),
            Query::BulkInsert(_) => unimplemented!(),
        }
    }

    /// Walks all expressions within the query
    pub fn walk_expr<T: FnMut(&Expr) -> ()>(&self, cb: &mut T) {
        for expr in self.exprs() {
            expr.walk(cb);
        }
    }
}

impl From<Select> for Query {
    fn from(v: Select) -> Self {
        Self::Select(v)
    }
}

impl From<Insert> for Query {
    fn from(v: Insert) -> Self {
        Self::Insert(v)
    }
}

impl From<BulkInsert> for Query {
    fn from(v: BulkInsert) -> Self {
        Self::BulkInsert(v)
    }
}

impl From<Update> for Query {
    fn from(v: Update) -> Self {
        Self::Update(v)
    }
}

impl From<Delete> for Query {
    fn from(v: Delete) -> Self {
        Self::Delete(v)
    }
}

/// The referenced entity and it's associated alias
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct EntitySource {
    /// The source entity
    #[serde(flatten)]
    pub entity: EntityId,
    /// The alias of the source, referenced in expressions
    pub alias: String,
}

impl EntitySource {
    pub fn new(entity: EntityId, alias: impl Into<String>) -> Self {
        Self {
            entity,
            alias: alias.into(),
        }
    }
}

/// Creates a new entity source
pub fn source(entity_id: impl Into<String>, alias: impl Into<String>) -> EntitySource {
    EntitySource::new(EntityId::new(entity_id), alias)
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

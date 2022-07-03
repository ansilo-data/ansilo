use serde::{Serialize, Deserialize};

use crate::common::data::{DataType, DataValue};

/// A SQLIL expression node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    EntityVersion(EntityVersionIdentifier),
    EntityVersionAttribute(EntityVersionAttributeIdentifier),
    Constant(Constant),
    Parameter(Parameter),
    UnaryOp(UnaryOp),
    BinaryOp(BinaryOp),
    FunctionCall(FunctionCall),
    AggregateCall(AggregateCall),
    // TODO:
    // SubSelect(SubSelect)
}

type SubExpr = Box<Expr>;

/// A reference to an entity version
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityVersionIdentifier {
    /// The ID of the referenced entity
    pub entity_id: String,
    /// The referenced version
    pub version_id: String,
}

impl EntityVersionIdentifier {
    pub fn new(entity_id: impl Into<String>, version_id: impl Into<String>) -> Self {
        Self {
            entity_id: entity_id.into(),
            version_id: version_id.into(),
        }
    }
}

/// A reference to an attribute from an entity version
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityVersionAttributeIdentifier {
    /// The referenced entity version
    pub entity: EntityVersionIdentifier,
    /// The referenced attribute id
    pub attribute_id: String,
}

impl EntityVersionAttributeIdentifier {
    pub fn new(entity: EntityVersionIdentifier, attribute_id: impl Into<String>) -> Self {
        Self {
            entity,
            attribute_id: attribute_id.into(),
        }
    }
}

/// A constant embedded in the query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Constant {
    /// The value of the constant
    pub value: DataValue,
}

impl Constant {
    pub fn new(value: DataValue) -> Self {
        Self { value }
    }
}

/// A parameter embedded in the query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Parameter {
    /// The data type of the constant
    pub r#type: DataType,
    /// An ID of the query param
    pub id: i32,
}

impl Parameter {
    pub fn new(r#type: DataType, id: i32) -> Self {
        Self { r#type, id }
    }
}

/// A unary operation over one expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnaryOp {
    /// The data type of the constant
    pub r#type: UnaryOpType,
    /// The expression being operated on
    pub expr: SubExpr,
}

impl UnaryOp {
    pub fn new(r#type: UnaryOpType, expr: Expr) -> Self {
        Self {
            r#type,
            expr: Box::new(expr),
        }
    }
}

/// Supported unary operators
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOpType {
    Not,
    Negate,
    BitwiseNot,
    IsNull,
    IsNotNull,
}

/// A binary operation over two expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinaryOp {
    /// The LHS of the expression
    pub left: SubExpr,
    /// The binary operator being used
    pub r#type: BinaryOpType,
    /// The RHS of the expression
    pub right: SubExpr,
}

impl BinaryOp {
    pub fn new(left: Expr, r#type: BinaryOpType, right: Expr) -> Self {
        Self {
            left: Box::new(left),
            r#type,
            right: Box::new(right),
        }
    }
}

/// Supported binary operators
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOpType {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Exponent,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseShiftLeft,
    BitwiseShiftRight,
    Concat,
    Regexp,
    In,
    NotIn,
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

/// Supported function calls
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FunctionCall {
    // Math functions
    Abs(SubExpr),
    // String functions
    Length(SubExpr),
    Uppercase(SubExpr),
    Lowercase(SubExpr),
    Substring(SubstringCall),
    // Date/time functions
    Now,
    // Other functions
    Uuid,
    Coalesce(Vec<SubExpr>),
}

/// Substring function call
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubstringCall {
    /// The string to operator on
    pub string: SubExpr,
    /// The 1-based index of the character to start from
    pub start: SubExpr,
    /// The number of characters to return
    pub len: SubExpr,
}

impl SubstringCall {
    pub fn new(string: Expr, start: Expr, len: Expr) -> Self {
        Self {
            string: Box::new(string),
            start: Box::new(start),
            len: Box::new(len),
        }
    }
}

/// Aggregate function calls
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateCall {
    // Math functions
    Sum(SubExpr),
    Count,
    CountDistinct(SubExpr),
    Max(SubExpr),
    Min(SubExpr),
    // String functions
    StringAgg(SubExpr, String),
}

/// Constructurs a new entity expression
pub fn entity(entity_id: impl Into<String>, version: impl Into<String>) -> EntityVersionIdentifier {
    EntityVersionIdentifier::new(entity_id, version)
}

/// Constructurs a new entity attribute expression
pub fn attr(
    entity_id: impl Into<String>,
    version: impl Into<String>,
    attr_id: impl Into<String>,
) -> EntityVersionAttributeIdentifier {
    EntityVersionAttributeIdentifier::new(entity(entity_id, version), attr_id)
}

impl Expr {
    pub fn entity(entity_id: impl Into<String>, version: impl Into<String>) -> Self {
        Self::EntityVersion(entity(entity_id, version))
    }

    pub fn attr(
        entity_id: impl Into<String>,
        version: impl Into<String>,
        attr_id: impl Into<String>,
    ) -> Self {
        Self::EntityVersionAttribute(attr(entity_id, version, attr_id))
    }
}

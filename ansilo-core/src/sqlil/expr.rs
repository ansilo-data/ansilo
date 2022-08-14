use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::data::{DataType, DataValue};

/// A SQLIL expression node
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
#[serde(tag = "@type")]
pub enum Expr {
    Attribute(AttributeId),
    Constant(Constant),
    Parameter(Parameter),
    UnaryOp(UnaryOp),
    BinaryOp(BinaryOp),
    Cast(Cast),
    FunctionCall(FunctionCall),
    AggregateCall(AggregateCall),
}

type SubExpr = Box<Expr>;

/// A reference to an entity version
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct EntityId {
    /// The ID of the referenced entity
    pub entity_id: String,
}

impl EntityId {
    pub fn new(entity_id: impl Into<String>) -> Self {
        Self {
            entity_id: entity_id.into(),
        }
    }
}

/// A reference to an attribute from an entity version
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct AttributeId {
    /// The referenced entity alias
    pub entity_alias: String,
    /// The referenced attribute id
    pub attribute_id: String,
}

impl AttributeId {
    pub fn new(entity_alias: impl Into<String>, attribute_id: impl Into<String>) -> Self {
        Self {
            entity_alias: entity_alias.into(),
            attribute_id: attribute_id.into(),
        }
    }
}

/// A constant embedded in the query
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Constant {
    /// The value of the constant
    #[bincode(with_serde)]
    pub value: DataValue,
}

impl Constant {
    pub fn new(value: DataValue) -> Self {
        Self { value }
    }
}

/// A parameter embedded in the query
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Parameter {
    /// The data type of the constant
    pub r#type: DataType,
    /// An ID of the query param
    pub id: u32,
}

impl Parameter {
    pub fn new(r#type: DataType, id: u32) -> Self {
        Self { r#type, id }
    }
}

/// A unary operation over one expression
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum UnaryOpType {
    LogicalNot,
    Negate,
    BitwiseNot,
    IsNull,
    IsNotNull,
}

/// A binary operation over two expressions
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum BinaryOpType {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Exponent,
    LogicalAnd,
    LogicalOr,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseShiftLeft,
    BitwiseShiftRight,
    Concat,
    Regexp,
    Equal,
    NullSafeEqual,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

/// Supported type casts
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct Cast {
    // Input value to the cast
    pub expr: SubExpr,
    // The resulting type
    pub r#type: DataType,
}

impl Cast {
    pub fn new(expr: SubExpr, r#type: DataType) -> Self {
        Self { expr, r#type }
    }
}

/// Supported function calls
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum FunctionCall {
    // Math functions
    Abs(SubExpr),
    // String functions
    Length(SubExpr),
    Uppercase(SubExpr),
    Lowercase(SubExpr),
    Substring(SubstringCall),
    // Date/time functions
    // Other functions
    Uuid,
    Coalesce(Vec<SubExpr>),
}

impl FunctionCall {
    fn walk<T: FnMut(&Expr) -> ()>(&self, cb: &mut T) {
        match self {
            FunctionCall::Abs(e) => e.walk(cb),
            FunctionCall::Length(e) => e.walk(cb),
            FunctionCall::Uppercase(e) => e.walk(cb),
            FunctionCall::Lowercase(e) => e.walk(cb),
            FunctionCall::Substring(e) => {
                e.string.walk(cb);
                e.len.walk(cb);
                e.start.walk(cb);
            }
            FunctionCall::Coalesce(e) => e.into_iter().for_each(|i| i.walk(cb)),
            FunctionCall::Uuid => {}
        }
    }
}

/// Substring function call
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub enum AggregateCall {
    // Math functions
    Sum(SubExpr),
    Count,
    CountDistinct(SubExpr),
    Max(SubExpr),
    Min(SubExpr),
    Average(SubExpr),
    // String functions
    StringAgg(StringAggCall),
}

impl AggregateCall {
    fn walk<T: FnMut(&Expr) -> ()>(&self, cb: &mut T) {
        match self {
            AggregateCall::Sum(e) => e.walk(cb),
            AggregateCall::Count => {}
            AggregateCall::CountDistinct(e) => e.walk(cb),
            AggregateCall::Max(e) => e.walk(cb),
            AggregateCall::Min(e) => e.walk(cb),
            AggregateCall::Average(e) => e.walk(cb),
            AggregateCall::StringAgg(e) => e.expr.walk(cb),
        }
    }
}

/// Call arguments to string aggregation
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct StringAggCall {
    /// The expr being aggregated
    pub expr: SubExpr,
    /// The seperator used during aggregation
    pub separator: String,
}

impl StringAggCall {
    pub fn new(expr: SubExpr, separator: String) -> Self {
        Self { expr, separator }
    }
}

/// Constructurs a new entity expression
pub fn entity(entity_id: impl Into<String>) -> EntityId {
    EntityId::new(entity_id)
}

/// Constructurs a new entity attribute expression
pub fn attr(alias: impl Into<String>, attr_id: impl Into<String>) -> AttributeId {
    AttributeId::new(alias, attr_id)
}

impl Expr {
    pub fn attr(alias: impl Into<String>, attr_id: impl Into<String>) -> Self {
        Self::Attribute(attr(alias, attr_id))
    }

    pub fn constant(val: DataValue) -> Self {
        Self::Constant(Constant::new(val))
    }

    /// Walks the expression tree, passing all nodes to the supplied callback
    pub fn walk<T: FnMut(&Expr) -> ()>(&self, cb: &mut T) {
        cb(self);

        match self {
            Expr::UnaryOp(e) => e.expr.walk(cb),
            Expr::BinaryOp(e) => {
                e.left.walk(cb);
                e.right.walk(cb);
            }
            Expr::Cast(e) => e.expr.walk(cb),
            Expr::FunctionCall(e) => e.walk(cb),
            Expr::AggregateCall(e) => e.walk(cb),
            Expr::Attribute(_) => {}
            Expr::Constant(_) => {}
            Expr::Parameter(_) => {}
        }
    }

    /// Returns whether any of the expressions in the tree pass the supplied
    /// filter callback
    pub fn walk_any<T: Fn(&Expr) -> bool>(&self, cb: T) -> bool {
        let mut flag = false;

        self.walk(&mut |e| {
            flag = flag || cb(e);
        });

        flag
    }

    /// Returns whether all of the expressions in the tree pass the supplied
    /// filter callback
    pub fn walk_all<T: Fn(&Expr) -> bool>(&self, cb: T) -> bool {
        let mut flag = true;

        self.walk(&mut |e| {
            flag = flag && cb(e);
        });

        flag
    }
}

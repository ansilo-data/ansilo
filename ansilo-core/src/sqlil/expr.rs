use crate::common::data::DataType;

/// A SQLIL expression node
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    EntityVersion(EntityVersionIdentifier),
    EntityVersionAttribute(EntityVersionAttributeIdentifier),
    Constant(Constant),
    Null,
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
#[derive(Debug, Clone, PartialEq)]
pub struct EntityVersionIdentifier {
    /// The ID of the referenced entity
    pub entity_id: String,
    /// The referenced version
    pub version_id: String,
}

impl EntityVersionIdentifier {
    pub fn new(entity_id: String, version_id: String) -> Self {
        Self {
            entity_id,
            version_id,
        }
    }
}

/// A reference to an attribute from an entity version
#[derive(Debug, Clone, PartialEq)]
pub struct EntityVersionAttributeIdentifier {
    /// The referenced entity version
    pub entity: EntityVersionIdentifier,
    /// The referenced attribute id
    pub attribute_id: String,
}

impl EntityVersionAttributeIdentifier {
    pub fn new(entity: EntityVersionIdentifier, attribute_id: String) -> Self {
        Self {
            entity,
            attribute_id,
        }
    }
}

/// A constant embedded in the query
#[derive(Debug, Clone, PartialEq)]
pub struct Constant {
    /// The data type of the constant
    pub r#type: DataType,
    /// A binary representation of the constant
    pub value: Vec<u8>,
}

impl Constant {
    pub fn new(r#type: DataType, value: Vec<u8>) -> Self {
        Self { r#type, value }
    }
}

/// A parameter embedded in the query
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOpType {
    Not,
    Negate,
    BitwiseNot,
}

/// A binary operation over two expressions
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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
    IsNull,
    IsNotNull,
}

/// Supported function calls
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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

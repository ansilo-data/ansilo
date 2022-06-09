use crate::common::data::DataType;

/// A SQLIL expression node
pub enum Expr {
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

/// A constant embedded in the query 
pub struct Constant {
    /// The data type of the constant
    pub r#type: DataType,
    /// A binary representation of the constant
    pub value: Vec<u8>,
}

/// A parameter embedded in the query 
pub struct Parameter {
    /// The data type of the constant
    pub r#type: DataType,
    /// An ID of the query param
    pub id: i32
}

/// A unary operation over one expression
pub struct UnaryOp {
    /// The data type of the constant
    pub r#type: UnaryOpType,
    /// The expression being operated on
    pub expr: SubExpr
}

/// Supported unary operators
pub enum UnaryOpType {
    Not,
    Negate,
    BitwiseNot
}

/// A binary operation over two expressions
pub struct BinaryOp {
    /// The LHS of the expression
    pub left: SubExpr,
    /// The binary operator being used
    pub r#type: BinaryOpType,
    /// The RHS of the expression
    pub right: SubExpr
}

/// Supported binary operators
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
    Coalesce(Vec<SubExpr>)
}

/// Substring function call
pub struct SubstringCall {
    /// The string to operator on
    pub string: SubExpr,
    /// The 1-based index of the character to start from
    pub start: SubExpr,
    /// The number of characters to return
    pub len: SubExpr
}

/// Aggregate function calls
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
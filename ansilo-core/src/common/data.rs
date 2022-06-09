use serde::{Serialize, Deserialize};

/// Data type of values
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataType {
    Varchar(VarcharOptions),
    Text(EncodingType),
    Binary,
    Boolean,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    FloatSingle,
    FloatDouble,
    Decimal(DecimalOptions),
    JSON,
    Date,
    Time,
    DateTime,
    DateTimeWithTZ,
    Uuid,
}

/// Options for the VARCHAR data type
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct VarcharOptions {
    /// Maximum length of the varchar data in bytes
    pub length: u32,
    /// The type of encoding of the varchar data
    pub encoding: EncodingType,
}

/// Types of encoding of textual data
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum EncodingType {
    Ascii,
    Utf8,
    Utf16,
    Utf32,
    Other
}

/// Decimal options
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DecimalOptions {
    /// The capacity of number of digits for the type
    pub precision: u16,
    /// The number of digits after the decimal point '.'
    pub scale: u16
}
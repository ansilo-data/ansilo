use serde::{Deserialize, Serialize};

/// Data type of values
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataType {
    Varchar(VarcharOptions),
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
    pub length: Option<u32>,
    /// The type of encoding of the varchar data
    pub encoding: EncodingType,
}

impl VarcharOptions {
    pub fn new(length: Option<u32>, encoding: EncodingType) -> Self {
        Self { length, encoding }
    }
}

/// Types of encoding of textual data
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum EncodingType {
    Ascii,
    Utf8,
    Utf16,
    Utf32,
    Other,
}

/// Decimal options
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct DecimalOptions {
    /// The capacity of number of digits for the type
    pub precision: Option<u16>,
    /// The number of digits after the decimal point '.'
    pub scale: Option<u16>,
}

impl DecimalOptions {
    pub fn new(precision: Option<u16>, scale: Option<u16>) -> Self {
        Self { precision, scale }
    }
}

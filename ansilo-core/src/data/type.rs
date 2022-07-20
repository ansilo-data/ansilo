use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Data type of values
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum DataType {
    Utf8String(StringOptions),
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
    Float32,
    Float64,
    Decimal(DecimalOptions),
    JSON,
    Date,
    Time,
    DateTime,
    DateTimeWithTZ,
    Uuid,
    Null,
}

impl DataType {
    pub fn rust_string() -> Self {
        Self::Utf8String(StringOptions::new(None))
    }
}

/// Options for the VARCHAR data type
#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct StringOptions {
    /// Maximum length of the varchar data in bytes
    pub length: Option<u32>,
}

impl StringOptions {
    pub fn new(length: Option<u32>) -> Self {
        Self { length }
    }
}

/// Types of encoding of textual data
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum EncodingType {
    Ascii,
    Utf8,
    Utf16,
}

/// Decimal options
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode, Default)]
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

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use super::DataValue;

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

// Provide conversion from DataValue into DataType
impl<'a> From<&'a DataValue> for DataType {
    fn from(v: &'a DataValue) -> Self {
        match v {
            DataValue::Null => DataType::Null,
            DataValue::Utf8String(_) => {
                DataType::Utf8String(StringOptions::default())
            }
            DataValue::Binary(_) => DataType::Binary,
            DataValue::Boolean(_) => DataType::Boolean,
            DataValue::Int8(_) => DataType::Int8,
            DataValue::UInt8(_) => DataType::UInt8,
            DataValue::Int16(_) => DataType::Int16,
            DataValue::UInt16(_) => DataType::UInt16,
            DataValue::Int32(_) => DataType::Int32,
            DataValue::UInt32(_) => DataType::UInt32,
            DataValue::Int64(_) => DataType::Int64,
            DataValue::UInt64(_) => DataType::UInt64,
            DataValue::Float32(_) => DataType::Float32,
            DataValue::Float64(_) => DataType::Float64,
            DataValue::Decimal(_) => DataType::Decimal(DecimalOptions::default()),
            DataValue::JSON(_) => DataType::JSON,
            DataValue::Date(_) => DataType::Date,
            DataValue::Time(_) => DataType::Time,
            DataValue::DateTime(_) => DataType::DateTime,
            DataValue::DateTimeWithTZ(_) => DataType::DateTimeWithTZ,
            DataValue::Uuid(_) => DataType::Uuid,
        }
    }
}

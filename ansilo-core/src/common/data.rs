use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

pub use chrono;
pub use chrono_tz;
pub use rust_decimal;
pub use uuid;

/// Data type of values
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
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
        Self::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
    }
}

/// Data container for respective types
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataValue {
    Null,
    Varchar(Vec<u8>),
    Binary(Vec<u8>),
    Boolean(bool),
    Int8(i8),
    UInt8(u8),
    Int16(i16),
    UInt16(u16),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal(rust_decimal::Decimal),
    JSON(String),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    DateTime(chrono::NaiveDateTime),
    DateTimeWithTZ((chrono::NaiveDateTime, chrono_tz::Tz)),
    Uuid(uuid::Uuid),
}

impl DataValue {
    pub fn is_null(&self) -> bool {
        *self == DataValue::Null
    }
}

/// Provide conversion from DataValue into DataType
impl From<DataValue> for DataType {
    fn from(v: DataValue) -> Self {
        match v {
            DataValue::Null => DataType::Null,
            DataValue::Varchar(_) => {
                DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
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
            DataValue::Decimal(_) => DataType::Decimal(DecimalOptions::new(None, None)),
            DataValue::JSON(_) => DataType::JSON,
            DataValue::Date(_) => DataType::Date,
            DataValue::Time(_) => DataType::Time,
            DataValue::DateTime(_) => DataType::DateTime,
            DataValue::DateTimeWithTZ(_) => DataType::DateTimeWithTZ,
            DataValue::Uuid(_) => DataType::Uuid,
        }
    }
}

impl From<&str> for DataValue {
    fn from(str: &str) -> Self {
        DataValue::Varchar(str.as_bytes().to_vec())
    }
}

/// Options for the VARCHAR data type
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Encode, Decode)]
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

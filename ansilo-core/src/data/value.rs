use serde::{Deserialize, Serialize};

use super::{DataType, StringOptions, DecimalOptions};

/// Data container for respective types
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataValue {
    Null,
    Utf8String(Vec<u8>),
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

// Provide conversion from DataValue into DataType
impl From<DataValue> for DataType {
    fn from(v: DataValue) -> Self {
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

impl From<&str> for DataValue {
    fn from(str: &str) -> Self {
        DataValue::Utf8String(str.as_bytes().to_vec())
    }
}

use serde::{Deserialize, Serialize};

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

impl From<&str> for DataValue {
    fn from(str: &str) -> Self {
        DataValue::Utf8String(str.as_bytes().to_vec())
    }
}
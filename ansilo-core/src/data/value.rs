use std::hash::Hash;

use anyhow::{bail, Result};
use chrono::{DateTime, LocalResult, TimeZone};
use enum_as_inner::EnumAsInner;
use serde::{Deserialize, Serialize};

use super::DataType;

/// Data container for respective types
#[derive(PartialEq, PartialOrd, Clone, Serialize, Deserialize, EnumAsInner)]
pub enum DataValue {
    Null,
    Utf8String(String),
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
    DateTimeWithTZ(DateTimeWithTZ),
    Uuid(uuid::Uuid),
}

impl DataValue {
    pub fn r#type(&self) -> DataType {
        self.into()
    }
}

impl From<&str> for DataValue {
    fn from(str: &str) -> Self {
        DataValue::Utf8String(str.to_string())
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct DateTimeWithTZ {
    /// The local date time
    pub dt: chrono::NaiveDateTime,
    /// The associated timezone
    pub tz: chrono_tz::Tz,
}

impl DateTimeWithTZ {
    pub fn new(dt: chrono::NaiveDateTime, tz: chrono_tz::Tz) -> Self {
        Self { dt, tz }
    }

    pub fn zoned(&self) -> Result<DateTime<chrono_tz::Tz>> {
        match self.tz.from_local_datetime(&self.dt) {
            LocalResult::Single(dt) => Ok(dt),
            _ => bail!(
                "Failed to parse local date/time '{:?}' in timezone '{}'",
                self.dt,
                self.tz.name()
            ),
        }
    }

    pub fn utc(&self) -> Result<DateTime<chrono::Utc>> {
        Ok(self.zoned()?.with_timezone(&chrono::Utc))
    }
}

impl PartialOrd for DateTimeWithTZ {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.dt.partial_cmp(&other.dt)
    }
}

impl std::fmt::Debug for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "Null"),
            Self::Utf8String(arg0) => f.debug_tuple("Utf8String").field(arg0).finish(),
            Self::Binary(arg0) => f.debug_tuple("Binary").field(arg0).finish(),
            Self::Boolean(arg0) => f.debug_tuple("Boolean").field(arg0).finish(),
            Self::Int8(arg0) => f.debug_tuple("Int8").field(arg0).finish(),
            Self::UInt8(arg0) => f.debug_tuple("UInt8").field(arg0).finish(),
            Self::Int16(arg0) => f.debug_tuple("Int16").field(arg0).finish(),
            Self::UInt16(arg0) => f.debug_tuple("UInt16").field(arg0).finish(),
            Self::Int32(arg0) => f.debug_tuple("Int32").field(arg0).finish(),
            Self::UInt32(arg0) => f.debug_tuple("UInt32").field(arg0).finish(),
            Self::Int64(arg0) => f.debug_tuple("Int64").field(arg0).finish(),
            Self::UInt64(arg0) => f.debug_tuple("UInt64").field(arg0).finish(),
            Self::Float32(arg0) => f.debug_tuple("Float32").field(arg0).finish(),
            Self::Float64(arg0) => f.debug_tuple("Float64").field(arg0).finish(),
            Self::Decimal(arg0) => f.debug_tuple("Decimal").field(arg0).finish(),
            Self::JSON(arg0) => f.debug_tuple("JSON").field(arg0).finish(),
            Self::Date(arg0) => f.debug_tuple("Date").field(arg0).finish(),
            Self::Time(arg0) => f.debug_tuple("Time").field(arg0).finish(),
            Self::DateTime(arg0) => f.debug_tuple("DateTime").field(arg0).finish(),
            Self::DateTimeWithTZ(arg0) => f.debug_tuple("DateTimeWithTZ").field(arg0).finish(),
            Self::Uuid(arg0) => f.debug_tuple("Uuid").field(arg0).finish(),
        }
    }
}

impl Hash for DataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            DataValue::Null => {}
            DataValue::Utf8String(data) => data.hash(state),
            DataValue::Binary(data) => data.hash(state),
            DataValue::Boolean(data) => data.hash(state),
            DataValue::Int8(data) => data.hash(state),
            DataValue::UInt8(data) => data.hash(state),
            DataValue::Int16(data) => data.hash(state),
            DataValue::UInt16(data) => data.hash(state),
            DataValue::Int32(data) => data.hash(state),
            DataValue::UInt32(data) => data.hash(state),
            DataValue::Int64(data) => data.hash(state),
            DataValue::UInt64(data) => data.hash(state),
            DataValue::Float32(data) => data.to_string().hash(state),
            DataValue::Float64(data) => data.to_string().hash(state),
            DataValue::Decimal(data) => data.hash(state),
            DataValue::JSON(data) => data.hash(state),
            DataValue::Date(data) => data.hash(state),
            DataValue::Time(data) => data.hash(state),
            DataValue::DateTime(data) => data.hash(state),
            DataValue::DateTimeWithTZ(data) => data.hash(state),
            DataValue::Uuid(data) => data.hash(state),
        }
    }
}

impl Eq for DataValue {}

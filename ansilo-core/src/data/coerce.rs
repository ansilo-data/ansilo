use std::cmp;

use anyhow::{bail, Result};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use rust_decimal::{
    prelude::{One, ToPrimitive},
    Decimal,
};
use uuid::Uuid;

use super::{DataType, DataValue, DateTimeWithTZ};

impl DataValue {
    /// Tries to coerce the data value supplied type.
    ///
    /// In order to ensure we do not allow users to lose data through accidental
    /// coercion we enforce that the rule:
    ///     COERCE(COERCE(A, NEW_TYPE), ORIG_TYPE) == A
    ///
    /// If this cannot hold due to data being discarded during the coercion we
    /// MUST bail out here.
    pub fn try_coerce_into(self, r#type: &DataType) -> Result<Self> {
        // Nulls are type-independent
        if self.is_null() {
            return Ok(self);
        }

        // If we are coercing into binary (our widest type) want to
        // easy roundtrip through the textual representation
        let data = if !self.as_binary().is_some() && r#type.is_binary() {
            self.try_coerce_into(&DataType::rust_string())
                .expect("Should be able to convert non-binary to string")
        } else {
            self
        };

        Ok(match data {
            DataValue::Null => unreachable!(),
            DataValue::Utf8String(data) => Self::try_coerce_utf8_string(data, r#type)?,
            DataValue::Binary(data) => Self::try_coerce_binary(data, r#type)?,
            DataValue::Boolean(data) => Self::try_coerce_boolean(data, r#type)?,
            DataValue::Int8(data) => Self::try_coerce_int8(data, r#type)?,
            DataValue::UInt8(data) => Self::try_coerce_uint8(data, r#type)?,
            DataValue::Int16(data) => Self::try_coerce_int16(data, r#type)?,
            DataValue::UInt16(data) => Self::try_coerce_uint16(data, r#type)?,
            DataValue::Int32(data) => Self::try_coerce_int32(data, r#type)?,
            DataValue::UInt32(data) => Self::try_coerce_uint32(data, r#type)?,
            DataValue::Int64(data) => Self::try_coerce_int64(data, r#type)?,
            DataValue::UInt64(data) => Self::try_coerce_uint64(data, r#type)?,
            DataValue::Float32(data) => Self::try_coerce_float32(data, r#type)?,
            DataValue::Float64(data) => Self::try_coerce_float64(data, r#type)?,
            DataValue::Decimal(data) => Self::try_coerce_decimal(data, r#type)?,
            DataValue::JSON(data) => Self::try_coerce_json(data, r#type)?,
            DataValue::Date(data) => Self::try_coerce_date(data, r#type)?,
            DataValue::Time(data) => Self::try_coerce_time(data, r#type)?,
            DataValue::DateTime(data) => Self::try_coerce_date_time(data, r#type)?,
            DataValue::DateTimeWithTZ(data) => Self::try_coerce_date_time_with_tz(data, r#type)?,
            DataValue::Uuid(data) => Self::try_coerce_uuid(data, r#type)?,
        })
    }

    fn try_coerce_utf8_string(data: String, r#type: &DataType) -> Result<DataValue> {
        match r#type {
            DataType::Utf8String(_) => return Ok(Self::Utf8String(data)),
            DataType::Binary => return Ok(Self::Binary(data.as_bytes().to_vec())),
            DataType::JSON if serde_json::from_str::<serde_json::Value>(&data).is_ok() => {
                return Ok(Self::JSON(data))
            }
            DataType::Boolean if data == "1" => return Ok(Self::Boolean(true)),
            DataType::Boolean if data == "0" => return Ok(Self::Boolean(false)),
            DataType::UInt8 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::UInt8(n));
                }
            }
            DataType::Int8 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::Int8(n));
                }
            }
            DataType::UInt16 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::UInt16(n));
                }
            }
            DataType::Int16 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::Int16(n));
                }
            }
            DataType::UInt32 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::UInt32(n));
                }
            }
            DataType::Int32 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::Int32(n));
                }
            }
            DataType::UInt64 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::UInt64(n));
                }
            }
            DataType::Int64 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::Int64(n));
                }
            }
            DataType::Float32 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::Float32(n));
                }
            }
            DataType::Float64 => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::Float64(n));
                }
            }
            DataType::Decimal(_) => {
                if let Ok(n) = data.parse() {
                    return Ok(DataValue::Decimal(n));
                }
            }
            DataType::Date => {
                if let Ok(date) = NaiveDate::parse_from_str(&data, "%Y-%m-%d") {
                    return Ok(Self::Date(date));
                }
            }
            DataType::Time => {
                if let Ok(time) = NaiveTime::parse_from_str(&data, "%H:%M:%S") {
                    return Ok(Self::Time(time));
                }
            }
            DataType::DateTime => {
                if let Ok(dt) = NaiveDateTime::parse_from_str(&data, "%Y-%m-%dT%H:%M:%S") {
                    return Ok(Self::DateTime(dt));
                }
            }
            DataType::DateTimeWithTZ => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(&data) {
                    return Ok(Self::DateTimeWithTZ(DateTimeWithTZ::new(
                        dt.naive_utc(),
                        chrono_tz::UTC,
                    )));
                }
            }
            DataType::Uuid => {
                if let Ok(uuid) = Uuid::try_parse(&data) {
                    return Ok(Self::Uuid(uuid));
                }
            }
            _ => {}
        };

        bail!(
            "No type coercion exists from type 'UTF-8 String' (\"{}\") to {:?}",
            &data[..cmp::min(50, data.len())],
            r#type
        )
    }

    fn try_coerce_binary(data: Vec<u8>, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Binary => Self::Binary(data),
            DataType::Utf8String(_) => {
                if let Ok(data) = String::from_utf8(data) {
                    DataValue::Utf8String(data)
                } else {
                    bail!("Failed to coerce binary data into UTF-8 string: data is not valid utf-8 encoded")
                }
            }
            _ => {
                // To support storing all types in binary we convert them to the textual
                // representation first, so if we are converting binart to another type
                // let's try coerce the string to that type
                if let Ok(str) = String::from_utf8(data) {
                    if let Ok(data) = Self::Utf8String(str).try_coerce_into(r#type) {
                        return Ok(data);
                    }

                    bail!("Failed to coerce binary data into UTF-8 string: data is not valid utf-8 encoded")
                }
                bail!("No type coercion exists from type 'binary' to {:?}", r#type)
            }
        })
    }

    fn try_coerce_boolean(data: bool, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Boolean => Self::Boolean(data),
            DataType::Int8 => Self::Int8(data as i8),
            DataType::UInt8 => Self::UInt8(data as u8),
            DataType::Int16 => Self::Int16(data as i16),
            DataType::UInt16 => Self::UInt16(data as u16),
            DataType::Int32 => Self::Int32(data as i32),
            DataType::UInt32 => Self::UInt32(data as u32),
            DataType::Int64 => Self::Int64(data as i64),
            DataType::UInt64 => Self::UInt64(data as u64),
            DataType::Float32 => Self::Float32(if data { 1.0 } else { 0.0 }),
            DataType::Float64 => Self::Float64(if data { 1.0 } else { 0.0 }),
            DataType::Decimal(_) => Self::Decimal(Decimal::new(data as _, 0)),
            DataType::Utf8String(_) => Self::Utf8String(if data { "1" } else { "0" }.to_string()),
            _ => bail!(
                "No type coercion exists from type 'boolean' to {:?}",
                r#type
            ),
        })
    }

    fn try_coerce_uint8(data: u8, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::UInt8 => Self::UInt8(data),
            DataType::Boolean if data == 0 => Self::Boolean(false),
            DataType::Boolean if data == 1 => Self::Boolean(true),
            DataType::Int8 => Self::Int8(data as i8),
            DataType::Int16 => Self::Int16(data as i16),
            DataType::UInt16 => Self::UInt16(data as u16),
            DataType::Int32 => Self::Int32(data as i32),
            DataType::UInt32 => Self::UInt32(data as u32),
            DataType::Int64 => Self::Int64(data as i64),
            DataType::UInt64 => Self::UInt64(data as u64),
            DataType::Float32 => Self::Float32(data as f32),
            DataType::Float64 => Self::Float64(data as f64),
            DataType::Decimal(_) => Self::Decimal(Decimal::new(data as _, 0)),
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'uint8' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_int8(data: i8, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Int8 => Self::Int8(data),
            DataType::Boolean if data == 0 => Self::Boolean(false),
            DataType::Boolean if data == 1 => Self::Boolean(true),
            DataType::UInt8 => Self::UInt8(data as u8),
            DataType::Int16 => Self::Int16(data as i16),
            DataType::UInt16 if data >= 0 => Self::UInt16(data as u16),
            DataType::Int32 => Self::Int32(data as i32),
            DataType::UInt32 if data >= 0 => Self::UInt32(data as u32),
            DataType::Int64 => Self::Int64(data as i64),
            DataType::UInt64 if data >= 0 => Self::UInt64(data as u64),
            DataType::Float32 => Self::Float32(data as f32),
            DataType::Float64 => Self::Float64(data as f64),
            DataType::Decimal(_) => Self::Decimal(Decimal::new(data as _, 0)),
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'int8' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_uint16(data: u16, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::UInt16 => Self::UInt16(data as u16),
            DataType::Boolean if data == 0 => Self::Boolean(false),
            DataType::Boolean if data == 1 => Self::Boolean(true),
            DataType::Int8 if data <= i8::MAX as _ => Self::Int8(data as i8),
            DataType::UInt8 if data >= u8::MIN as _ && data <= u8::MAX as _ => {
                Self::UInt8(data as u8)
            }
            DataType::Int16 if data <= i16::MAX as _ => Self::Int16(data as i16),
            DataType::Int32 => Self::Int32(data as i32),
            DataType::UInt32 => Self::UInt32(data as u32),
            DataType::Int64 => Self::Int64(data as i64),
            DataType::UInt64 => Self::UInt64(data as u64),
            DataType::Float32 => Self::Float32(data as f32),
            DataType::Float64 => Self::Float64(data as f64),
            DataType::Decimal(_) => Self::Decimal(Decimal::new(data as _, 0)),
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'uint16' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_int16(data: i16, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Int16 => Self::Int16(data),
            DataType::Boolean if data == 0 => Self::Boolean(false),
            DataType::Boolean if data == 1 => Self::Boolean(true),
            DataType::Int8 if data >= i8::MIN as _ && data <= i8::MAX as _ => {
                Self::Int8(data as i8)
            }
            DataType::UInt8 if data >= u8::MIN as _ && data <= u8::MAX as _ => {
                Self::UInt8(data as u8)
            }
            DataType::UInt16 if data >= 0 => Self::UInt16(data as u16),
            DataType::Int32 => Self::Int32(data as i32),
            DataType::UInt32 if data >= 0 => Self::UInt32(data as u32),
            DataType::Int64 => Self::Int64(data as i64),
            DataType::UInt64 if data >= 0 => Self::UInt64(data as u64),
            DataType::Float32 => Self::Float32(data as f32),
            DataType::Float64 => Self::Float64(data as f64),
            DataType::Decimal(_) => Self::Decimal(Decimal::new(data as _, 0)),
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'int16' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_uint32(data: u32, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::UInt32 => Self::UInt32(data),
            DataType::Boolean if data == 0 => Self::Boolean(false),
            DataType::Boolean if data == 1 => Self::Boolean(true),
            DataType::Int8 if data <= i8::MAX as _ => Self::Int8(data as i8),
            DataType::UInt8 if data <= u8::MAX as _ => Self::UInt8(data as u8),
            DataType::Int16 if data <= i16::MAX as _ => Self::Int16(data as i16),
            DataType::UInt16 if data <= u16::MAX as _ => Self::UInt16(data as u16),
            DataType::Int32 if data <= i32::MAX as _ => Self::Int32(data as i32),
            DataType::Int64 => Self::Int64(data as i64),
            DataType::UInt64 => Self::UInt64(data as u64),
            DataType::Float32 if (data as f32) as u32 == data => Self::Float32(data as f32),
            DataType::Float64 if (data as f64) as u32 == data => Self::Float64(data as f64),
            DataType::Decimal(_) => Self::Decimal(Decimal::new(data as _, 0)),
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'uint32' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_int32(data: i32, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Int32 => Self::Int32(data),
            DataType::Boolean if data == 0 => Self::Boolean(false),
            DataType::Boolean if data == 1 => Self::Boolean(true),
            DataType::Int8 if data >= i8::MIN as _ && data <= i8::MAX as _ => {
                Self::Int8(data as i8)
            }
            DataType::UInt8 if data >= u8::MIN as _ && data <= u8::MAX as _ => {
                Self::UInt8(data as u8)
            }
            DataType::Int16 if data >= i16::MIN as _ && data <= i16::MAX as _ => {
                Self::Int16(data as i16)
            }
            DataType::UInt16 if data >= u16::MIN as _ && data <= u16::MAX as _ => {
                Self::UInt16(data as u16)
            }
            DataType::UInt32 if data >= 0 => Self::UInt32(data as u32),
            DataType::Int64 => Self::Int64(data as i64),
            DataType::UInt64 if data >= 0 => Self::UInt64(data as u64),
            DataType::Float32 if (data as f32) as i32 == data => Self::Float32(data as f32),
            DataType::Float64 if (data as f64) as i32 == data => Self::Float64(data as f64),
            DataType::Decimal(_) => Self::Decimal(Decimal::new(data as _, 0)),
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'int32' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_uint64(data: u64, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::UInt64 => Self::UInt64(data),
            DataType::Boolean if data == 0 => Self::Boolean(false),
            DataType::Boolean if data == 1 => Self::Boolean(true),
            DataType::Int8 if data <= i8::MAX as _ => Self::Int8(data as i8),
            DataType::UInt8 if data <= u8::MAX as _ => Self::UInt8(data as u8),
            DataType::Int16 if data <= i16::MAX as _ => Self::Int16(data as i16),
            DataType::UInt16 if data <= u16::MAX as _ => Self::UInt16(data as u16),
            DataType::Int32 if data <= i32::MAX as _ => Self::Int32(data as i32),
            DataType::UInt32 if data <= u32::MAX as _ => Self::UInt32(data as u32),
            DataType::Int64 if data <= i64::MAX as _ => Self::Int64(data as i64),
            DataType::Float32 if (data as f32) as u64 == data => Self::Float32(data as f32),
            DataType::Float64 if (data as f64) as u64 == data => Self::Float64(data as f64),
            DataType::Decimal(_) if data <= i64::MAX as _ => {
                Self::Decimal(Decimal::new(data as _, 0))
            }
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'uint64' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_int64(data: i64, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Int64 => Self::Int64(data),
            DataType::Boolean if data == 0 => Self::Boolean(false),
            DataType::Boolean if data == 1 => Self::Boolean(true),
            DataType::Int8 if data >= i8::MIN as _ && data <= i8::MAX as _ => {
                Self::Int8(data as i8)
            }
            DataType::UInt8 if data >= u8::MIN as _ && data <= u8::MAX as _ => {
                Self::UInt8(data as u8)
            }
            DataType::Int16 if data >= i16::MIN as _ && data <= i16::MAX as _ => {
                Self::Int16(data as i16)
            }
            DataType::UInt16 if data >= u16::MIN as _ && data <= u16::MAX as _ => {
                Self::UInt16(data as u16)
            }
            DataType::Int32 if data >= i32::MIN as _ && data <= i32::MAX as _ => {
                Self::Int32(data as i32)
            }
            DataType::UInt32 if data >= u32::MIN as _ && data <= u32::MAX as _ => {
                Self::UInt32(data as u32)
            }
            DataType::UInt64 if data >= u64::MIN as _ => Self::UInt64(data as u64),
            DataType::Float32 if (data as f32) as i64 == data => Self::Float32(data as f32),
            DataType::Float64 if (data as f64) as i64 == data => Self::Float64(data as f64),
            DataType::Decimal(_) => Self::Decimal(Decimal::new(data as _, 0)),
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'int64' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_decimal(data: Decimal, r#type: &DataType) -> Result<DataValue> {
        if let DataType::Decimal(_) = r#type {
            return Ok(Self::Decimal(data));
        }

        if data.fract().is_zero() {
            match r#type {
                DataType::Boolean if data.is_zero() => return Ok(Self::Boolean(false)),
                DataType::Boolean if data.is_one() => return Ok(Self::Boolean(true)),
                DataType::UInt8 => {
                    if let Some(val) = data.to_u8() {
                        return Ok(DataValue::UInt8(val));
                    }
                }
                DataType::Int8 => {
                    if let Some(val) = data.to_i8() {
                        return Ok(DataValue::Int8(val));
                    }
                }
                DataType::UInt16 => {
                    if let Some(val) = data.to_u16() {
                        return Ok(DataValue::UInt16(val));
                    }
                }
                DataType::Int16 => {
                    if let Some(val) = data.to_i16() {
                        return Ok(DataValue::Int16(val));
                    }
                }
                DataType::UInt32 => {
                    if let Some(val) = data.to_u32() {
                        return Ok(DataValue::UInt32(val));
                    }
                }
                DataType::Int32 => {
                    if let Some(val) = data.to_i32() {
                        return Ok(DataValue::Int32(val));
                    }
                }
                DataType::UInt64 => {
                    if let Some(val) = data.to_u64() {
                        return Ok(DataValue::UInt64(val));
                    }
                }
                DataType::Int64 => {
                    if let Some(val) = data.to_i64() {
                        return Ok(DataValue::Int64(val));
                    }
                }
                _ => {}
            }
        }

        match r#type {
            DataType::Float32 => {
                if let Some(val) = data.to_f32() {
                    return Ok(DataValue::Float32(val));
                }
            }
            DataType::Float64 => {
                if let Some(val) = data.to_f64() {
                    return Ok(DataValue::Float64(val));
                }
            }
            DataType::Utf8String(_) => return Ok(Self::Utf8String(data.to_string())),
            _ => {}
        }

        bail!(
            "No type coercion exists from type 'decimal' ({}) to {:?}",
            data,
            r#type
        )
    }

    fn try_coerce_float32(data: f32, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Float32 => Self::Float32(data),
            DataType::Float64 => Self::Float64(data as f64),
            DataType::Boolean if data == 0.0 => Self::Boolean(false),
            DataType::Boolean if data == 1.0 => Self::Boolean(true),
            DataType::Int8
                if data.trunc() == data && data >= i8::MIN as _ && data <= i8::MAX as _ =>
            {
                Self::Int8(data as i8)
            }
            DataType::UInt8
                if data.trunc() == data && data >= u8::MIN as _ && data <= u8::MAX as _ =>
            {
                Self::UInt8(data as u8)
            }
            DataType::Int16
                if data.trunc() == data && data >= i16::MIN as _ && data <= i16::MAX as _ =>
            {
                Self::Int16(data as i16)
            }
            DataType::UInt16
                if data.trunc() == data && data >= u16::MIN as _ && data <= u16::MAX as _ =>
            {
                Self::UInt16(data as u16)
            }
            DataType::Int32
                if data.trunc() == data && data >= f32::MIN as _ && data <= f32::MAX as _ =>
            {
                Self::Int32(data as i32)
            }
            DataType::UInt32
                if data.trunc() == data && data >= u32::MIN as _ && data <= u32::MAX as _ =>
            {
                Self::UInt32(data as u32)
            }
            DataType::Int64
                if data.trunc() == data && data >= i64::MIN as _ && data <= i64::MAX as _ =>
            {
                Self::Int64(data as i64)
            }
            DataType::UInt64 if data.trunc() == data && data >= u64::MIN as _ => {
                Self::UInt64(data as u64)
            }
            DataType::Decimal(_) if Decimal::from_f32_retain(data).is_some() => {
                Self::Decimal(Decimal::from_f32_retain(data).unwrap())
            }
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'float32' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_float64(data: f64, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Float64 => Self::Float64(data),
            DataType::Float32 if (data as f32) as f64 == data => Self::Float32(data as f32),
            DataType::Boolean if data == 0.0 => Self::Boolean(false),
            DataType::Boolean if data == 1.0 => Self::Boolean(true),
            DataType::Int8
                if data.trunc() == data && data >= i8::MIN as _ && data <= i8::MAX as _ =>
            {
                Self::Int8(data as i8)
            }
            DataType::UInt8
                if data.trunc() == data && data >= u8::MIN as _ && data <= u8::MAX as _ =>
            {
                Self::UInt8(data as u8)
            }
            DataType::Int16
                if data.trunc() == data && data >= i16::MIN as _ && data <= i16::MAX as _ =>
            {
                Self::Int16(data as i16)
            }
            DataType::UInt16
                if data.trunc() == data && data >= u16::MIN as _ && data <= u16::MAX as _ =>
            {
                Self::UInt16(data as u16)
            }
            DataType::Int32
                if data.trunc() == data && data >= f64::MIN as _ && data <= f32::MAX as _ =>
            {
                Self::Int32(data as i32)
            }
            DataType::UInt32
                if data.trunc() == data && data >= u32::MIN as _ && data <= u32::MAX as _ =>
            {
                Self::UInt32(data as u32)
            }
            DataType::Int64
                if data.trunc() == data && data >= i64::MIN as _ && data <= i64::MAX as _ =>
            {
                Self::Int64(data as i64)
            }
            DataType::UInt64 if data.trunc() == data && data >= u64::MIN as _ => {
                Self::UInt64(data as u64)
            }
            DataType::Decimal(_) if Decimal::from_f64_retain(data).is_some() => {
                Self::Decimal(Decimal::from_f64_retain(data).unwrap())
            }
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'float64' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_json(data: String, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::JSON => Self::JSON(data),
            DataType::Utf8String(_) => Self::Utf8String(data),
            DataType::Binary => Self::Binary(data.as_bytes().to_vec()),
            _ => bail!(
                "No type coercion exists from type 'JSON' ({}) to {:?}",
                &data[..cmp::min(50, data.len())],
                r#type
            ),
        })
    }

    fn try_coerce_date(data: NaiveDate, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Date => Self::Date(data),
            DataType::DateTime => Self::DateTime(data.and_hms_opt(0, 0, 0).unwrap()),
            DataType::Utf8String(_) => Self::Utf8String(data.format("%Y-%m-%d").to_string()),
            _ => bail!(
                "No type coercion exists from type 'date' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_time(data: NaiveTime, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Time => Self::Time(data),
            DataType::DateTime => {
                Self::DateTime(NaiveDateTime::new(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(), data))
            }
            DataType::Utf8String(_) => Self::Utf8String(data.format("%H:%M:%S").to_string()),
            _ => bail!(
                "No type coercion exists from type 'time' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_date_time(data: NaiveDateTime, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::DateTime => Self::DateTime(data),
            DataType::Date
                if (data.hour(), data.minute(), data.second(), data.nanosecond())
                    == (0, 0, 0, 0) =>
            {
                DataValue::Date(data.date())
            }
            DataType::Time if (data.year(), data.month(), data.day()) == (1970, 1, 1) => {
                DataValue::Time(data.time())
            }
            DataType::Utf8String(_) => {
                Self::Utf8String(data.format("%Y-%m-%dT%H:%M:%S").to_string())
            }
            _ => bail!(
                "No type coercion exists from type 'date/time' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_date_time_with_tz(data: DateTimeWithTZ, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::DateTimeWithTZ => Self::DateTimeWithTZ(data),
            DataType::Utf8String(_) if data.tz == chrono_tz::UTC => {
                Self::Utf8String(data.zoned()?.to_rfc3339())
            }
            _ => bail!(
                "No type coercion exists from type 'date/time with timezone' ({:?}) to {:?}",
                data,
                r#type
            ),
        })
    }

    fn try_coerce_uuid(data: Uuid, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Uuid => Self::Uuid(data),
            DataType::Utf8String(_) => Self::Utf8String(data.to_string()),
            _ => bail!(
                "No type coercion exists from type 'uuid' ({}) to {:?}",
                data,
                r#type
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono_tz::Tz;

    use super::*;
    use crate::data::*;

    #[test]
    fn test_data_value_coerce_no_data_loss() {
        let test_cases = vec![
            (
                vec![DataValue::Utf8String("Hello world".into())],
                vec![DataType::Binary],
            ),
            (
                vec![DataValue::Binary("Hello world".as_bytes().to_vec())],
                vec![DataType::Utf8String(StringOptions::default())],
            ),
            (
                vec![DataValue::Boolean(true), DataValue::Boolean(false)],
                vec![
                    DataType::Boolean,
                    DataType::Int8,
                    DataType::UInt8,
                    DataType::Int16,
                    DataType::UInt16,
                    DataType::Int32,
                    DataType::UInt32,
                    DataType::UInt64,
                    DataType::Int64,
                    DataType::Float32,
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::UInt8(123),
                    DataValue::Int8(123),
                    DataValue::Int16(123),
                    DataValue::UInt16(123),
                    DataValue::Int32(123),
                    DataValue::UInt32(123),
                    DataValue::Int64(123),
                    DataValue::UInt64(123),
                ],
                vec![
                    DataType::Int8,
                    DataType::UInt8,
                    DataType::Int16,
                    DataType::UInt16,
                    DataType::UInt32,
                    DataType::Int32,
                    DataType::UInt64,
                    DataType::Int64,
                    DataType::Float32,
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::Int8(-123),
                    DataValue::Int16(-123),
                    DataValue::Int32(-123),
                    DataValue::Int64(-123),
                ],
                vec![
                    DataType::Int8,
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Float32,
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::Int16(i16::MAX),
                    DataValue::UInt16(i16::MAX as _),
                    DataValue::Int32(i16::MAX as _),
                    DataValue::UInt32(i16::MAX as _),
                    DataValue::Int64(i16::MAX as _),
                    DataValue::UInt64(i16::MAX as _),
                ],
                vec![
                    DataType::Int16,
                    DataType::UInt16,
                    DataType::UInt32,
                    DataType::Int32,
                    DataType::UInt64,
                    DataType::Int64,
                    DataType::Float32,
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::Int16(i16::MIN),
                    DataValue::Int32(i16::MIN as _),
                    DataValue::Int64(i16::MIN as _),
                ],
                vec![
                    DataType::Int16,
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Float32,
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::Int32(i32::MAX),
                    DataValue::UInt32(i32::MAX as _),
                    DataValue::Int64(i32::MAX as _),
                    DataValue::UInt64(i32::MAX as _),
                ],
                vec![
                    DataType::Int32,
                    DataType::UInt32,
                    DataType::UInt64,
                    DataType::Int64,
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![DataValue::Int32(i32::MIN), DataValue::Int64(i32::MIN as _)],
                vec![
                    DataType::Int32,
                    DataType::Int64,
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::Int64(i64::MAX as _),
                    DataValue::UInt64(i64::MAX as _),
                ],
                vec![
                    DataType::UInt64,
                    DataType::Int64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![DataValue::Int64(i64::MIN)],
                vec![
                    DataType::Int64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![DataValue::Float32(1234.5678)],
                vec![
                    DataType::Float32,
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![DataValue::Float64(98764321.12345)],
                vec![
                    DataType::Float64,
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![DataValue::Decimal(
                    Decimal::from_f64_retain(12345.6789).unwrap(),
                )],
                vec![
                    DataType::Decimal(DecimalOptions::default()),
                    DataType::Float64,
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::Utf8String("{}".into()),
                    DataValue::Utf8String("\"abc\"".into()),
                ],
                vec![
                    DataType::Utf8String(StringOptions::default()),
                    DataType::JSON,
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::JSON("{}".to_string()),
                    DataValue::JSON("\"abc\"".to_string()),
                ],
                vec![
                    DataType::JSON,
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![
                    DataValue::Date(NaiveDate::from_ymd_opt(2020, 10, 25).unwrap()),
                    DataValue::Time(NaiveTime::from_hms_opt(16, 54, 32).unwrap()),
                    DataValue::DateTime(NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(2020, 10, 25).unwrap(),
                        NaiveTime::from_hms_opt(16, 54, 32).unwrap(),
                    )),
                    DataValue::DateTimeWithTZ(DateTimeWithTZ::new(
                        NaiveDateTime::new(
                            NaiveDate::from_ymd_opt(2020, 10, 25).unwrap(),
                            NaiveTime::from_hms_opt(16, 54, 32).unwrap(),
                        ),
                        Tz::UTC,
                    )),
                ],
                vec![
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![DataValue::Uuid(Uuid::new_v4())],
                vec![
                    DataType::Uuid,
                    DataType::Utf8String(StringOptions::default()),
                    DataType::Binary,
                ],
            ),
            (
                vec![DataValue::Date(NaiveDate::from_ymd_opt(2020, 10, 25).unwrap())],
                vec![DataType::DateTime],
            ),
            (
                vec![DataValue::Time(NaiveTime::from_hms_nano_opt(12, 43, 56, 1234).unwrap())],
                vec![DataType::DateTime],
            ),
        ];

        for (vals, types) in test_cases.into_iter() {
            for data in vals {
                for r#type in types.clone() {
                    let orig_type: DataType = (&data).into();

                    assert_eq!(
                        data.clone().try_coerce_into(&orig_type).unwrap(),
                        data,
                        "{:?} must be able to coerce into current type without error",
                        orig_type
                    );

                    let coerced = data.clone().try_coerce_into(&r#type).unwrap();
                    let coerced_type: DataType = (&coerced).into();

                    assert_eq!(
                        coerced_type, r#type,
                        "Unexpected data type returned when coercing {:?} into {:?}",
                        orig_type, r#type
                    );

                    assert_eq!(
                        coerced.clone().try_coerce_into(&orig_type).unwrap(),
                        data,
                        "{:?} type must be able to be coerced to {:?} and back without data loss",
                        orig_type,
                        r#type
                    );
                }
            }
        }
    }

    #[test]
    fn test_data_value_coerce_utf8_string() {
        let data = "Hello".to_string();

        assert_eq!(
            DataValue::Utf8String(data.clone())
                .try_coerce_into(&DataType::Binary)
                .unwrap(),
            DataValue::Binary(data.as_bytes().to_vec())
        );
    }

    #[test]
    fn test_data_value_coerce_utf8_string_to_json() {
        let data = "{\"hello\": \"world\"}".to_string();

        assert_eq!(
            DataValue::Utf8String(data.clone())
                .try_coerce_into(&DataType::JSON)
                .unwrap(),
            DataValue::JSON(data)
        );

        DataValue::Utf8String("INVALID JSON".into())
            .try_coerce_into(&DataType::JSON)
            .unwrap_err();
    }

    #[test]
    fn test_data_value_coerce_binary() {
        let data = "Hello".to_string();
        let invalid_utf8_data = [0u8, 255u8].to_vec();

        assert_eq!(
            DataValue::Binary(data.as_bytes().to_vec().clone())
                .try_coerce_into(&DataType::Utf8String(StringOptions::default()))
                .unwrap(),
            DataValue::Utf8String(data.clone())
        );

        DataValue::Binary(invalid_utf8_data.clone())
            .try_coerce_into(&DataType::Utf8String(StringOptions::default()))
            .unwrap_err();
    }

    #[test]
    fn test_data_value_coerce_boolean() {
        assert_eq!(
            DataValue::Boolean(true)
                .try_coerce_into(&DataType::Int8)
                .unwrap(),
            DataValue::Int8(1)
        );

        assert_eq!(
            DataValue::Boolean(true)
                .try_coerce_into(&DataType::UInt8)
                .unwrap(),
            DataValue::UInt8(1)
        );

        assert_eq!(
            DataValue::Boolean(false)
                .try_coerce_into(&DataType::Int8)
                .unwrap(),
            DataValue::Int8(0)
        );

        assert_eq!(
            DataValue::Boolean(false)
                .try_coerce_into(&DataType::UInt8)
                .unwrap(),
            DataValue::UInt8(0)
        );
    }

    #[test]
    fn test_data_value_coerce_uint8() {
        assert_eq!(
            DataValue::UInt8(0)
                .try_coerce_into(&DataType::Boolean)
                .unwrap(),
            DataValue::Boolean(false)
        );

        DataValue::UInt8(2)
            .try_coerce_into(&DataType::Boolean)
            .unwrap_err();
    }

    #[test]
    fn test_data_value_coerce_int8() {
        assert_eq!(
            DataValue::Int8(1)
                .try_coerce_into(&DataType::Boolean)
                .unwrap(),
            DataValue::Boolean(true)
        );

        DataValue::Int8(12)
            .try_coerce_into(&DataType::Boolean)
            .unwrap_err();
    }

    #[test]
    fn test_data_value_coerce_uint32() {
        assert_eq!(
            DataValue::UInt32(1234)
                .try_coerce_into(&DataType::Int32)
                .unwrap(),
            DataValue::Int32(1234)
        );
        assert_eq!(
            DataValue::UInt32(u32::MAX)
                .try_coerce_into(&DataType::Int64)
                .unwrap(),
            DataValue::Int64(u32::MAX as _)
        );

        DataValue::UInt32(u32::MAX)
            .try_coerce_into(&DataType::Int32)
            .unwrap_err();
    }

    #[test]
    fn test_data_value_coerce_int32() {
        assert_eq!(
            DataValue::Int32(1234)
                .try_coerce_into(&DataType::UInt32)
                .unwrap(),
            DataValue::UInt32(1234)
        );

        DataValue::Int32(-1)
            .try_coerce_into(&DataType::UInt32)
            .unwrap_err();
    }

    #[test]
    fn test_data_value_coerce_uint64() {
        assert_eq!(
            DataValue::UInt64(1234)
                .try_coerce_into(&DataType::Int64)
                .unwrap(),
            DataValue::Int64(1234)
        );

        DataValue::UInt64(u64::MAX)
            .try_coerce_into(&DataType::Int64)
            .unwrap_err();
    }

    #[test]
    fn test_data_value_coerce_int64() {
        assert_eq!(
            DataValue::Int64(1234)
                .try_coerce_into(&DataType::UInt64)
                .unwrap(),
            DataValue::UInt64(1234)
        );
        assert_eq!(
            DataValue::Int64(1234)
                .try_coerce_into(&DataType::UInt32)
                .unwrap(),
            DataValue::UInt32(1234)
        );
        assert_eq!(
            DataValue::Int64(1234)
                .try_coerce_into(&DataType::Int32)
                .unwrap(),
            DataValue::Int32(1234)
        );

        DataValue::Int64(-1)
            .try_coerce_into(&DataType::UInt64)
            .unwrap_err();
        DataValue::Int64(-1)
            .try_coerce_into(&DataType::UInt32)
            .unwrap_err();
        DataValue::Int64(i64::MAX)
            .try_coerce_into(&DataType::UInt32)
            .unwrap_err();
        DataValue::Int64(i64::MIN)
            .try_coerce_into(&DataType::Int32)
            .unwrap_err();
        DataValue::Int64(i64::MAX)
            .try_coerce_into(&DataType::Int32)
            .unwrap_err();
    }

    #[test]
    fn test_data_value_coerce_decimal() {
        assert_eq!(
            DataValue::Decimal(Decimal::new(100, 0))
                .try_coerce_into(&DataType::UInt64)
                .unwrap(),
            DataValue::UInt64(100)
        );

        DataValue::Decimal(Decimal::new(313, 2))
            .try_coerce_into(&DataType::UInt64)
            .unwrap_err();
    }
}

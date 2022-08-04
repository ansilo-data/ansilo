use anyhow::{bail, Result};
use rust_decimal::{prelude::ToPrimitive, Decimal};

use super::{DataType, DataValue};

/// TODO: implement remaining coercions (all types + numeric type expansions)
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
        Ok(match self {
            // Nulls are type-independent
            DataValue::Null => self,
            DataValue::Utf8String(data) => Self::try_coerce_utf8_string(data, r#type)?,
            DataValue::Binary(data) => Self::try_coerce_binary(data, r#type)?,
            DataValue::Boolean(data) => Self::try_coerce_boolean(data, r#type)?,
            DataValue::Int8(data) => Self::try_coerce_int8(data, r#type)?,
            DataValue::UInt8(data) => Self::try_coerce_uint8(data, r#type)?,
            // DataValue::Int16(data) => Self::try_coerce_int16(data, r#type)?,
            // DataValue::UInt16(data) => Self::try_coerce_uint16(data, r#type)?,
            DataValue::Int32(data) => Self::try_coerce_int32(data, r#type)?,
            DataValue::UInt32(data) => Self::try_coerce_uint32(data, r#type)?,
            DataValue::Int64(data) => Self::try_coerce_int64(data, r#type)?,
            DataValue::UInt64(data) => Self::try_coerce_uint64(data, r#type)?,
            // DataValue::Float32(data) => Self::try_coerce_float32(data, r#type)?,
            // DataValue::Float64(data) => Self::try_coerce_float64(data, r#type)?,
            DataValue::Decimal(data) => Self::try_coerce_decimal(data, r#type)?,
            // DataValue::JSON(data) => Self::try_coerce_json(data, r#type)?,
            // DataValue::Date(data) => Self::try_coerce_date(data, r#type)?,
            // DataValue::Time(data) => Self::try_coerce_time(data, r#type)?,
            // DataValue::DateTime(data) => Self::try_coerce_date_time(data, r#type)?,
            // DataValue::DateTimeWithTZ(data) => Self::try_coerce_date_time_with_tz(data, r#type)?,
            // DataValue::Uuid(data) => Self::try_coerce_uuid(data, r#type)?,
            _ => todo!(),
        })
    }

    fn try_coerce_utf8_string(data: Vec<u8>, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Utf8String(_) => Self::Utf8String(data),
            DataType::Binary => Self::Binary(data),
            _ => bail!(
                "No type coercion exists from type 'UTF-8 String' to {:?}",
                r#type
            ),
        })
    }

    fn try_coerce_binary(data: Vec<u8>, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Binary => Self::Binary(data),
            DataType::Utf8String(_) => {
                if let Ok(data) = String::from_utf8(data) {
                    DataValue::Utf8String(data.into_bytes())
                } else {
                    bail!("Failed to coerce binary data into UTF-8 string: data is not valid utf-8 encoded")
                }
            }
            _ => bail!("No type coercion exists from type 'binary' to {:?}", r#type),
        })
    }

    fn try_coerce_boolean(data: bool, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Boolean => Self::Boolean(data),
            DataType::Int8 => Self::Int8(data as i8),
            DataType::UInt8 => Self::UInt8(data as u8),
            _ => bail!(
                "No type coercion exists from type 'boolean' to {:?}",
                r#type
            ),
        })
    }

    fn try_coerce_uint8(data: u8, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::UInt8 => Self::UInt8(data),
            DataType::Boolean => match data {
                0 => Self::Boolean(false),
                1 => Self::Boolean(true),
                _ => bail!("Failed to convert from type 'uint8' to 'boolean': expecting 0 or 1, found {data}")
            }
            _ => bail!(
                "No type coercion exists from type 'uint8' to {:?}",
                r#type
            ),
        })
    }

    fn try_coerce_int8(data: i8, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::Int8 => Self::Int8(data),
            DataType::Boolean => match data {
                0 => Self::Boolean(false),
                1 => Self::Boolean(true),
                _ => bail!("Failed to convert from type 'int8' to 'boolean': expecting 0 or 1, found {data}")
            }
            _ => bail!(
                "No type coercion exists from type 'int8' to {:?}",
                r#type
            ),
        })
    }

    fn try_coerce_uint32(data: u32, r#type: &DataType) -> Result<DataValue> {
        Ok(match r#type {
            DataType::UInt32 => Self::UInt32(data),
            DataType::Int32 if data < i32::MAX as _ => DataValue::Int32(data as i32),
            DataType::Int64 => Self::Int64(data as _),
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
            DataType::UInt32 if data >= 0 => DataValue::UInt32(data as u32),
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
            DataType::Int64 if data < i64::MAX as _ => DataValue::Int64(data as i64),
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
            DataType::UInt64 if data >= 0 => DataValue::UInt64(data as u64),
            DataType::Int32 if data >= i32::MIN as _ && data <= i32::MAX as _ => {
                Self::Int32(data as i32)
            }
            DataType::UInt32 if data >= 0 && data <= u32::MAX as _ => Self::UInt32(data as u32),
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
                DataType::UInt64 => {
                    if let Some(val) = data.to_u64() {
                        return Ok(DataValue::UInt64(val));
                    }
                }
                _ => {}
            }
        }

        bail!(
            "No type coercion exists from type 'decimal' ({}) to {:?}",
            data,
            r#type
        )
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::data::*;

    #[test]
    fn test_data_value_coerce_no_data_loss() {
        let test_cases = vec![
            (
                DataValue::Utf8String("Hello world".as_bytes().to_vec()),
                DataType::Binary,
            ),
            (
                DataValue::Binary("Hello world".as_bytes().to_vec()),
                DataType::Utf8String(StringOptions::default()),
            ),
            (DataValue::Boolean(true), DataType::UInt8),
            (DataValue::Boolean(false), DataType::UInt8),
            (DataValue::UInt8(1), DataType::Boolean),
            (DataValue::Int8(0), DataType::Boolean),
            (DataValue::UInt64(123456), DataType::Int64),
            (DataValue::Int64(123456), DataType::UInt64),
        ];

        for (data, r#type) in test_cases.into_iter() {
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

    #[test]
    fn test_data_value_coerce_utf8_string() {
        let data = "Hello".as_bytes().to_vec();

        assert_eq!(
            DataValue::Utf8String(data.clone())
                .try_coerce_into(&DataType::Binary)
                .unwrap(),
            DataValue::Binary(data.clone())
        );
    }

    #[test]
    fn test_data_value_coerce_binary() {
        let data = "Hello".as_bytes().to_vec();
        let invalid_utf8_data = [0u8, 255u8].to_vec();

        assert_eq!(
            DataValue::Binary(data.clone())
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

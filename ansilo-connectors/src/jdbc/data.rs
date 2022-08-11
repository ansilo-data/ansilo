use ansilo_core::{
    data::{DataType, DecimalOptions, StringOptions},
    err::{self, bail, Result},
};

/// Trait for defining type conversions from rust DataValue/DataType's
/// to their equivalent JDBC types.
///
/// @see ansilo-connectors/src/jdbc/java/src/main/java/com/ansilo/connectors/data/JdbcType.java
/// @see https://docs.oracle.com/cd/E19830-01/819-4721/beajw/index.html
pub trait JdbcTypeMapping: Clone + Send + Sync + 'static {
    /// Convert the rustland data type into the equivalent JDBC data type
    /// (Inverse conversion of the previous impl)
    fn to_jdbc(r#type: &DataType) -> Result<JdbcType> {
        default_type_to_jdbc(r#type)
    }

    /// Converts the supplied jdbc data type id to the equivalent
    /// data type defined in rustland
    fn to_rust(r#type: JdbcType) -> Result<DataType> {
        default_type_to_rust(r#type)
    }
}

/// Default JDBC type mappings
#[derive(Clone)]
pub struct JdbcDefaultTypeMapping;

impl JdbcTypeMapping for JdbcDefaultTypeMapping {}

/// Constants representing JDBC data types.
/// These constants are also defined in java:
/// @see ansilo-connectors/src/jdbc/java/src/main/java/com/ansilo/connectors/data/JdbcType.java
///
/// NB: If you update these, make sure to update the java constants.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
pub enum JdbcType {
    Bit = 1,
    TinyInt = 2,
    SmallInt = 3,
    Integer = 4,
    BigInt = 5,
    Float = 6,
    Real = 7,
    Double = 8,
    Numeric = 9,
    Decimal = 10,
    Char = 11,
    Varchar = 12,
    LongVarchar = 32,
    Date = 13,
    Time = 14,
    Timestamp = 15,
    Binary = 16,
    Null = 17,
    JavaObject = 18,
    Distinct = 19,
    Struct = 20,
    Array = 21,
    Blob = 22,
    Clob = 23,
    Boolean = 24,
    NChar = 25,
    NVarchar = 26,
    LongNVarchar = 27,
    NClob = 28,
    SqlXml = 29,
    TimeWithTimezone = 30,
    TimestampWithTimezone = 31,
}

impl TryFrom<i32> for JdbcType {
    type Error = err::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => JdbcType::Bit,
            2 => JdbcType::TinyInt,
            3 => JdbcType::SmallInt,
            4 => JdbcType::Integer,
            5 => JdbcType::BigInt,
            6 => JdbcType::Float,
            7 => JdbcType::Real,
            8 => JdbcType::Double,
            9 => JdbcType::Numeric,
            10 => JdbcType::Decimal,
            11 => JdbcType::Char,
            12 => JdbcType::Varchar,
            32 => JdbcType::LongVarchar,
            13 => JdbcType::Date,
            14 => JdbcType::Time,
            15 => JdbcType::Timestamp,
            16 => JdbcType::Binary,
            17 => JdbcType::Null,
            18 => JdbcType::JavaObject,
            19 => JdbcType::Distinct,
            20 => JdbcType::Struct,
            21 => JdbcType::Array,
            22 => JdbcType::Blob,
            23 => JdbcType::Clob,
            24 => JdbcType::Boolean,
            25 => JdbcType::NChar,
            26 => JdbcType::NVarchar,
            27 => JdbcType::LongNVarchar,
            28 => JdbcType::NClob,
            29 => JdbcType::SqlXml,
            30 => JdbcType::TimeWithTimezone,
            31 => JdbcType::TimestampWithTimezone,
            _ => bail!("Unrecognized JDBC data type constant: {}", value),
        })
    }
}

pub(crate) fn default_type_to_jdbc(r#type: &DataType) -> Result<JdbcType> {
    Ok(match r#type {
        DataType::Boolean => JdbcType::Bit,
        DataType::Int8 => JdbcType::TinyInt,
        DataType::Int16 => JdbcType::SmallInt,
        DataType::Int32 => JdbcType::Integer,
        DataType::Int64 => JdbcType::BigInt,
        DataType::Float32 => JdbcType::Float,
        DataType::Float64 => JdbcType::Double,
        DataType::Decimal(_) => JdbcType::Decimal,
        DataType::Date => JdbcType::Date,
        DataType::Time => JdbcType::Time,
        DataType::DateTime => JdbcType::Timestamp,
        DataType::Null => JdbcType::Null,
        DataType::JSON => JdbcType::NVarchar,
        DataType::Utf8String(_) => JdbcType::NVarchar,
        DataType::Binary => JdbcType::Blob,
        DataType::DateTimeWithTZ => JdbcType::TimestampWithTimezone,
        DataType::Uuid => JdbcType::Varchar,
        _ => bail!("{:?} is not supported", r#type),
    })
}

pub(crate) fn default_type_to_rust(r#type: JdbcType) -> Result<DataType> {
    Ok(match r#type {
        JdbcType::Bit => DataType::Boolean,
        JdbcType::TinyInt => DataType::Int8,
        JdbcType::SmallInt => DataType::Int16,
        JdbcType::Integer => DataType::Int32,
        JdbcType::BigInt => DataType::Int64,
        JdbcType::Float => DataType::Float32,
        JdbcType::Real => DataType::Float32,
        JdbcType::Double => DataType::Float64,
        JdbcType::Numeric => DataType::Int64,
        JdbcType::Decimal => DataType::Decimal(DecimalOptions::default()),
        JdbcType::Varchar | JdbcType::Char | JdbcType::LongVarchar => {
            DataType::Utf8String(StringOptions::default())
        }
        JdbcType::Date => DataType::Date,
        JdbcType::Time => DataType::Time,
        JdbcType::Timestamp => DataType::DateTime,
        JdbcType::Binary => DataType::Binary,
        JdbcType::Null => DataType::Null,
        JdbcType::JavaObject => DataType::JSON,
        JdbcType::Distinct => DataType::Utf8String(StringOptions::default()),
        JdbcType::Struct => DataType::JSON,
        JdbcType::Array => DataType::JSON,
        JdbcType::Blob => DataType::Binary,
        JdbcType::Clob => DataType::Binary,
        JdbcType::Boolean => DataType::Boolean,
        JdbcType::NChar | JdbcType::NVarchar | JdbcType::LongNVarchar => {
            DataType::Utf8String(StringOptions::default())
        }
        JdbcType::NClob => DataType::Binary,
        JdbcType::SqlXml => bail!("SQLXML type is not supported"),
        JdbcType::TimeWithTimezone => DataType::DateTimeWithTZ,
        JdbcType::TimestampWithTimezone => DataType::DateTimeWithTZ,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jdbc_data_type_conversions() {
        let data_type = [
            DataType::Utf8String(StringOptions::default()),
            DataType::Binary,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal(DecimalOptions::default()),
            DataType::Date,
            DataType::Time,
            DataType::DateTime,
            DataType::DateTimeWithTZ,
        ];

        for dt in data_type.into_iter() {
            assert_eq!(
                default_type_to_rust(default_type_to_jdbc(&dt).unwrap()).unwrap(),
                dt
            );
        }
    }
}

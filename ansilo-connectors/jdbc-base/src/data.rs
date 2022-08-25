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
    fn to_jdbc(r#type: &DataType) -> Result<JavaDataType> {
        default_type_to_jdbc(r#type)
    }

    /// Converts the supplied jdbc data type id to the equivalent
    /// data type defined in rustland
    fn to_rust(r#type: JavaDataType) -> Result<DataType> {
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
pub enum JavaDataType {
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    Float32 = 5,
    Float64 = 6,
    Decimal = 7,
    Date = 8,
    Time = 9,
    DateTime = 10,
    DateTimeWithTz = 11,
    Binary = 12,
    Null = 13,
    Boolean = 14,
    Utf8String = 15,
    Json = 16,
}

impl TryFrom<i32> for JavaDataType {
    type Error = err::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => Int8,
            2 => Int16,
            3 => Int32,
            4 => Int64,
            5 => Float32,
            6 => Float64,
            7 => Decimal,
            8 => Date,
            9 => Time,
            10 => DateTime,
            11 => DateTimeWithTz,
            12 => Binary,
            13 => Null,
            14 => Boolean,
            15 => Utf8String,
            16 => Json,
            _ => bail!("Unrecognized JDBC data type constant: {}", value),
        })
    }
}

impl From<DataType> for JavaDataType {
    fn from(r#type: DataType) -> Self {
        match r#type {
            DataType::Boolean => JavaDataType::Boolean,
            DataType::Int8 => JavaDataType::TinyInt,
            DataType::Int16 => JavaDataType::SmallInt,
            DataType::Int32 => JavaDataType::Integer,
            DataType::Int64 => JavaDataType::BigInt,
            DataType::Float32 => JavaDataType::Float,
            DataType::Float64 => JavaDataType::Double,
            DataType::Decimal(_) => JavaDataType::Decimal,
            DataType::Date => JavaDataType::Date,
            DataType::Time => JavaDataType::Time,
            DataType::DateTime => JavaDataType::Timestamp,
            DataType::Null => JavaDataType::Null,
            DataType::JSON => JavaDataType::Json,
            DataType::Utf8String(_) => JavaDataType::NVarchar,
            DataType::Binary => JavaDataType::Blob,
            DataType::DateTimeWithTZ => JavaDataType::TimestampWithTimezone,
            DataType::Uuid => JavaDataType::Varchar,
            _ => bail!("{:?} is not supported", r#type),
        }
    }
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
            DataType::JSON,
        ];

        for dt in data_type.into_iter() {
            assert_eq!(
                default_type_to_rust(default_type_to_jdbc(&dt).unwrap()).unwrap(),
                dt
            );
        }
    }
}

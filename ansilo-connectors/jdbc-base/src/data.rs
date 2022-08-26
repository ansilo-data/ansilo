use ansilo_core::{
    data::{DataType, DecimalOptions, StringOptions},
    err::{self, bail, Result},
};

/// Constants representing JDBC data types.
/// These constants are also defined in java:
/// @see ansilo-connectors/src/jdbc/java/src/main/java/com/ansilo/connectors/data/JdbcType.java
///
/// NB: If you update these, make sure to update the java constants.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
pub enum JavaDataType {
    Int8 = 1,
    UInt8 = 2,
    Int16 = 3,
    UInt16 = 4,
    Int32 = 5,
    UInt32 = 6,
    Int64 = 7,
    UInt64 = 8,
    Float32 = 9,
    Float64 = 10,
    Decimal = 11,
    Date = 12,
    Time = 13,
    DateTime = 14,
    DateTimeWithTZ = 15,
    Binary = 16,
    Null = 17,
    Boolean = 18,
    Utf8String = 19,
    JSON = 20,
    Uuid = 21,
}

impl TryFrom<i32> for JavaDataType {
    type Error = err::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => Self::Int8,
            2 => Self::UInt8,
            3 => Self::Int16,
            4 => Self::UInt16,
            5 => Self::Int32,
            6 => Self::UInt32,
            7 => Self::Int64,
            8 => Self::UInt64,
            9 => Self::Float32,
            10 => Self::Float64,
            11 => Self::Decimal,
            12 => Self::Date,
            13 => Self::Time,
            14 => Self::DateTime,
            15 => Self::DateTimeWithTZ,
            16 => Self::Binary,
            17 => Self::Null,
            18 => Self::Boolean,
            19 => Self::Utf8String,
            20 => Self::JSON,
            21 => Self::Uuid,
            _ => bail!("Unrecognized JDBC data type constant: {}", value),
        })
    }
}

impl From<&DataType> for JavaDataType {
    fn from(r#type: &DataType) -> Self {
        match r#type {
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::UInt8 => Self::UInt8,
            DataType::Int16 => Self::Int16,
            DataType::UInt16 => Self::UInt16,
            DataType::Int32 => Self::Int32,
            DataType::UInt32 => Self::UInt32,
            DataType::Int64 => Self::Int64,
            DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Decimal(_) => Self::Decimal,
            DataType::Date => Self::Date,
            DataType::Time => Self::Time,
            DataType::DateTime => Self::DateTime,
            DataType::Null => Self::Null,
            DataType::JSON => Self::JSON,
            DataType::Utf8String(_) => Self::Utf8String,
            DataType::Binary => Self::Binary,
            DataType::DateTimeWithTZ => Self::DateTimeWithTZ,
            DataType::Uuid => Self::Uuid,
        }
    }
}

impl Into<DataType> for JavaDataType {
    fn into(self) -> DataType {
        match self {
            JavaDataType::Int8 => DataType::Int8,
            JavaDataType::UInt8 => DataType::UInt8,
            JavaDataType::Int16 => DataType::Int16,
            JavaDataType::UInt16 => DataType::UInt16,
            JavaDataType::Int32 => DataType::Int32,
            JavaDataType::UInt32 => DataType::UInt32,
            JavaDataType::Int64 => DataType::Int64,
            JavaDataType::UInt64 => DataType::UInt64,
            JavaDataType::Float32 => DataType::Float32,
            JavaDataType::Float64 => DataType::Float64,
            JavaDataType::Decimal => DataType::Decimal(DecimalOptions::default()),
            JavaDataType::Date => DataType::Date,
            JavaDataType::Time => DataType::Time,
            JavaDataType::DateTime => DataType::DateTime,
            JavaDataType::DateTimeWithTZ => DataType::DateTimeWithTZ,
            JavaDataType::Binary => DataType::Binary,
            JavaDataType::Null => DataType::Null,
            JavaDataType::Boolean => DataType::Boolean,
            JavaDataType::Utf8String => DataType::Utf8String(StringOptions::default()),
            JavaDataType::JSON => DataType::JSON,
            JavaDataType::Uuid => DataType::Uuid,
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
            DataType::UInt8,
            DataType::Int16,
            DataType::UInt16,
            DataType::Int32,
            DataType::UInt32,
            DataType::Int64,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal(DecimalOptions::default()),
            DataType::Date,
            DataType::Time,
            DataType::DateTime,
            DataType::DateTimeWithTZ,
            DataType::JSON,
            DataType::Uuid,
        ];

        for dt in data_type.into_iter() {
            let converted: DataType = JavaDataType::try_from(JavaDataType::from(&dt) as i32)
                .unwrap()
                .into();
            assert_eq!(converted, dt);
        }
    }
}

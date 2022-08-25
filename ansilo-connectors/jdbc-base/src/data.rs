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
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    Float32 = 5,
    Float64 = 6,
    Decimal = 7,
    Date = 8,
    Time = 9,
    DateTime = 10,
    DateTimeWithTZ = 11,
    Binary = 12,
    Null = 13,
    Boolean = 14,
    Utf8String = 15,
    JSON = 16,
}

impl TryFrom<i32> for JavaDataType {
    type Error = err::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => Self::Int8,
            2 => Self::Int16,
            3 => Self::Int32,
            4 => Self::Int64,
            5 => Self::Float32,
            6 => Self::Float64,
            7 => Self::Decimal,
            8 => Self::Date,
            9 => Self::Time,
            10 => Self::DateTime,
            11 => Self::DateTimeWithTZ,
            12 => Self::Binary,
            13 => Self::Null,
            14 => Self::Boolean,
            15 => Self::Utf8String,
            16 => Self::JSON,
            _ => bail!("Unrecognized JDBC data type constant: {}", value),
        })
    }
}

impl From<&DataType> for JavaDataType {
    fn from(r#type: &DataType) -> Self {
        match r#type {
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
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
            // DataType::Uuid => Self::Uuid,
            _ => panic!("TODO: remaining types"),
        }
    }
}

impl Into<DataType> for JavaDataType {
    fn into(self) -> DataType {
        match self {
            JavaDataType::Int8 => DataType::Int8,
            JavaDataType::Int16 => DataType::Int16,
            JavaDataType::Int32 => DataType::Int32,
            JavaDataType::Int64 => DataType::Int64,
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
            let converted: DataType = JavaDataType::try_from(JavaDataType::from(&dt) as i32)
                .unwrap()
                .into();
            assert_eq!(converted, dt);
        }
    }
}

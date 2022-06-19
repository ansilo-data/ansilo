use ansilo_core::{
    common::data::{DataType, DecimalOptions, EncodingType, VarcharOptions},
    err::{self, bail, Result},
};

#[derive(Debug, Clone, PartialEq)]
pub struct JdbcDataType(pub DataType);

/// Converts the supplied jdbc data type id to the equivalent
/// data type defined in rustland
///
/// @see ansilo-connectors/src/jdbc/java/src/main/java/com/ansilo/connectors/data/JdbcDataType.java
/// @see https://docs.oracle.com/cd/E19830-01/819-4721/beajw/index.html
impl TryFrom<i32> for JdbcDataType {
    type Error = err::Error;

    fn try_from(value: i32) -> Result<Self> {
        let data_type = match value {
            1 => DataType::Boolean,
            2 => DataType::Int8,
            3 => DataType::Int16,
            4 => DataType::Int32,
            5 => DataType::Int64,
            6 => DataType::FloatSingle,
            7 => DataType::FloatSingle,
            8 => DataType::FloatDouble,
            9 => DataType::Int64,
            10 => DataType::Decimal(DecimalOptions::default()),
            11 => DataType::Varchar(VarcharOptions::new(None, EncodingType::Ascii)),
            12 => DataType::Varchar(VarcharOptions::new(None, EncodingType::Ascii)),
            13 => DataType::Date,
            14 => DataType::Time,
            15 => DataType::DateTime,
            16 => DataType::DateTime,
            17 => DataType::Boolean, // TODO: verify
            18 => DataType::JSON,
            19 => DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
            20 => DataType::JSON,
            21 => DataType::JSON,
            22 => DataType::Binary,
            23 => DataType::Binary,
            24 => DataType::Boolean,
            25 | 26 | 27 => DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
            28 => DataType::Binary,
            29 => bail!("SQLXML is not a supported data type"),
            30 => DataType::DateTimeWithTZ,
            31 => DataType::DateTimeWithTZ,
            _ => bail!("Unknown JDBC data type id: {}", value),
        };

        Ok(Self(data_type))
    }
}

/// Convert the rustland data type into the equivalent JDBC data type
/// (Inverse conversion of the previous impl)
impl TryInto<i32> for JdbcDataType {
    type Error = err::Error;

    fn try_into(self) -> Result<i32> {
        let id = match self.0 {
            DataType::Varchar(_) => 26,
            DataType::Binary => 22,
            DataType::Boolean => 1,
            DataType::Int8 => 2,
            DataType::UInt8 => bail!("UInt8 not supported"),
            DataType::Int16 => 3,
            DataType::UInt16 => bail!("UInt16 not supported"),
            DataType::Int32 => 4,
            DataType::UInt32 => bail!("UInt32 not supported"),
            DataType::Int64 => 5,
            DataType::UInt64 => bail!("UInt64 not supported"),
            DataType::FloatSingle => 6,
            DataType::FloatDouble => 8,
            DataType::Decimal(_) => 10,
            DataType::JSON => 26,
            DataType::Date => 13,
            DataType::Time => 14,
            DataType::DateTime => 15,
            DataType::DateTimeWithTZ => 31,
            DataType::Uuid => 12,
        };

        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jdbc_data_type_conversions() {
        let data_type = [
            DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8)),
            DataType::Binary,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::FloatSingle,
            DataType::FloatDouble,
            DataType::Decimal(DecimalOptions::default()),
            DataType::Date,
            DataType::Time,
            DataType::DateTime,
            DataType::DateTimeWithTZ,
        ];

        for dt in data_type.into_iter() {
            assert_eq!(
                TryInto::<i32>::try_into(JdbcDataType(dt.clone()))
                    .and_then(JdbcDataType::try_from)
                    .unwrap()
                    .0,
                dt
            );
        }
    }
}

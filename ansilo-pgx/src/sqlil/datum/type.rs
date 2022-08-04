use ansilo_core::{
    data::{DataType, DecimalOptions, StringOptions},
    err::{bail, Result},
};
use pgx::pg_sys;

/// Converts the supplied postgres type oid to the equivalent mapped DataType
pub fn from_pg_type(type_oid: pg_sys::Oid) -> Result<DataType> {
    match type_oid {
        pg_sys::INT2OID => Ok(DataType::Int16),
        pg_sys::INT4OID => Ok(DataType::Int32),
        pg_sys::INT8OID => Ok(DataType::Int64),
        pg_sys::FLOAT4OID => Ok(DataType::Float32),
        pg_sys::FLOAT8OID => Ok(DataType::Float64),
        pg_sys::NUMERICOID => Ok(DataType::Decimal(DecimalOptions::new(None, None))),
        // We only support UTF8 postgres
        pg_sys::VARCHAROID | pg_sys::TEXTOID => Ok(DataType::Utf8String(StringOptions::default())),
        pg_sys::CHAROID => {
            bail!("Postgres CHAR types are not supported, use another integer or character type")
        }
        //
        pg_sys::BYTEAOID => Ok(DataType::Binary),
        //
        pg_sys::BOOLOID => Ok(DataType::Boolean),
        //
        pg_sys::JSONOID => Ok(DataType::JSON),
        pg_sys::JSONBOID => Ok(DataType::JSON),
        //
        pg_sys::DATEOID => Ok(DataType::Date),
        pg_sys::TIMEOID => Ok(DataType::Time),
        pg_sys::TIMESTAMPOID => Ok(DataType::DateTime),
        pg_sys::TIMESTAMPTZOID => Ok(DataType::DateTimeWithTZ),
        //
        pg_sys::UUIDOID => Ok(DataType::Uuid),
        _ => bail!("Unknown type oid: {type_oid}"),
    }
}

/// Converts the supplied DataType to the equivalent mapped postgres type oid
pub fn into_pg_type(r#type: &DataType) -> Result<pg_sys::Oid> {
    match r#type {
        DataType::Int8 => Ok(pg_sys::INT2OID),
        DataType::Int16 => Ok(pg_sys::INT2OID),
        DataType::Int32 => Ok(pg_sys::INT4OID),
        DataType::Int64 => Ok(pg_sys::INT8OID),
        DataType::UInt8 => Ok(pg_sys::INT2OID),
        DataType::UInt16 => Ok(pg_sys::INT4OID),
        DataType::UInt32 => Ok(pg_sys::INT8OID),
        DataType::UInt64 => Ok(pg_sys::NUMERICOID),
        DataType::Float32 => Ok(pg_sys::FLOAT4OID),
        DataType::Float64 => Ok(pg_sys::FLOAT8OID),
        DataType::Decimal(_) => Ok(pg_sys::NUMERICOID),
        DataType::Utf8String(_) => Ok(pg_sys::VARCHAROID),
        //
        DataType::Binary => Ok(pg_sys::BYTEAOID),
        //
        DataType::Boolean => Ok(pg_sys::BOOLOID),
        //
        DataType::JSON => Ok(pg_sys::JSONOID),
        //
        DataType::Date => Ok(pg_sys::DATEOID),
        DataType::Time => Ok(pg_sys::TIMEOID),
        DataType::DateTime => Ok(pg_sys::TIMESTAMPOID),
        DataType::DateTimeWithTZ => Ok(pg_sys::TIMESTAMPTZOID),
        //
        DataType::Uuid => Ok(pg_sys::UUIDOID),
        DataType::Null => Ok(pg_sys::UNKNOWNOID),
    }
}

#[cfg(test)]
mod pg_tests {
    use super::*;

    #[test]
    fn test_sqlil_type_from_pg_type() {
        assert_eq!(from_pg_type(pg_sys::INT2OID).unwrap(), DataType::Int16);
        assert_eq!(from_pg_type(pg_sys::UUIDOID).unwrap(), DataType::Uuid);
    }

    #[test]
    fn test_sqlil_type_into_pg_type() {
        assert_eq!(into_pg_type(&DataType::Int16).unwrap(), pg_sys::INT2OID);
        assert_eq!(into_pg_type(&DataType::Uuid).unwrap(), pg_sys::UUIDOID);
    }
}

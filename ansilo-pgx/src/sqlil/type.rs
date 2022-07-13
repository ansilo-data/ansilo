use ansilo_core::{
    common::data::{DataType, DecimalOptions, EncodingType, VarcharOptions},
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
        pg_sys::VARCHAROID | pg_sys::TEXTOID => Ok(DataType::Varchar(VarcharOptions::new(
            None,
            EncodingType::Utf8,
        ))),
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

#[cfg(test)]
mod pg_tests {
    use super::*;

    #[test]
    fn test_sqlil_type_from_pg_type() {
        assert_eq!(from_pg_type(pg_sys::INT2OID).unwrap(), DataType::Int16);
        assert_eq!(from_pg_type(pg_sys::UUIDOID).unwrap(), DataType::Uuid);
    }
}

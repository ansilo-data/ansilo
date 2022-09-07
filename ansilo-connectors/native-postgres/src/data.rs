use ansilo_core::{
    data::{chrono_tz::Tz, DataType, DataValue, DateTimeWithTZ},
    err::{bail, Result},
};
use tokio_postgres::{
    types::{ToSql, Type},
    Row,
};

/// Mapping between a DataType and postgres type
pub fn to_pg_type(r#type: &DataType) -> Type {
    match r#type {
        DataType::Utf8String(_) => Type::TEXT,
        DataType::Binary => Type::BYTEA,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::INT2,
        DataType::UInt8 => Type::INT2,
        DataType::Int16 => Type::INT2,
        DataType::UInt16 => Type::INT4,
        DataType::Int32 => Type::INT4,
        DataType::UInt32 => Type::INT8,
        DataType::Int64 => Type::INT8,
        DataType::UInt64 => Type::NUMERIC,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Decimal(_) => Type::NUMERIC,
        DataType::JSON => Type::JSON,
        DataType::Date => Type::DATE,
        DataType::Time => Type::TIME,
        DataType::DateTime => Type::TIMESTAMP,
        DataType::DateTimeWithTZ => Type::TIMESTAMPTZ,
        DataType::Uuid => Type::UUID,
        DataType::Null => Type::ANY,
    }
}

/// Mapping from pg type to DataType
pub fn from_pg_type(r#type: &Type) -> Result<DataType> {
    Ok(match *r#type {
        Type::TEXT | Type::VARCHAR => DataType::Utf8String(Default::default()),
        Type::BYTEA | Type::VARBIT => DataType::Binary,
        Type::BOOL | Type::BIT => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::NUMERIC => DataType::Decimal(Default::default()),
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::JSON | Type::JSONB => DataType::JSON,
        Type::DATE => DataType::Date,
        Type::TIME => DataType::Time,
        Type::TIMESTAMP => DataType::DateTime,
        Type::TIMESTAMPTZ => DataType::DateTimeWithTZ,
        Type::UUID => DataType::Uuid,
        _ => bail!("Postgres type {} is not supported", r#type),
    })
}

/// Converts a DataValue into the supplied postgres type
pub fn to_pg(val: DataValue, r#type: &Type) -> Result<Box<dyn ToSql>> {
    // Coerce to the desired type if necessary
    let val = val.try_coerce_into(&from_pg_type(r#type)?)?;

    Ok(match val {
        DataValue::Null => Box::new(Option::<bool>::None),
        DataValue::Utf8String(d) => Box::new(d),
        DataValue::Binary(_) => todo!(),
        DataValue::Boolean(_) => todo!(),
        DataValue::Int8(_) => todo!(),
        DataValue::UInt8(_) => todo!(),
        DataValue::Int16(_) => todo!(),
        DataValue::UInt16(_) => todo!(),
        DataValue::Int32(_) => todo!(),
        DataValue::UInt32(_) => todo!(),
        DataValue::Int64(_) => todo!(),
        DataValue::UInt64(_) => todo!(),
        DataValue::Float32(_) => todo!(),
        DataValue::Float64(_) => todo!(),
        DataValue::Decimal(_) => todo!(),
        DataValue::JSON(_) => todo!(),
        DataValue::Date(_) => todo!(),
        DataValue::Time(_) => todo!(),
        DataValue::DateTime(_) => todo!(),
        DataValue::DateTimeWithTZ(_) => todo!(),
        DataValue::Uuid(_) => todo!(),
    })
}

/// Converts a DataValue into the supplied postgres type
pub fn from_pg(row: &Row, idx: usize, r#type: &Type) -> Result<DataValue> {
    Ok(match from_pg_type(r#type)? {
        DataType::Utf8String(_) => DataValue::Utf8String(row.try_get(idx)?),
        DataType::Binary => DataValue::Binary(row.try_get(idx)?),
        DataType::Boolean => DataValue::Boolean(row.try_get(idx)?),
        DataType::Int8 => DataValue::Int8(row.try_get(idx)?),
        DataType::Int16 => DataValue::Int16(row.try_get(idx)?),
        DataType::Int32 => DataValue::Int32(row.try_get(idx)?),
        DataType::Int64 => DataValue::Int64(row.try_get(idx)?),
        DataType::Float32 => DataValue::Float32(row.try_get(idx)?),
        DataType::Float64 => DataValue::Float64(row.try_get(idx)?),
        DataType::Decimal(_) => DataValue::Decimal(row.try_get(idx)?),
        DataType::JSON => DataValue::JSON(row.try_get(idx)?),
        DataType::Date => DataValue::Date(row.try_get(idx)?),
        DataType::Time => DataValue::Time(row.try_get(idx)?),
        DataType::DateTime => DataValue::DateTime(row.try_get(idx)?),
        DataType::DateTimeWithTZ => {
            DataValue::DateTimeWithTZ(DateTimeWithTZ::new(row.try_get(idx)?, Tz::UTC))
        }
        DataType::Uuid => DataValue::Uuid(row.try_get(idx)?),
        DataType::Null => DataValue::Null,
        DataType::UInt8 => unreachable!(),
        DataType::UInt16 => unreachable!(),
        DataType::UInt32 => unreachable!(),
        DataType::UInt64 => unreachable!(),
    })
}

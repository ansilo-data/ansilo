use ansilo_core::{
    data::{rust_decimal::prelude::ToPrimitive, DataValue},
    err::Context,
};
use mongodb::bson::{self, spec::BinarySubtype, Binary, Bson};

/// Converts the mongodb bson into extjson representation
pub fn doc_to_json(doc: Bson) -> Result<serde_json::Value> {
    Ok(doc.into_relaxed_extjson())
}

/// Converts the supplied extjson to mongodb bson
pub fn json_to_doc(json: serde_json::Value) -> Result<Bson> {
    Bson::try_from(json.clone())
        .with_context(|| format!("Failed to convert json to bson: {:?}", json))
}

/// Converts a DataValue to a bson
pub fn val_to_bson(val: DataValue) -> Result<Bson> {
    let res = match val {
        DataValue::Null => Bson::Null,
        DataValue::Utf8String(v) => Bson::String(v),
        DataValue::Binary(b) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: b,
        }),
        DataValue::Boolean(b) => Bson::Boolean(b),
        DataValue::Int8(v) => Bson::Int32(v as _),
        DataValue::UInt8(v) => Bson::Int32(v as _),
        DataValue::Int16(v) => Bson::Int32(v as _),
        DataValue::UInt16(v) => Bson::Int32(v as _),
        DataValue::Int32(v) => Bson::Int32(v),
        DataValue::UInt32(v) => Bson::Int64(v as _),
        DataValue::Int64(v) => Bson::Int64(v),
        DataValue::UInt64(v) if v <= i64::MAX => Bson::Int64(v as _),
        DataValue::UInt64(v) => Bson::Double(v as f64),
        DataValue::Float32(v) => Bson::Double(v as _),
        DataValue::Float64(v) => Bson::Double(v),
        DataValue::Decimal(v) if v.fract().is_zero() && v.to_i64().is_some() => {
            Bson::Int64(v.to_i64().unwrap())
        }
        DataValue::Decimal(v) if v.to_f64().is_some() => Bson::Float64(v.to_f64().unwrap()),
        DataValue::Decimal(v) => Bson::String(v.to_string()),
        DataValue::JSON(v) => Bson::try_from(
            serde_json::from_str::<serde_json::Value>(&v).context("Failed to parse json")?,
        )
        .context("Failed to convert json to bson")?,
        DataValue::Date(date) => Bson::String(format!(date, "%Y-%m-%d")),
        DataValue::Time(time) => Bson::String(format!(time, "%H:%M:%S%.6f")),
        DataValue::DateTime(dt) => Bson::String(format!(dt, "%Y-%m-%dT%H:%M:%S")),
        DataValue::DateTimeWithTZ(dt) => Bson::DateTime(bson::DateTime::from_chrono(dt.utc()?)?),
        DataValue::Uuid(uuid) => Bson::String(uuid.to_string()),
    };

    Ok(res)
}

use anyhow::Result;

use super::{DataValue, DataType};

impl DataValue {
    /// Tries to coerce the data value supplied type.
    /// 
    /// In order to ensure we do not allow users to lose data through accidental
    /// coercion we enforce that the rule: 
    ///     COERCE(COERCE(A, NEW_TYPE), ORIG_TYPE) == A
    ///
    /// If this cannot hold due to data being discarded during the coercion we
    /// MUST bail out here.
    fn try_coerce_into(self, r#type: DataType) -> Result<Self> {
        Ok(match self {
            // Nulls are type-independent
            DataValue::Null => self,
            DataValue::Utf8String(_) => todo!(),
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
}
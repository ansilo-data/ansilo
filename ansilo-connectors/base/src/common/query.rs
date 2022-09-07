use ansilo_core::{
    data::{DataType, DataValue},
    sqlil,
};
use enum_as_inner::EnumAsInner;
use serde::Serialize;

use crate::interface::QueryInputStructure;

/// A query parameter
#[derive(Debug, Clone, PartialEq, Serialize, EnumAsInner)]
pub enum QueryParam {
    /// A dynamic query parameter that can modified for every query execution
    ///
    /// These query parameters are written to the QueryHandle before the query is executed.
    Dynamic(sqlil::Parameter),
    /// A constant query parameter that is immutable across executions
    Constant(DataValue),
}

impl QueryParam {
    pub fn dynamic(param: sqlil::Parameter) -> Self {
        Self::Dynamic(param)
    }

    pub fn dynamic2(id: u32, r#type: DataType) -> Self {
        Self::Dynamic(sqlil::Parameter::new(r#type, id))
    }

    pub fn constant(param: DataValue) -> Self {
        Self::Constant(param)
    }

    /// Gets the type of the query parameter
    pub fn r#type(&self) -> DataType {
        match self {
            QueryParam::Dynamic(p) => p.r#type.clone(),
            QueryParam::Constant(v) => v.r#type(),
        }
    }
}

/// Conversion of array of query parameters in the equivalent
/// query input structure that filters to dynamic parameters.
/// (Constant parameters are not written to queries).
impl<T: AsRef<[QueryParam]>> From<T> for QueryInputStructure {
    fn from(params: T) -> Self {
        Self::new(
            params
                .as_ref()
                .iter()
                .filter_map(|p| p.as_dynamic().map(|p| p))
                .map(|p| (p.id, p.r#type.clone()))
                .collect(),
        )
    }
}

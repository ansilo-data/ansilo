use std::collections::HashMap;

use ansilo_core::{data::DataType, err::Result};
use bincode::{Decode, Encode};

use super::ResultSet;

/// A query which is executing
pub trait QueryHandle {
    type TResultSet: ResultSet;

    /// Gets the types of the input expected by the query
    fn get_structure(&self) -> Result<QueryInputStructure>;

    /// Writes query parameter data to the underlying query
    /// Returns the number of bytes written
    fn write(&mut self, buff: &[u8]) -> Result<usize>;

    /// Restarts the query, so new query parameters can be written
    fn restart(&mut self) -> Result<()>;

    /// Executes the query, returning the generated result set
    fn execute_query(&mut self) -> Result<Self::TResultSet>;

    /// Executes the query, returning the number of affected rows, if known
    fn execute_modify(&mut self) -> Result<Option<u64>>;

    /// Returns a loggable representation of the query
    fn logged(&self) -> Result<LoggedQuery>;
}

/// The structure of data expected by a query
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct QueryInputStructure {
    /// The list of query parameter ids and their associated data types
    ///
    /// The parameters are to be written to the query in the order they appear in the vector.
    /// A parameter with the same id can appear multiple times.
    pub params: Vec<(u32, DataType)>,
}

impl QueryInputStructure {
    pub fn new(params: Vec<(u32, DataType)>) -> Self {
        Self { params }
    }

    pub fn types(&self) -> Vec<DataType> {
        self.params.iter().map(|(_, t)| t.clone()).collect()
    }
}

/// A string representation of a query, used mainly for logging
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct LoggedQuery {
    query: String,
    params: Vec<String>,
    other: HashMap<String, String>,
}

impl LoggedQuery {
    pub fn new(
        query: impl Into<String>,
        params: Vec<String>,
        other: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            query: query.into(),
            params,
            other: other.unwrap_or_default(),
        }
    }

    pub fn new_query(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            params: vec![],
            other: HashMap::new(),
        }
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn params(&self) -> &Vec<String> {
        &self.params
    }

    pub fn other(&self) -> &HashMap<String, String> {
        &self.other
    }

    pub fn other_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.other
    }
}

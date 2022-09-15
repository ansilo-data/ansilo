use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum QueryResponse {
    #[serde(rename = "success")]
    Success(QueryResults),
    #[serde(rename = "error")]
    Error(QueryError),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResults {
    pub columns: Vec<(String, String)>,
    pub data: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryError {
    pub message: String,
}

impl From<String> for QueryError {
    fn from(message: String) -> Self {
        Self { message }
    }
}

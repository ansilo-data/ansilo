use serde::{Serialize, Deserialize};

/// Networking options for the node
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct NetworkingConfig {
    /// The listening port of the node
    pub port: u16
    // TODO
}

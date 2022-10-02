use serde::{Deserialize, Serialize};

const DEFAULT_MEMORY: u32 = 512;
const DEFAULT_CONNECTIONS: u32 = 10;

/// Configuration options for resource allocation
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceConfig {
    /// Total memory capacity in megabytes.
    /// This is used a guide for configuring our subsystems but is not by any means
    /// a hard limit. If a hard limit is required the user should configure the appropriate
    /// ulimit.
    ///
    /// We have the following allocation of the total memory:
    /// - 1/2 of total memory goes to postgres
    /// - 1/3 of total memory goes to the JVM
    /// - Remaining 1/6 is used to for rust
    pub memory: Option<u32>,
    /// Maximum connections to postgres
    pub connections: Option<u32>,
}

impl ResourceConfig {
    /// Gets the memory capacity in megabytes for this instance
    pub fn total_memory(&self) -> u32 {
        self.memory.unwrap_or(DEFAULT_MEMORY)
    }

    /// Gets the number of connections to postgres
    pub fn connections(&self) -> u32 {
        self.connections.unwrap_or(DEFAULT_CONNECTIONS)
    }

    /// Gets the memory allocated to the jvm in megabytes
    pub fn jvm_memory_mb(&self) -> u32 {
        self.total_memory() / 3
    }

    /// Gets the memory allocated to postgres
    pub fn pg_memory_mb(&self) -> u32 {
        self.total_memory() / 2
    }
}

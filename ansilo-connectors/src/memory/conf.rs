use std::{collections::HashMap, sync::RwLock};

use ansilo_core::data::DataValue;
use serde::{Deserialize, Serialize};

/// The in-memory data store config, all data is stored in the data structure
/// below
#[derive(Debug, Serialize, Deserialize)]
pub struct MemoryConnectionConfig {
    /// The in-memory data queried by the connector
    /// This 2D tabular data keyed by the respective the string "{entity_id}-{version_id}"
    data: RwLock<HashMap<String, Vec<Vec<DataValue>>>>,
}

impl MemoryConnectionConfig {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_data(
        &self,
        entity_id: impl Into<String>,
        version_id: impl Into<String>,
        rows: Vec<Vec<DataValue>>,
    ) {
        let mut data = self.data.write().unwrap();
        data.insert(format!("{}-{}", entity_id.into(), version_id.into()), rows);
    }

    pub fn with_data_mut<F: FnOnce(&mut Vec<Vec<DataValue>>) -> R, R>(
        &self,
        entity_id: impl Into<String>,
        version_id: impl Into<String>,
        cb: F,
    ) -> Option<R> {
        let mut data = self.data.write().unwrap();
        let rows = data.get_mut(&format!("{}-{}", entity_id.into(), version_id.into()))?;

        Some(cb(rows))
    }

    pub fn with_data<F: FnOnce(&Vec<Vec<DataValue>>) -> R, R>(
        &self,
        entity_id: impl Into<String>,
        version_id: impl Into<String>,
        cb: F,
    ) -> Option<R> {
        let data = self.data.read().unwrap();
        let rows = data.get(&format!("{}-{}", entity_id.into(), version_id.into()))?;

        Some(cb(rows))
    }
}

impl Clone for MemoryConnectionConfig {
    fn clone(&self) -> Self {
        Self {
            data: RwLock::new(self.data.read().unwrap().clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_connector_connection_config() {
        let conf = MemoryConnectionConfig::new();

        conf.set_data("a", "1.0", vec![vec![DataValue::Null]]);

        let rows = conf.with_data("a", "1.0", |data| data.clone());

        assert_eq!(rows, Some(vec![vec![DataValue::Null]]));

        assert!(conf.with_data("a", "2.0", |_| ()).is_none());
    }
}

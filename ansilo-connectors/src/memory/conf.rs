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
    /// We also keep track of row id's to ensure they are uniquely assigned for each entity
    row_ids: RwLock<HashMap<String, u64>>,
}

impl MemoryConnectionConfig {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            row_ids: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_data(
        &self,
        entity_id: impl Into<String>,
        version_id: impl Into<String>,
        mut rows: Vec<Vec<DataValue>>,
    ) {
        let mut data = self.data.write().unwrap();
        let entity_id: String = entity_id.into();
        let version_id: String = version_id.into();
        self.append_row_ids(
            &entity_id,
            &version_id,
            rows.iter_mut().collect::<Vec<_>>().as_mut_slice(),
        );
        data.insert(format!("{}-{}", entity_id, version_id), rows);
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

    pub fn get_data(
        &self,
        entity_id: impl Into<String>,
        version_id: impl Into<String>,
    ) -> Option<Vec<Vec<DataValue>>> {
        self.with_data(entity_id, version_id, |rows| {
            let mut rows = rows.clone();
            self.remove_row_ids(&mut rows);
            rows
        })
    }

    pub(super) fn append_row_ids(
        &self,
        entity_id: impl Into<String>,
        version_id: impl Into<String>,
        rows: &mut [&mut Vec<DataValue>],
    ) {
        let mut row_ids = self.row_ids.write().unwrap();

        let id = {
            let key = format!("{}-{}", entity_id.into(), version_id.into());

            if !row_ids.contains_key(&key) {
                row_ids.insert(key.to_string(), 0);
            }

            row_ids.get_mut(&key).unwrap()
        };

        for row in rows.iter_mut() {
            row.push(DataValue::UInt64(*id));
            *id += 1;
        }
    }

    pub(super) fn remove_row_ids(&self, rows: &mut Vec<Vec<DataValue>>) {
        for row in rows.iter_mut() {
            row.remove(row.len() - 1);
        }
    }
}

impl Clone for MemoryConnectionConfig {
    fn clone(&self) -> Self {
        Self {
            data: RwLock::new(self.data.read().unwrap().clone()),
            row_ids: RwLock::new(self.row_ids.read().unwrap().clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_connector_connection_config() {
        let conf = MemoryConnectionConfig::new();

        conf.set_data(
            "a",
            "1.0",
            vec![vec![DataValue::Null], vec![DataValue::Null]],
        );

        // should append row ids
        let rows = conf.with_data("a", "1.0", |data| data.clone());

        assert_eq!(
            rows,
            Some(vec![
                vec![DataValue::Null, DataValue::UInt64(0)],
                vec![DataValue::Null, DataValue::UInt64(1)]
            ])
        );

        assert!(conf.with_data("a", "2.0", |_| ()).is_none());

        // should remove row ids
        let rows = conf.get_data("a", "1.0");

        assert_eq!(
            rows,
            Some(vec![vec![DataValue::Null], vec![DataValue::Null]])
        );
    }
}

use std::collections::HashMap;

use ansilo_core::{data::DataValue, sqlil::EntityVersionIdentifier};
use serde::{Deserialize, Serialize};

use crate::common::entity::EntitySource;

use super::MemoryConnectorEntitySourceConfig;

/// The connection config for the Oracle JDBC driver
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryConnectionConfig {
    /// The in-memory data queried by the connector
    /// This 2D tabular data keyed by the respective the string "{entity_id}-{version_id}"
    data: HashMap<String, Vec<Vec<DataValue>>>,
}

impl MemoryConnectionConfig {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn set_data(
        &mut self,
        entity_id: impl Into<String>,
        version_id: impl Into<String>,
        data: Vec<Vec<DataValue>>,
    ) {
        self.data
            .insert(format!("{}-{}", entity_id.into(), version_id.into()), data);
    }

    pub fn get_data(
        &self,
        entity_id: impl Into<String>,
        version_id: impl Into<String>,
    ) -> Option<&Vec<Vec<DataValue>>> {
        self.data
            .get(&format!("{}-{}", entity_id.into(), version_id.into()))
    }

    pub fn get_entity_data(&self, entity: &EntitySource<MemoryConnectorEntitySourceConfig>) -> Option<&Vec<Vec<DataValue>>> {
        self.get_data(&entity.conf.id, &entity.version_id)
    }

    pub fn get_entity_id_data(&self, entity: &EntityVersionIdentifier) -> Option<&Vec<Vec<DataValue>>> {
        self.get_data(&entity.entity_id, &entity.version_id)
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::sqlil;

    use super::*;

    #[test]
    fn test_memory_connector_connection_config() {
        let mut conf = MemoryConnectionConfig::new();

        conf.set_data("a", "1.0", vec![vec![DataValue::Null]]);
        
        assert_eq!(conf.get_data("a", "1.0"), Some(&vec![vec![DataValue::Null]]));
        assert_eq!(conf.get_entity_id_data(&sqlil::entity("a", "1.0")), Some(&vec![vec![DataValue::Null]]));
        assert_eq!(conf.get_data("a", "2.0"), None);
        assert_eq!(conf.get_data("b", "1.0"), None);
    }
}
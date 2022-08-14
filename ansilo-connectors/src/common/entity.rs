use std::collections::{hash_map::Values, HashMap};

use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
    sqlil::EntityId,
};

use crate::interface::Connector;

/// Configuration of all entities attached to a connector
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectorEntityConfig<TEntitySourceConfig>
where
    TEntitySourceConfig: Clone + Sized,
{
    /// The configuration of all the entities attached to this connector
    /// Keyed by the tuple of (entity id, version id)
    entities: HashMap<String, EntitySource<TEntitySourceConfig>>,
}

/// Metadata about an entity version
#[derive(Debug, Clone, PartialEq)]
pub struct EntitySource<TEntitySourceConfig>
where
    TEntitySourceConfig: Sized,
{
    /// The entity config
    pub conf: EntityConfig,
    /// The connector-specific source config
    pub source: TEntitySourceConfig,
}

impl<TEntitySourceConfig> EntitySource<TEntitySourceConfig>
where
    TEntitySourceConfig: Sized,
{
    pub fn new(conf: EntityConfig, source_conf: TEntitySourceConfig) -> Self {
        Self {
            conf,
            source: source_conf,
        }
    }
}

impl<T> ConnectorEntityConfig<T>
where
    T: Clone + Sized,
{
    pub fn new() -> Self {
        Self {
            entities: HashMap::new(),
        }
    }

    pub fn from<TConnector: Connector>(
        nc: &NodeConfig,
        data_source_id: &str,
    ) -> Result<ConnectorEntityConfig<TConnector::TEntitySourceConfig>> {
        let mut conf = ConnectorEntityConfig::<TConnector::TEntitySourceConfig>::new();

        for entity in nc
            .entities
            .iter()
            .filter(|e| &e.source.data_source_id == data_source_id)
        {
            let source = TConnector::parse_entity_source_options(entity.source.options.clone())?;

            conf.add(EntitySource::<TConnector::TEntitySourceConfig>::new(
                entity.clone(),
                source,
            ));
        }

        Ok(conf)
    }

    pub fn add(&mut self, entity: EntitySource<T>) {
        self.entities.insert(entity.conf.id.clone(), entity);
    }

    pub fn entities(&self) -> Values<String, EntitySource<T>> {
        self.entities.values()
    }

    pub fn find(&self, id: &EntityId) -> Option<&EntitySource<T>> {
        self.entities.get(&id.entity_id)
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::config::{EntityConfig, EntitySourceConfig};

    use super::*;

    #[test]
    fn test_connector_entity_config_new() {
        let conf = ConnectorEntityConfig::<()>::new();

        assert!(conf.entities.is_empty());
    }

    #[test]
    fn test_connector_entity_config_add_and_find() {
        let mut conf = ConnectorEntityConfig::<()>::new();
        let entity_source = EntitySource::new(
            EntityConfig::minimal("entity_id", vec![], EntitySourceConfig::minimal("")),
            (),
        );

        conf.add(entity_source.clone());

        assert_eq!(
            conf.entities.get(&("entity_id".to_string())),
            Some(&entity_source)
        );
        assert_eq!(conf.find(&EntityId::new("entity_id")), Some(&entity_source));
    }
}

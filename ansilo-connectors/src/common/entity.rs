use std::collections::{hash_map::Values, HashMap};

use ansilo_core::{
    config::{EntityConfig, EntityVersionConfig, NodeConfig},
    err::{bail, Result},
    sqlil::EntityVersionIdentifier,
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
    entities: HashMap<(String, String), EntitySource<TEntitySourceConfig>>,
}

/// Metadata about an entity version
#[derive(Debug, Clone, PartialEq)]
pub struct EntitySource<TEntitySourceConfig>
where
    TEntitySourceConfig: Sized,
{
    /// The entity config
    pub conf: EntityConfig,
    /// The ID of the entity version
    pub version_id: String,
    /// The connector-specific source config
    pub source_conf: TEntitySourceConfig,
}

impl<TEntitySourceConfig> EntitySource<TEntitySourceConfig>
where
    TEntitySourceConfig: Sized,
{
    pub fn new(
        conf: EntityConfig,
        version_id: String,
        source_conf: TEntitySourceConfig,
    ) -> Result<Self> {
        if !conf.versions.iter().any(|i| i.version == version_id) {
            bail!("No version {} found in entity {}", version_id, conf.id)
        }

        Ok(Self {
            conf,
            version_id,
            source_conf,
        })
    }

    pub fn minimal(
        entity_id: impl Into<String>,
        version: EntityVersionConfig,
        source_conf: TEntitySourceConfig,
    ) -> Self {
        Self {
            conf: EntityConfig::minimal(entity_id, vec![version.clone()]),
            version_id: version.version,
            source_conf,
        }
    }

    pub fn version(&self) -> &EntityVersionConfig {
        self.conf
            .versions
            .iter()
            .find(|i| i.version == self.version_id)
            .unwrap()
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

        for entity in nc.entities.iter() {
            for version in entity
                .versions
                .iter()
                .filter(|i| i.source.data_source_id == data_source_id)
            {
                let source =
                    TConnector::parse_entity_source_options(version.source.options.clone())?;

                conf.add(EntitySource::<TConnector::TEntitySourceConfig>::new(
                    entity.clone(),
                    version.version.clone(),
                    source,
                )?);
            }
        }

        Ok(conf)
    }

    pub fn add(&mut self, entity: EntitySource<T>) {
        self.entities
            .insert((entity.conf.id.clone(), entity.version_id.clone()), entity);
    }

    pub fn entities(&self) -> Values<(String, String), EntitySource<T>> {
        self.entities.values()
    }

    pub fn find(&self, id: &EntityVersionIdentifier) -> Option<&EntitySource<T>> {
        self.entities
            .get(&(id.entity_id.clone(), id.version_id.clone()))
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::config::{EntitySourceConfig, EntityVersionConfig};

    use super::*;

    #[test]
    fn test_connector_entity_config_new() {
        let conf = ConnectorEntityConfig::<()>::new();

        assert!(conf.entities.is_empty());
    }

    #[test]
    fn test_connector_entity_config_add_and_find() {
        let mut conf = ConnectorEntityConfig::<()>::new();
        let entity_source = EntitySource::minimal(
            "entity_id",
            EntityVersionConfig::minimal("version_id", vec![], EntitySourceConfig::minimal("")),
            (),
        );

        conf.add(entity_source.clone());

        assert_eq!(
            conf.entities
                .get(&("entity_id".to_string(), "version_id".to_string())),
            Some(&entity_source)
        );
        assert_eq!(
            conf.find(&EntityVersionIdentifier::new("entity_id", "version_id")),
            Some(&entity_source)
        );
    }
}

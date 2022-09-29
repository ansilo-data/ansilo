use std::marker::PhantomData;

use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::{bail, Result},
};

use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};

use crate::{FileConnection, FileIO, FileSourceConfig};

pub struct FileEntityValidator<F: FileIO> {
    _io: PhantomData<F>,
}

impl<F: FileIO> EntityValidator for FileEntityValidator<F> {
    type TConnection = FileConnection<F>;
    type TEntitySourceConfig = FileSourceConfig;

    fn validate(
        _con: &mut FileConnection<F>,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<FileSourceConfig>> {
        if entity.id.starts_with(".") || entity.id.contains("/") {
            bail!("Invalid entity id");
        }

        Ok(EntitySource::new(
            entity.clone(),
            FileSourceConfig::new(entity.id.clone()),
        ))
    }
}

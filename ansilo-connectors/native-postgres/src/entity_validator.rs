use std::{marker::PhantomData, ops::DerefMut};

use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};
use tokio_postgres::Client;

use crate::PostgresConnection;

use super::PostgresEntitySourceConfig;
use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};

/// The entity validator for Postgres
pub struct PostgresEntityValidator<T> {
    _data: PhantomData<T>,
}

impl<T: DerefMut<Target = Client>> EntityValidator for PostgresEntityValidator<T> {
    type TConnection = PostgresConnection<T>;
    type TEntitySourceConfig = PostgresEntitySourceConfig;

    fn validate(
        _connection: &mut Self::TConnection,
        entity: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<PostgresEntitySourceConfig>> {
        Ok(EntitySource::new(
            entity.clone(),
            PostgresEntitySourceConfig::parse(entity.source.options.clone())?,
        ))
    }
}

use ansilo_core::{
    config::{EntityVersionConfig, NodeConfig},
    err::Result,
};

use crate::{common::entity::EntitySource, interface::EntityValidator, jdbc::JdbcConnection};

use super::OracleJdbcEntitySourceConfig;

/// The entity validator for Oracle JDBC
pub struct OracleJdbcEntityValidator {}

impl<'a> EntityValidator<JdbcConnection<'a>, OracleJdbcEntitySourceConfig>
    for OracleJdbcEntityValidator
{
    fn validate(
        &self,
        connection: &JdbcConnection<'a>,
        entity_version: &EntityVersionConfig,
        nc: &NodeConfig,
    ) -> Result<EntitySource<OracleJdbcEntitySourceConfig>> {
        todo!()
    }
}

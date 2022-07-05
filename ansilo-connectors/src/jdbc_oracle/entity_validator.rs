use ansilo_core::{
    config::{EntityVersionConfig, NodeConfig},
    err::Result,
};

use crate::{common::entity::EntitySource, interface::EntityValidator, jdbc::JdbcConnection};

use super::OracleJdbcEntitySourceConfig;

/// The entity validator for Oracle JDBC
pub struct OracleJdbcEntityValidator {}

impl EntityValidator<JdbcConnection, OracleJdbcEntitySourceConfig>
    for OracleJdbcEntityValidator
{
    fn validate(
        connection: &JdbcConnection,
        entity_version: &EntityVersionConfig,
        nc: &NodeConfig,
    ) -> Result<EntitySource<OracleJdbcEntitySourceConfig>> {
        todo!()
    }
}

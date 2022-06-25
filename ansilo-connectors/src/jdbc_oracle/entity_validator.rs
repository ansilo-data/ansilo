use ansilo_core::{config::EntityVersionConfig, err::Result};

use crate::{
    interface::{EntityValidator, EntityVersionMetadata},
    jdbc::JdbcConnection,
};

use super::OracleJdbcEntitySourceConfig;

/// The entity validator for Oracle JDBC
pub struct OracleJdbcEntityValidator {}

impl<'a> EntityValidator<JdbcConnection<'a>, OracleJdbcEntitySourceConfig>
    for OracleJdbcEntityValidator
{
    fn validate(
        &self,
        connection: &JdbcConnection<'a>,
        entity_version: EntityVersionConfig,
    ) -> Result<EntityVersionMetadata<OracleJdbcEntitySourceConfig>> {
        todo!()
    }
}

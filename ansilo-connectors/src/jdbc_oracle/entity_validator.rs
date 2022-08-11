use ansilo_core::{
    config::{EntityVersionConfig, NodeConfig},
    err::Result,
};

use crate::{common::entity::EntitySource, interface::EntityValidator, jdbc::{JdbcConnection, JdbcDefaultTypeMapping}};

use super::OracleJdbcEntitySourceConfig;

/// The entity validator for Oracle JDBC
pub struct OracleJdbcEntityValidator {}

impl EntityValidator for OracleJdbcEntityValidator {
    type TConnection = JdbcConnection<JdbcDefaultTypeMapping>;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

    fn validate(
        connection: &mut Self::TConnection,
        entity_version: &EntityVersionConfig,
        nc: &NodeConfig,
    ) -> Result<EntitySource<OracleJdbcEntitySourceConfig>> {
        todo!()
    }
}

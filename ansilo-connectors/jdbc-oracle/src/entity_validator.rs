use ansilo_core::{
    config::{EntityConfig, NodeConfig},
    err::Result,
};

use super::OracleJdbcEntitySourceConfig;
use ansilo_connectors_base::{common::entity::EntitySource, interface::EntityValidator};
use ansilo_connectors_jdbc_base::{JdbcConnection, JdbcDefaultTypeMapping};

/// The entity validator for Oracle JDBC
pub struct OracleJdbcEntityValidator {}

impl EntityValidator for OracleJdbcEntityValidator {
    type TConnection = JdbcConnection<JdbcDefaultTypeMapping>;
    type TEntitySourceConfig = OracleJdbcEntitySourceConfig;

    fn validate(
        _connection: &mut Self::TConnection,
        _entity_version: &EntityConfig,
        _nc: &NodeConfig,
    ) -> Result<EntitySource<OracleJdbcEntitySourceConfig>> {
        todo!()
    }
}
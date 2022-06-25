use ansilo_core::err::Result;

use crate::{
    interface::{EntitySearcher, EntityVersionMetadata},
    jdbc::JdbcConnection,
};

use super::OracleJdbcEntitySourceConfig;

/// The entity searcher for Oracle JDBC
pub struct OracleJdbcEntitySearcher {}

impl<'a> EntitySearcher<JdbcConnection<'a>, OracleJdbcEntitySourceConfig>
    for OracleJdbcEntitySearcher
{
    fn discover(
        &self,
        connection: &JdbcConnection<'a>,
    ) -> Result<Vec<EntityVersionMetadata<OracleJdbcEntitySourceConfig>>> {
        todo!()
    }
}

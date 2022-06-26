use ansilo_core::{err::Result, sqlil::select::Select, common::data::DataType};

use crate::{jdbc::{JdbcConnection, JdbcQuery}, interface::QueryCompiler};

/// Query compiler for Oracle JDBC driver
pub struct OracleJdbcQueryCompiler;

impl<'a> QueryCompiler<JdbcConnection<'a>, JdbcQuery> for OracleJdbcQueryCompiler {
    fn compile_select(&self, con: &JdbcConnection<'a>, select: &Select) -> Result<JdbcQuery> {
        /// TODO: implement
        let params = Vec::<DataType>::new();
        todo!()
    }
}

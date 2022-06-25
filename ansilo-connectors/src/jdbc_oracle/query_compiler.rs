use ansilo_core::{err::Result, sqlil::select::Select};

use crate::jdbc::{JdbcConnection, JdbcQuery};

/// Query compiler for Oracle JDBC driver
pub struct OracleJdbcQueryCompiler;

impl OracleJdbcQueryCompiler {
    pub fn compile_select<'a>(con: &JdbcConnection<'a>, select: &Select) -> Result<JdbcQuery> {
        todo!()
    }
}

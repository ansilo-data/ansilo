use ansilo_core::err::Result;

use crate::interface::{ResultSet, RowStructure};

/// Implementation of the JDBC result set
pub struct JdbcResultSet {

}

impl ResultSet for JdbcResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        todo!()
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<u32> {
        todo!()
    }
}
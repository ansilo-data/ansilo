use ansilo_core::{
    common::data::DataType,
    err::{Context, Result},
};
use jni::objects::{GlobalRef, JList, JString};

use crate::interface::{ResultSet, RowStructure};

use super::{Jvm, JdbcDataType};

/// Implementation of the JDBC result set
pub struct JdbcResultSet<'a> {
    pub jvm: &'a Jvm<'a>,
    pub jdbc_result_set: GlobalRef,
}

impl<'a> JdbcResultSet<'a> {
    pub fn new(jvm: &'a Jvm<'a>, jdbc_result_set: GlobalRef) -> Self {
        Self {
            jvm,
            jdbc_result_set,
        }
    }
}

impl<'a> ResultSet<'a> for JdbcResultSet<'a> {
    fn get_structure(&self) -> Result<RowStructure> {
        let env = &self.jvm.env;

        let jdbc_structure = env
            .call_method(
                self.jdbc_result_set.as_obj(),
                "getRowStructure",
                "()Lcom/ansilo/connectors/result/JdbcRowStructure;",
                &[],
            )
            .context("Failed to call JdbcResultSet::getRowStructure")?
            .l()
            .context("Failed to convert JdbcRowStructure into object")?;

        let jdbc_cols = env
            .call_method(jdbc_structure, "getCols", "()Ljava/util/List;", &[])
            .context("Failed to call JdbcRowStructure::getCols")?
            .l()
            .context("Failed to convert List into object")?;
        let jdbc_cols = JList::from_env(env, jdbc_cols).context("Failed to read list")?;

        let mut structure = RowStructure::new(vec![]);

        for col in jdbc_cols.iter().context("Failed to iterate list")? {
            let name = env
                .call_method(col, "getName", "()Ljava/lang/String;", &[])
                .context("Failed to call JdbcRowColumnInfo::getName")?
                .l()
                .context("Failed to convert to object")?;
            let name = env
                .get_string(JString::from(name))
                .context("Failed to convert java string")
                .and_then(|i| {
                    i.to_str()
                        .map(|i| i.to_string())
                        .context("Failed to convert java string")
                })?;

            let data_type_id = env
                .call_method(col, "getDataTypeId", "()LI;", &[])
                .context("Failed to call JdbcRowColumnInfo::getDataTypeId")?
                .i()
                .context("Failed to convert to int")?;

            structure
                .cols
                .push((name, JdbcDataType::try_from(data_type_id)?.0));
        }

        Ok(structure)
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<u32> {
        todo!()
    }
}

use ansilo_core::{
    common::data::DataType,
    err::{Context, Result},
};
use jni::objects::{GlobalRef, JList, JString};

use crate::interface::{ResultSet, RowStructure};

use super::{JdbcDataType, Jvm};

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

#[cfg(test)]
mod tests {
    use ansilo_core::common::data::{EncodingType, VarcharOptions};
    use jni::objects::JValue;

    use super::*;

    #[test]
    fn test_get_row_structure() {
        let jvm = Jvm::boot().unwrap();
        let env = &jvm.env;

        // ensure sqlite is loaded
        let class = env.find_class("org/sqlite/JDBC").unwrap();
        println!("class: {:?}", class);

        // create sqlite in-memory jdbc instance
        // let drivers = env
        //     .call_static_method(
        //         "java/sql/DriverManager",
        //         "getDrivers",
        //         "()Ljava/util/Enumeration;",
        //         &[],
        //     )
        //     .unwrap()
        //     .l()
        //     .unwrap();
            
        // let drivers = JList::from_env(env, drivers).context("Failed to read list").unwrap();

        // for driver in drivers.iter().unwrap() {
        //     let name = env.call_method(driver, "toString", "()Ljava/lang/String;", &[]).unwrap().l().unwrap();
        //     let name = env.get_string(JString::from(name)).unwrap();
        //     println!("driver: {:?}", name.to_string_lossy());
        // }

        // create sqlite in-memory jdbc instance
        let jdbc_con = env
            .call_static_method(
                "java/sql/DriverManager",
                "getConnection",
                "(Ljava/lang/String;)Ljava/sql/Connection;",
                &[JValue::Object(
                    *env.new_string("jdbc:sqlite::memory:").unwrap(),
                )],
            )
            .unwrap()
            .l()
            .unwrap();

        // create statement
        let jdbc_statement = env
            .call_method(jdbc_con, "createStatement", "()Ljava/sql/Statement;", &[])
            .unwrap()
            .l()
            .unwrap();

        // execute query
        let jdbc_result_set = env
            .call_method(
                jdbc_statement,
                "executeQuery",
                "()Ljava/sql/ResultSet;",
                &[JValue::Object(
                    *env.new_string("SELECT 1 as num, \"foo\" as str").unwrap(),
                )],
            )
            .unwrap()
            .l()
            .unwrap();
        let jdbc_result_set = env.new_global_ref(jdbc_result_set).unwrap();

        let rust_wrapper = JdbcResultSet::new(&jvm, jdbc_result_set);

        let row_structure = rust_wrapper.get_structure().unwrap();

        assert_eq!(
            row_structure,
            RowStructure::new(vec![
                ("num".to_string(), DataType::Int32),
                (
                    "str".to_string(),
                    DataType::Varchar(VarcharOptions::new(None, EncodingType::Utf8))
                ),
            ])
        );
    }
}

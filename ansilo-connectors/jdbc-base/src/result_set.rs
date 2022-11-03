use std::sync::Arc;

use ansilo_core::err::{Context, Result};
use jni::{
    objects::{GlobalRef, JList, JMethodID, JString, JValue},
    signature::{Primitive, ReturnType},
};

use ansilo_connectors_base::interface::{ResultSet, RowStructure};

use super::{JavaDataType, Jvm};

/// Implementation of the JDBC result set
pub struct JdbcResultSet {
    pub jvm: Arc<Jvm>,
    pub jdbc_result_set: GlobalRef,
    pub read_method_id: Option<JMethodID>,
}

impl JdbcResultSet {
    pub fn new(jvm: Arc<Jvm>, jdbc_result_set: GlobalRef) -> Self {
        Self {
            jvm,
            jdbc_result_set,
            read_method_id: None,
        }
    }
}

impl ResultSet for JdbcResultSet {
    fn get_structure(&self) -> Result<RowStructure> {
        self.jvm.with_local_frame(32, |env| {
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

            self.jvm.check_exceptions(env)?;

            let jdbc_cols = env
                .call_method(jdbc_structure, "getCols", "()Ljava/util/List;", &[])
                .context("Failed to call JdbcRowStructure::getCols")?
                .l()
                .context("Failed to convert List into object")?;

            self.jvm.check_exceptions(env)?;

            let jdbc_cols = JList::from_env(env, jdbc_cols).context("Failed to read list")?;

            let mut structure = RowStructure::new(vec![]);

            for col in jdbc_cols.iter().context("Failed to iterate list")? {
                let name = env.auto_local(
                    env.call_method(col, "getName", "()Ljava/lang/String;", &[])
                        .context("Failed to call JdbcRowColumnInfo::getName")?
                        .l()
                        .context("Failed to convert to object")?,
                );
                self.jvm.check_exceptions(env)?;

                let name = env
                    .get_string(JString::from(name.as_obj()))
                    .context("Failed to convert column name to java string")
                    .and_then(|i| {
                        cesu8::from_java_cesu8(i.to_bytes())
                            .map(|i| i.to_string())
                            .context(
                                "Failed to convert column name to java string during utf8 parsing",
                            )
                    })?;

                let jdbc_type_id = env
                    .call_method(col, "getDataTypeId", "()I", &[])
                    .context("Failed to call JdbcRowColumnInfo::getDataTypeId")?
                    .i()
                    .context("Failed to convert to int")?;
                self.jvm.check_exceptions(env)?;

                let java_data_type = JavaDataType::try_from(jdbc_type_id)?;
                structure.cols.push((name, java_data_type.into()));
            }

            Ok(structure)
        })
    }

    fn read(&mut self, buff: &mut [u8]) -> Result<usize> {
        self.jvm.with_local_frame(32, |env| {
            if self.read_method_id.is_none() {
                self.read_method_id = Some(
                    env.get_method_id(
                        "com/ansilo/connectors/result/JdbcResultSet",
                        "read",
                        "(Ljava/nio/ByteBuffer;)I",
                    )
                    .context("Failed to get method id of JdbcResultSet::read")?,
                );
            }

            let jvm_buff = unsafe {
                *env.new_direct_byte_buffer(buff.as_mut_ptr(), buff.len())
                    .context("Failed to create java ByteBuffer")?
            };

            let result = env
                .call_method_unchecked(
                    self.jdbc_result_set.as_obj(),
                    JMethodID::from(self.read_method_id.unwrap()),
                    ReturnType::Primitive(Primitive::Int),
                    &[JValue::Object(jvm_buff).into()],
                )
                .context("Failed to call JdbcResultSet::read")?
                .i()
                .context("Failed to parse return value of JdbcResultSet::read")?;

            self.jvm.check_exceptions(env)?;

            result
                .try_into()
                .context("Return value of JdbcResuletSet::read cannot be < 0")
        })
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::data::{DataType, StringOptions};
    use jni::objects::{JObject, JValue};

    use crate::tests::create_sqlite_memory_connection;

    use super::*;

    fn execute_query(jvm: &Arc<Jvm>, jdbc_con: JObject, query: &str) -> JdbcResultSet {
        let env = &jvm.env().unwrap();

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
                "(Ljava/lang/String;)Ljava/sql/ResultSet;",
                &[JValue::Object(*env.new_string(query).unwrap())],
            )
            .unwrap()
            .l()
            .unwrap();

        let data_map = env
            .new_object(
                "com/ansilo/connectors/mapping/SqliteJdbcDataMapping",
                "()V",
                &[],
            )
            .unwrap();

        let jdbc_result_set = env
            .new_object(
                "com/ansilo/connectors/result/JdbcResultSet",
                "(Lcom/ansilo/connectors/mapping/JdbcDataMapping;Ljava/sql/ResultSet;)V",
                &[JValue::Object(data_map), JValue::Object(jdbc_result_set)],
            )
            .unwrap();

        let jdbc_result_set = env.new_global_ref(jdbc_result_set).unwrap();

        JdbcResultSet::new(Arc::clone(jvm), jdbc_result_set)
    }

    #[test]
    fn test_get_row_structure() {
        let jvm = Arc::new(Jvm::boot(None).unwrap());

        let jdbc_con = create_sqlite_memory_connection(&jvm);
        let result_set = execute_query(&jvm, jdbc_con, "SELECT 1 as num, \"abc\" as str");

        let row_structure = result_set.get_structure().unwrap();

        assert_eq!(
            row_structure,
            RowStructure::new(vec![
                ("num".to_string(), DataType::Int32),
                (
                    "str".to_string(),
                    DataType::Utf8String(StringOptions::default())
                ),
            ])
        );
    }

    #[test]
    fn test_result_set_read_int() {
        let jvm = Arc::new(Jvm::boot(None).unwrap());

        let jdbc_con = create_sqlite_memory_connection(&jvm);
        let mut result_set = execute_query(&jvm, jdbc_con, "SELECT 1 as num");

        let mut buff = [0; 1024];
        let read = result_set.read(&mut buff[..]).unwrap();

        assert_eq!(
            buff[..read],
            [
                vec![1u8], // (not null)
                1i32.to_be_bytes().to_vec()
            ]
            .concat()
        );
    }

    #[test]
    fn test_result_set_read_string() {
        let jvm = Arc::new(Jvm::boot(None).unwrap());

        let jdbc_con = create_sqlite_memory_connection(&jvm);
        let mut result_set = execute_query(&jvm, jdbc_con, "SELECT \"abc\" as str");

        let mut buff = [0; 1024];
        let read = result_set.read(&mut buff[..]).unwrap();

        assert_eq!(
            buff[..read],
            [
                vec![1u8],                 // (not null)
                vec![3u8],                 // (read length)
                "abc".as_bytes().to_vec(), // (data)
                vec![0u8],                 // (EOF)
            ]
            .concat()
        );
    }
}

use ansilo_core::err::{Context, Result};
use jni::{
    objects::{GlobalRef, JList, JMethodID, JString, JValue},
    signature::{JavaType, Primitive},
};

use crate::interface::{ResultSet, RowStructure};

use super::{JdbcDataType, Jvm};

/// Implementation of the JDBC result set
pub struct JdbcResultSet<'a> {
    pub jvm: &'a Jvm<'a>,
    pub jdbc_result_set: GlobalRef,
    pub read_method_id: Option<JMethodID<'a>>,
}

impl<'a> JdbcResultSet<'a> {
    pub fn new(jvm: &'a Jvm<'a>, jdbc_result_set: GlobalRef) -> Self {
        Self {
            jvm,
            jdbc_result_set,
            read_method_id: None,
        }
    }
}

impl<'a> ResultSet<'a> for JdbcResultSet<'a> {
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

            let jdbc_cols = env
                .call_method(jdbc_structure, "getCols", "()Ljava/util/List;", &[])
                .context("Failed to call JdbcRowStructure::getCols")?
                .l()
                .context("Failed to convert List into object")?;
            let jdbc_cols = JList::from_env(env, jdbc_cols).context("Failed to read list")?;

            let mut structure = RowStructure::new(vec![]);

            for col in jdbc_cols.iter().context("Failed to iterate list")? {
                let name = env.auto_local(
                    env.call_method(col, "getName", "()Ljava/lang/String;", &[])
                        .context("Failed to call JdbcRowColumnInfo::getName")?
                        .l()
                        .context("Failed to convert to object")?,
                );
                let name = env
                    .get_string(JString::from(name.as_obj()))
                    .context("Failed to convert java string")
                    .and_then(|i| {
                        i.to_str()
                            .map(|i| i.to_string())
                            .context("Failed to convert java string")
                    })?;

                let data_type_id = env
                    .call_method(col, "getDataTypeId", "()I", &[])
                    .context("Failed to call JdbcRowColumnInfo::getDataTypeId")?
                    .i()
                    .context("Failed to convert to int")?;

                structure
                    .cols
                    .push((name, JdbcDataType::try_from(data_type_id)?.0));
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

            let jvm_buff = *env
                .new_direct_byte_buffer(buff)
                .context("Failed to create java ByteBuffer")?;

            let result = env
                .call_method_unchecked(
                    self.jdbc_result_set.as_obj(),
                    self.read_method_id.unwrap(),
                    JavaType::Primitive(Primitive::Int),
                    &[JValue::Object(jvm_buff)],
                )
                .context("Failed to call JdbcResultSet::read")?
                .i()
                .context("Failed to parse return value of JdbcResultSet::read")?;

            // TODO: exception handling

            result
                .try_into()
                .context("Return value of JdbcResuletSet::read cannot be < 0")
        })
    }
}

#[cfg(test)]
mod tests {
    use ansilo_core::common::data::{DataType, EncodingType, VarcharOptions};
    use jni::objects::{JObject, JValue};

    use crate::jdbc::test::create_sqlite_memory_connection;

    use super::*;

    fn execute_query<'a>(
        jvm: &'a Jvm<'a>,
        jdbc_con: JObject<'a>,
        query: &str,
    ) -> JdbcResultSet<'a> {
        let env = &jvm.env;

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

        let jdbc_result_set = env
            .new_object(
                "com/ansilo/connectors/result/JdbcResultSet",
                "(Ljava/sql/ResultSet;)V",
                &[JValue::Object(jdbc_result_set)],
            )
            .unwrap();

        let jdbc_result_set = env.new_global_ref(jdbc_result_set).unwrap();

        JdbcResultSet::new(&jvm, jdbc_result_set)
    }

    #[test]
    fn test_get_row_structure() {
        let jvm = Jvm::boot().unwrap();

        let jdbc_con = create_sqlite_memory_connection(&jvm);
        let result_set = execute_query(&jvm, jdbc_con, "SELECT 1 as num, \"abc\" as str");

        let row_structure = result_set.get_structure().unwrap();

        assert_eq!(
            row_structure,
            RowStructure::new(vec![
                ("num".to_string(), DataType::Int32),
                (
                    "str".to_string(),
                    DataType::Varchar(VarcharOptions::new(None, EncodingType::Ascii))
                ),
            ])
        );
    }

    #[test]
    fn test_result_set_read_int() {
        let jvm = Jvm::boot().unwrap();

        let jdbc_con = create_sqlite_memory_connection(&jvm);
        let mut result_set = execute_query(&jvm, jdbc_con, "SELECT 1 as num");

        let mut buff = [0; 1024];
        let read = result_set.read(&mut buff[..]).unwrap();

        assert_eq!(
            buff[..read],
            [
                vec![1u8], // (not null)
                1i32.to_ne_bytes().to_vec()
            ]
            .concat()
        );
    }

    #[test]
    fn test_result_set_read_string() {
        let jvm = Jvm::boot().unwrap();

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

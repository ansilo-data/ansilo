use std::sync::Arc;

use ansilo_core::err::{bail, Context, Result};
use jni::{
    objects::{GlobalRef, JMethodID, JObject, JString, JValue},
    signature::{JavaType, Primitive},
    sys::jmethodID,
};
use serde::Serialize;

use ansilo_connectors_base::{
    common::{data::DataWriter, query::QueryParam},
    interface::{LoggedQuery, QueryHandle, QueryInputStructure},
};

use crate::JavaDataType;

use super::{JdbcResultSet, Jvm};

/// JDBC query
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JdbcQuery {
    /// The query (likely SQL) as a string
    pub query: String,
    /// Types of query parameters expected by the query
    pub params: Vec<QueryParam>,
}

impl JdbcQuery {
    pub fn new(query: impl Into<String>, params: Vec<QueryParam>) -> Self {
        Self {
            query: query.into(),
            params,
        }
    }
}

/// JDBC prepared query
pub struct JdbcPreparedQuery {
    pub jvm: Arc<Jvm>,
    pub jdbc_prepared_statement: GlobalRef,
    pub query: JdbcQuery,
    write_method_id: Option<jmethodID>,
    as_read_only_buffer_method_id: Option<jmethodID>,
}

impl JdbcPreparedQuery {
    pub fn new(jvm: Arc<Jvm>, jdbc_prepared_statement: GlobalRef, query: JdbcQuery) -> Self {
        Self {
            jvm,
            jdbc_prepared_statement,
            query,
            write_method_id: None,
            as_read_only_buffer_method_id: None,
        }
    }

    fn init_method_ids(&mut self) -> Result<()> {
        let env = self.jvm.env()?;

        if self.write_method_id.is_none() {
            self.write_method_id = Some(
                env.get_method_id(
                    "com/ansilo/connectors/query/JdbcPreparedQuery",
                    "write",
                    "(Ljava/nio/ByteBuffer;)I",
                )
                .context("Failed to find JdbcPreparedQuery::write method")?
                .into_inner(),
            );
        }

        if self.as_read_only_buffer_method_id.is_none() {
            self.as_read_only_buffer_method_id = Some(
                env.get_method_id(
                    "java/nio/ByteBuffer",
                    "asReadOnlyBuffer",
                    "()Ljava/nio/ByteBuffer;",
                )
                .context("Failed to find ByteBuffer::asReadOnlyBuffer method")?
                .into_inner(),
            );
        }

        Ok(())
    }
}

impl QueryHandle for JdbcPreparedQuery {
    type TResultSet = JdbcResultSet;

    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(QueryInputStructure::from(&self.query.params))
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        self.init_method_ids()?;
        self.jvm.with_local_frame(32, |env| {
            // Our supplied buff is a immutable reference however our jni interface
            // requires an &mut [u8]. We use some unsafe code to override this restriction
            // and manually enforce it using https://docs.oracle.com/javase/10/docs/api/java/nio/ByteBuffer.html#asReadOnlyBuffer()
            let byte_buff = unsafe {
                // TODO: Hopefully get https://github.com/jni-rs/jni-rs/pull/351 merged
                let byte_buff = env
                    .new_direct_byte_buffer_raw(buff.as_ptr() as *mut _, buff.len())
                    .context("Failed to create direct byte buffer")?;

                let byte_buff = env.auto_local(
                    env.call_method_unchecked(
                        *byte_buff,
                        JMethodID::from(self.as_read_only_buffer_method_id.unwrap()),
                        JavaType::Object("java/nio/ByteBuffer".to_owned()),
                        &[],
                    )
                    .context("Failed to call ByteBuffer::asReadOnlyBuffer")?
                    .l()
                    .context("Failed to convert ByteBuffer to object")?,
                );

                self.jvm.check_exceptions(env)?;

                byte_buff
            };

            let written = env
                .call_method_unchecked(
                    self.jdbc_prepared_statement.as_obj(),
                    JMethodID::from(self.write_method_id.unwrap()),
                    JavaType::Primitive(Primitive::Int),
                    &[JValue::Object(byte_buff.as_obj())],
                )
                .context("Failed to invoke JdbcPreparedQuery::write")?
                .i()
                .context("Failed to convert JdbcPreparedQuery::write return value into int")?;

            self.jvm.check_exceptions(env)?;

            if written < 0 {
                bail!("JdbcPreparedQuery::write returned value less than 0");
            }

            Ok(written.try_into().unwrap())
        })
    }

    fn restart(&mut self) -> Result<()> {
        self.jvm.with_local_frame(32, |env| {
            let _ = env
                .call_method(self.jdbc_prepared_statement.as_obj(), "restart", "()V", &[])
                .context("Failed to invoke JdbcPreparedQuery::restart")?;

            self.jvm.check_exceptions(env)?;

            Ok(())
        })
    }

    fn execute_query(&mut self) -> Result<JdbcResultSet> {
        self.jvm.with_local_frame(32, |env| {
            let jdbc_result_set = env
                .call_method(
                    self.jdbc_prepared_statement.as_obj(),
                    "executeQuery",
                    "()Lcom/ansilo/connectors/result/JdbcResultSet;",
                    &[],
                )
                .context("Failed to invoke JdbcPreparedQuery::executeQuery")?
                .l()
                .context("Failed to convert JdbcResultSet into object")?;

            self.jvm.check_exceptions(env)?;

            let jdbc_result_set = env.new_global_ref(jdbc_result_set)?;

            Ok(JdbcResultSet::new(Arc::clone(&self.jvm), jdbc_result_set))
        })
    }

    fn execute_modify(&mut self) -> Result<Option<u64>> {
        self.jvm.with_local_frame(32, |env| {
            let long = env
                .call_method(
                    self.jdbc_prepared_statement.as_obj(),
                    "executeModify",
                    "()Ljava/lang/Long;",
                    &[],
                )
                .context("Failed to invoke JdbcPreparedQuery::executeModify")?
                .l()
                .context("Failed to convert Long into object")?;

            self.jvm.check_exceptions(env)?;

            // Null means the number is unknown
            if env
                .is_same_object(long, JObject::null())
                .context("Failed to null-check")?
            {
                return Ok(None);
            }

            let long = env
                .call_method(long, "longValue", "()J", &[])
                .context("Failed to invoke Long::longValue")?
                .j()
                .context("Failed to convert long into i64")?;

            Ok(Some(long as _))
        })
    }

    fn logged(&self) -> Result<LoggedQuery> {
        let params = self.jvm.with_local_frame(32, |env| {
            let logged_params = env.auto_local(
                env.call_method(
                    self.jdbc_prepared_statement.as_obj(),
                    "getLoggedParamsAsJson",
                    "()Ljava/lang/String;",
                    &[],
                )
                .context("Failed to invoke JdbcPreparedQuery::getLoggedParamsAsJson")?
                .l()
                .context("Failed to convert List into object")?,
            );

            self.jvm.check_exceptions(env)?;

            let json = env
                .get_string(JString::from(logged_params.as_obj()))
                .context("Failed to convert LoggedParam to java string")
                .map(|i| {
                    cesu8::from_java_cesu8(i.to_bytes())
                        .unwrap_or_else(|_| String::from_utf8_lossy(i.to_bytes()))
                        .to_string()
                })?;

            let params: Vec<String> =
                serde_json::from_str(&json).context("Failed to parse logged params json")?;

            Ok(params)
        })?;

        Ok(LoggedQuery::new(&self.query.query, params, None))
    }
}

/// Initialises a new instance of the JdbcParameter class which
/// copies the current query parameter
/// @see ansilo-connectors/src/jdbc/java/src/main/java/com/ansilo/connectors/query/JdbcParameter.java
pub(crate) fn to_java_jdbc_parameter<'a>(
    param: &QueryParam,
    index: usize,
    jvm: &'a Jvm,
) -> Result<JObject<'a>> {
    let env = jvm.env()?;

    let result = match param {
        QueryParam::Dynamic(p) => env.call_static_method(
            "com/ansilo/connectors/query/JdbcParameter",
            "createDynamic",
            "(II)Lcom/ansilo/connectors/query/JdbcParameter;",
            &[
                JValue::Int(index as i32),
                JValue::Int(JavaDataType::from(&p.r#type) as i32),
            ],
        ),
        QueryParam::Constant(data_value) => {
            let mut buff = DataWriter::to_vec_one(data_value.clone())?;

            let byte_buff = env
                .new_direct_byte_buffer(buff.as_mut_slice())
                .context("Failed to init ByteBuffer")?;

            env.call_static_method(
                "com/ansilo/connectors/query/JdbcParameter",
                "createConstantCopied",
                "(IILjava/nio/ByteBuffer;)Lcom/ansilo/connectors/query/JdbcParameter;",
                &[
                    JValue::Int(index as i32),
                    JValue::Int(JavaDataType::from(&data_value.r#type()) as i32),
                    JValue::Object(*byte_buff),
                ],
            )
        }
    };

    jvm.check_exceptions(&env)?;

    Ok(result
        .context("Failed to create JdbcParameter instance")?
        .l()
        .context("Failed to convert return of JdbcParameter factory to object")?)
}

#[cfg(test)]
mod tests {
    use ansilo_core::data::{DataType, DataValue, StringOptions};
    use jni::objects::{JObject, JString};

    use crate::tests::create_sqlite_memory_connection;
    use ansilo_connectors_base::common::data::ResultSetReader;

    use super::*;

    fn create_prepared_query(
        jvm: &Arc<Jvm>,
        jdbc_con: JObject,
        query: &str,
        params: Vec<QueryParam>,
    ) -> JdbcPreparedQuery {
        let query = JdbcQuery::new(query, params);
        let env = &jvm.env().unwrap();

        let prepared_statement = env
            .call_method(
                jdbc_con,
                "prepareStatement",
                "(Ljava/lang/String;)Ljava/sql/PreparedStatement;",
                &[JValue::Object(*env.new_string(&query.query).unwrap())],
            )
            .unwrap();

        let param_types = env.new_object("java/util/ArrayList", "()V", &[]).unwrap();

        for (idx, param) in query.params.iter().enumerate() {
            let data_type = to_java_jdbc_parameter(param, idx + 1, jvm).unwrap();

            env.call_method(
                param_types,
                "add",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(data_type)],
            )
            .unwrap();
        }

        let data_map = env
            .new_object(
                "com/ansilo/connectors/mapping/SqliteJdbcDataMapping",
                "()V",
                &[],
            )
            .unwrap();

        let jdbc_prepared_query = env
            .new_object(
                "com/ansilo/connectors/query/JdbcPreparedQuery",
                "(Lcom/ansilo/connectors/mapping/JdbcDataMapping;Ljava/sql/PreparedStatement;Ljava/util/List;)V",
                &[JValue::Object(data_map),prepared_statement, JValue::Object(param_types)],
            )
            .unwrap();

        let jdbc_prepared_query = env.new_global_ref(jdbc_prepared_query).unwrap();

        JdbcPreparedQuery::new(Arc::clone(&jvm), jdbc_prepared_query, query)
    }

    #[test]
    fn test_prepared_query_no_params() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(&jvm, jdbc_con, "SELECT 1 as num", vec![]);

        let rs = prepared_query.execute_query().unwrap();
        let mut rs = ResultSetReader::new(rs).unwrap();

        assert_eq!(rs.read_data_value().unwrap(), Some(DataValue::Int32(1)));
        assert_eq!(rs.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_prepared_query_with_int_param() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(
            &jvm,
            jdbc_con,
            "SELECT ? as num",
            vec![QueryParam::dynamic2(1, DataType::Int32)],
        );

        let wrote = prepared_query
            .write(
                [
                    vec![1u8],                      // not null
                    123_i32.to_be_bytes().to_vec(), // value
                ]
                .concat()
                .as_slice(),
            )
            .unwrap();

        assert_eq!(wrote, 5);

        let rs = prepared_query.execute_query().unwrap();
        let mut rs = ResultSetReader::new(rs).unwrap();

        assert_eq!(rs.read_data_value().unwrap(), Some(DataValue::Int32(123)));
        assert_eq!(rs.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_prepared_query_with_varchar_param() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(
            &jvm,
            jdbc_con,
            "SELECT ? as str",
            vec![QueryParam::dynamic2(
                1,
                DataType::Utf8String(StringOptions::default()),
            )],
        );

        let wrote = prepared_query
            .write(
                [
                    vec![1u8],                 // not null
                    vec![3u8],                 // length
                    "abc".as_bytes().to_vec(), // data
                    vec![0u8],                 // eof
                ]
                .concat()
                .as_slice(),
            )
            .unwrap();

        assert_eq!(wrote, 6);

        let rs = prepared_query.execute_query().unwrap();
        let mut rs = ResultSetReader::new(rs).unwrap();

        assert_eq!(
            rs.read_data_value().unwrap(),
            Some(DataValue::Utf8String("abc".into()))
        );
        assert_eq!(rs.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_prepared_query_with_missing_param() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(
            &jvm,
            jdbc_con,
            "SELECT ? as num",
            vec![QueryParam::dynamic2(1, DataType::Int32)],
        );

        assert!(prepared_query.execute_query().is_err());
    }

    #[test]
    fn test_prepared_query_multiple_execute() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(
            &jvm,
            jdbc_con,
            "SELECT ? as num",
            vec![QueryParam::dynamic2(1, DataType::Int32)],
        );

        for i in [123_i32, 456, 789, 999] {
            let wrote = prepared_query
                .write(
                    [
                        vec![1u8],                // not null
                        i.to_be_bytes().to_vec(), // value
                    ]
                    .concat()
                    .as_slice(),
                )
                .unwrap();

            assert_eq!(wrote, 5);

            let rs = prepared_query.execute_query().unwrap();
            let mut rs = ResultSetReader::new(rs).unwrap();

            assert_eq!(rs.read_data_value().unwrap(), Some(DataValue::Int32(i)));
            assert_eq!(rs.read_data_value().unwrap(), None);

            prepared_query.restart().unwrap();
        }
    }

    #[test]
    fn test_prepared_query_with_constant_int_param() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(
            &jvm,
            jdbc_con,
            "SELECT ? as num",
            vec![QueryParam::Constant(DataValue::Int32(123))],
        );

        let rs = prepared_query.execute_query().unwrap();
        let mut rs = ResultSetReader::new(rs).unwrap();

        assert_eq!(rs.read_data_value().unwrap(), Some(DataValue::Int32(123)));
        assert_eq!(rs.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_prepared_query_get_logged() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(
            &jvm,
            jdbc_con,
            "SELECT ? as num, ? as str",
            vec![
                QueryParam::dynamic2(1, DataType::Int32),
                QueryParam::dynamic2(2, DataType::rust_string()),
            ],
        );

        prepared_query
            .write(
                [
                    vec![1u8],                      // not null
                    1234i32.to_be_bytes().to_vec(), // value
                    vec![1u8],                      // not null
                    vec![3u8],                      // length
                    "foo".as_bytes().to_vec(),      // data
                    vec![0u8],                      // eof
                ]
                .concat()
                .as_slice(),
            )
            .unwrap();

        let logged = prepared_query.logged().unwrap();

        assert_eq!(
            logged,
            LoggedQuery::new(
                "SELECT ? as num, ? as str",
                vec![
                    "LoggedParam [index=1, method=setInt, value=1234]".into(),
                    "LoggedParam [index=2, method=setString, value=foo]".into()
                ],
                None
            )
        );

        // Restart should clear query log
        prepared_query.restart().unwrap();
        let logged = prepared_query.logged().unwrap();
        assert_eq!(
            logged,
            LoggedQuery::new("SELECT ? as num, ? as str", vec![], None)
        );
    }

    #[test]
    fn test_jdbc_query_param_into_java_dynamic() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let param = QueryParam::dynamic2(1, DataType::Int32);

        let java_obj = to_java_jdbc_parameter(&param, 1, &jvm).unwrap();
        let class = jvm.env().unwrap().get_object_class(java_obj).unwrap();

        assert_eq!(
            jvm.env()
                .unwrap()
                .get_string(JString::from(
                    jvm.env()
                        .unwrap()
                        .call_method(*class, "getName", "()Ljava/lang/String;", &[])
                        .unwrap()
                        .l()
                        .unwrap()
                ))
                .unwrap()
                .to_str()
                .unwrap(),
            "com.ansilo.connectors.query.JdbcParameter"
        );
        assert_eq!(
            jvm.env()
                .unwrap()
                .call_method(java_obj, "isConstant", "()Z", &[])
                .unwrap()
                .z()
                .unwrap(),
            false
        );
    }

    #[test]
    fn test_jdbc_query_param_into_java_constant() {
        let jvm = Jvm::boot().unwrap();
        let param = QueryParam::Constant(DataValue::Int32(1123));

        let java_obj = to_java_jdbc_parameter(&param, 1, &jvm).unwrap();
        let class = jvm.env().unwrap().get_object_class(java_obj).unwrap();

        assert_eq!(
            jvm.env()
                .unwrap()
                .get_string(JString::from(
                    jvm.env()
                        .unwrap()
                        .call_method(*class, "getName", "()Ljava/lang/String;", &[])
                        .unwrap()
                        .l()
                        .unwrap()
                ))
                .unwrap()
                .to_str()
                .unwrap(),
            "com.ansilo.connectors.query.JdbcParameter"
        );
        assert_eq!(
            jvm.env()
                .unwrap()
                .call_method(java_obj, "isConstant", "()Z", &[])
                .unwrap()
                .z()
                .unwrap(),
            true
        );
    }
}

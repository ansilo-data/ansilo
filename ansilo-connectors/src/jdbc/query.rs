use std::sync::Arc;

use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, Context, Result},
};
use jni::{
    objects::{GlobalRef, JMethodID, JObject, JValue},
    signature::{JavaType, Primitive},
    sys::jmethodID,
};
use serde::Serialize;

use crate::{
    common::data::DataWriter,
    interface::{QueryHandle, QueryInputStructure},
};

use super::{JdbcDataType, JdbcResultSet, Jvm};

/// JDBC query
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JdbcQuery {
    /// The query (likely SQL) as a string
    pub query: String,
    /// Types of query parameters expected by the query
    pub params: Vec<JdbcQueryParam>,
}

/// JDBC query param
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum JdbcQueryParam {
    /// A dynamic query parameter that can modified for every query execution
    Dynamic(u32, DataType),
    /// A constant query parameter that is immutable across executions
    Constant(DataValue),
}

impl JdbcQuery {
    pub fn new(query: impl Into<String>, params: Vec<JdbcQueryParam>) -> Self {
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
    pub params: Vec<JdbcQueryParam>,
    write_method_id: Option<jmethodID>,
    as_read_only_buffer_method_id: Option<jmethodID>,
}

impl JdbcPreparedQuery {
    pub fn new(
        jvm: Arc<Jvm>,
        jdbc_prepared_statement: GlobalRef,
        params: Vec<JdbcQueryParam>,
    ) -> Self {
        Self {
            jvm,
            jdbc_prepared_statement,
            params,
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
        Ok(QueryInputStructure::new(
            self.params
                .iter()
                .filter_map(|i| match i {
                    JdbcQueryParam::Dynamic(id, data_type) => Some((*id, data_type.clone())),
                    JdbcQueryParam::Constant(_) => None,
                })
                .collect(),
        ))
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

                let byte_buff = env
                    .call_method_unchecked(
                        *byte_buff,
                        JMethodID::from(self.as_read_only_buffer_method_id.unwrap()),
                        JavaType::Object("java/nio/ByteBuffer".to_owned()),
                        &[],
                    )
                    .context("Failed to call ByteBuffer::asReadOnlyBuffer")?
                    .l()
                    .context("Failed to convert ByteBuffer to object")?;

                self.jvm.check_exceptions(env)?;

                byte_buff
            };

            let written = env
                .call_method_unchecked(
                    self.jdbc_prepared_statement.as_obj(),
                    JMethodID::from(self.write_method_id.unwrap()),
                    JavaType::Primitive(Primitive::Int),
                    &[JValue::Object(byte_buff)],
                )
                .context("Failed to invoke JdbcPreparedQuery::execute")?
                .i()
                .context("Failed to convert JdbcPreparedQuery::execute return value into int")?;

            self.jvm.check_exceptions(env)?;

            if written < 0 {
                bail!("JdbcPreparedQuery::execute returned value less than 0");
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

    fn execute(&mut self) -> Result<JdbcResultSet> {
        self.jvm.with_local_frame(32, |env| {
            let jdbc_result_set = env
                .call_method(
                    self.jdbc_prepared_statement.as_obj(),
                    "execute",
                    "()Lcom/ansilo/connectors/result/JdbcResultSet;",
                    &[],
                )
                .context("Failed to invoke JdbcPreparedQuery::execute")?
                .l()
                .context("Failed to convert JdbcResultSet into object")?;

            self.jvm.check_exceptions(env)?;

            let jdbc_result_set = env.new_global_ref(jdbc_result_set)?;

            Ok(JdbcResultSet::new(Arc::clone(&self.jvm), jdbc_result_set))
        })
    }
}

impl JdbcQueryParam {
    /// Initialises a new instance of the JdbcParameter class which
    /// copies the current query parameter
    /// @see ansilo-connectors/src/jdbc/java/src/main/java/com/ansilo/connectors/query/JdbcParameter.java
    pub(crate) fn to_java_jdbc_parameter<'a>(
        &self,
        index: usize,
        jvm: &'a Jvm,
    ) -> Result<JObject<'a>> {
        let env = jvm.env()?;

        let result = match self {
            JdbcQueryParam::Dynamic(_, data_type) => env.call_static_method(
                "com/ansilo/connectors/query/JdbcParameter",
                "createDynamic",
                "(II)Lcom/ansilo/connectors/query/JdbcParameter;",
                &[
                    JValue::Int(index as i32),
                    JValue::Int(
                        JdbcDataType(data_type.clone())
                            .try_into()
                            .context("Failed to parse query param data type")?,
                    ),
                ],
            ),
            JdbcQueryParam::Constant(data_value) => {
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
                        JValue::Int(
                            JdbcDataType(data_value.into())
                                .try_into()
                                .context("Failed to parse query param data type")?,
                        ),
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
}

#[cfg(test)]
mod tests {
    use ansilo_core::data::{DataValue, StringOptions};
    use jni::objects::{JObject, JString};

    use crate::{common::data::ResultSetReader, jdbc::tests::create_sqlite_memory_connection};

    use super::*;

    fn create_prepared_query(
        jvm: &Arc<Jvm>,
        jdbc_con: JObject,
        query: &str,
        params: Vec<JdbcQueryParam>,
    ) -> JdbcPreparedQuery {
        let env = &jvm.env().unwrap();

        let prepared_statement = env
            .call_method(
                jdbc_con,
                "prepareStatement",
                "(Ljava/lang/String;)Ljava/sql/PreparedStatement;",
                &[JValue::Object(*env.new_string(query).unwrap())],
            )
            .unwrap();

        let param_types = env.new_object("java/util/ArrayList", "()V", &[]).unwrap();

        for (idx, param) in params.iter().enumerate() {
            let data_type = param.to_java_jdbc_parameter(idx + 1, jvm).unwrap();

            env.call_method(
                param_types,
                "add",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(data_type)],
            )
            .unwrap();
        }

        let jdbc_prepared_query = env
            .new_object(
                "com/ansilo/connectors/query/JdbcPreparedQuery",
                "(Ljava/sql/PreparedStatement;Ljava/util/List;)V",
                &[prepared_statement, JValue::Object(param_types)],
            )
            .unwrap();

        let jdbc_prepared_query = env.new_global_ref(jdbc_prepared_query).unwrap();

        JdbcPreparedQuery::new(Arc::clone(&jvm), jdbc_prepared_query, params)
    }

    #[test]
    fn test_prepared_query_no_params() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(&jvm, jdbc_con, "SELECT 1 as num", vec![]);

        let rs = prepared_query.execute().unwrap();
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
            vec![JdbcQueryParam::Dynamic(1, DataType::Int32)],
        );

        let wrote = prepared_query
            .write(
                [
                    vec![1u8],                      // not null
                    123_i32.to_ne_bytes().to_vec(), // value
                ]
                .concat()
                .as_slice(),
            )
            .unwrap();

        assert_eq!(wrote, 5);

        let rs = prepared_query.execute().unwrap();
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
            vec![JdbcQueryParam::Dynamic(
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

        let rs = prepared_query.execute().unwrap();
        let mut rs = ResultSetReader::new(rs).unwrap();

        assert_eq!(
            rs.read_data_value().unwrap(),
            Some(DataValue::Utf8String("abc".as_bytes().to_vec()))
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
            vec![JdbcQueryParam::Dynamic(1, DataType::Int32)],
        );

        assert!(prepared_query.execute().is_err());
    }

    #[test]
    fn test_prepared_query_multiple_execute() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let jdbc_con = create_sqlite_memory_connection(&jvm);

        let mut prepared_query = create_prepared_query(
            &jvm,
            jdbc_con,
            "SELECT ? as num",
            vec![JdbcQueryParam::Dynamic(1, DataType::Int32)],
        );

        for i in [123_i32, 456, 789, 999] {
            let wrote = prepared_query
                .write(
                    [
                        vec![1u8],                // not null
                        i.to_ne_bytes().to_vec(), // value
                    ]
                    .concat()
                    .as_slice(),
                )
                .unwrap();

            assert_eq!(wrote, 5);

            let rs = prepared_query.execute().unwrap();
            let mut rs = ResultSetReader::new(rs).unwrap();

            assert_eq!(rs.read_data_value().unwrap(), Some(DataValue::Int32(i)));
            assert_eq!(rs.read_data_value().unwrap(), None);
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
            vec![JdbcQueryParam::Constant(DataValue::Int32(123))],
        );

        let rs = prepared_query.execute().unwrap();
        let mut rs = ResultSetReader::new(rs).unwrap();

        assert_eq!(rs.read_data_value().unwrap(), Some(DataValue::Int32(123)));
        assert_eq!(rs.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_jdbc_query_param_into_java_dynamic() {
        let jvm = Arc::new(Jvm::boot().unwrap());
        let param = JdbcQueryParam::Dynamic(1, DataType::Int32);

        let java_obj = param.to_java_jdbc_parameter(1, &jvm).unwrap();
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
        let param = JdbcQueryParam::Constant(DataValue::Int32(1123));

        let java_obj = param.to_java_jdbc_parameter(1, &jvm).unwrap();
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

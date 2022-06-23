use ansilo_core::{
    common::data::DataType,
    err::{bail, Context, Result},
};
use jni::{
    objects::{GlobalRef, JList, JMethodID, JString, JValue},
    signature::{JavaType, Primitive},
};

use crate::interface::{QueryHandle, QueryInputStructure};

use super::{JdbcDataType, JdbcResultSet, Jvm};

/// JDBC query
#[derive(Debug, Clone, PartialEq)]
pub struct JdbcQuery {
    /// The query (likely SQL) as a string
    pub query: String,
    /// Types of query parameters expected by the query
    pub params: Vec<DataType>,
}

impl JdbcQuery {
    pub fn new(query: String) -> Self {
        Self {
            query,
            params: vec![],
        }
    }
}

/// JDBC prepared query
pub struct JdbcPreparedQuery<'a> {
    pub jvm: &'a Jvm<'a>,
    pub jdbc_prepared_statement: GlobalRef,
    pub params: Vec<DataType>,
    write_method_id: Option<JMethodID<'a>>,
    as_read_only_buffer_method_id: Option<JMethodID<'a>>,
}

impl<'a> JdbcPreparedQuery<'a> {
    pub fn new(
        jvm: &'a Jvm<'a>,
        jdbc_prepared_statement: GlobalRef,
        params: Vec<DataType>,
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
        let env = &self.jvm.env;

        if self.write_method_id.is_none() {
            self.write_method_id = Some(
                env.get_method_id(
                    "com/ansilo/connectors/query/JdbcPreparedQuery",
                    "write",
                    "(Ljava/nio/ByteBuffer;)I",
                )
                .context("Failed to find JdbcPreparedQuery::write method")?,
            );
        }

        if self.as_read_only_buffer_method_id.is_none() {
            self.as_read_only_buffer_method_id = Some(
                env.get_method_id(
                    "java/nio/ByteBuffer",
                    "asReadOnlyByteBuffer",
                    "()Ljava/nio/ByteBuffer;",
                )
                .context("Failed to find ByteBuffer::asReadOnlyBuffer method")?,
            );
        }

        Ok(())
    }
}

impl<'a> QueryHandle<'a, JdbcResultSet<'a>> for JdbcPreparedQuery<'a> {
    fn get_structure(&self) -> Result<QueryInputStructure> {
        Ok(QueryInputStructure::new(self.params.clone()))
    }

    fn write(&mut self, buff: &[u8]) -> Result<usize> {
        self.init_method_ids()?;
        let env = &self.jvm.env;

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
                    self.as_read_only_buffer_method_id.unwrap(),
                    JavaType::Object("java/nio/ByteBuffer".to_owned()),
                    &[],
                )
                .context("Failed to call ByteBuffer::asReadOnlyBuffer")?
                .l()
                .context("Failed to convert ByteBuffer to object")?;

            byte_buff
        };

        let written = env
            .call_method_unchecked(
                self.jdbc_prepared_statement.as_obj(),
                self.write_method_id.unwrap(),
                JavaType::Primitive(Primitive::Int),
                &[JValue::Object(byte_buff)],
            )
            .context("Failed to invoke JdbcPreparedQuery::execute")?
            .i()
            .context("Failed to convert JdbcPreparedQuery::execute return value into int")?;

        if written < 0 {
            bail!("JdbcPreparedQuery::execute returned value less than 0");
        }

        // TODO: exception handling
        Ok(written.try_into().unwrap())
    }

    fn execute(&mut self) -> Result<JdbcResultSet<'a>> {
        let env = &self.jvm.env;

        let jdbc_result_set = env
            .call_method(
                self.jdbc_prepared_statement.as_obj(),
                "execute",
                "(Lcom/ansilo/connectors/query/JdbcPreparedQuery;)Lcom/ansilo/connectors/result/JdbcResultSet;",
                &[
                ]
            )
            .context("Failed to invoke JdbcConnection::execute")?
            .l()
            .context("Failed to convert JdbcResultSet into object")?;

        // TODO: exception handling

        let jdbc_result_set = env.new_global_ref(jdbc_result_set)?;

        Ok(JdbcResultSet::new(&self.jvm, jdbc_result_set))
    }
}

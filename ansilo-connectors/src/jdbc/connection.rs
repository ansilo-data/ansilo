use ansilo_core::err::{Context, Result};
use jni::{
    objects::{GlobalRef, JObject, JString, JValue},
    strings::JNIString,
    JNIEnv,
};

use crate::interface::{Connection, ConnectionOpener};

use super::{
    result_set::JdbcResultSet, JdbcConnectionConfig, JdbcDataType, JdbcPreparedQuery, JdbcQuery,
    Jvm,
};

/// Implementation for opening JDBC connections
pub struct JdbcConnectionOpener;

impl JdbcConnectionOpener {
    fn new() -> Self {
        Self {}
    }
}

impl<'a, TConnectionOptions> ConnectionOpener<TConnectionOptions, JdbcConnection<'a>>
    for JdbcConnectionOpener
where
    TConnectionOptions: JdbcConnectionConfig,
{
    fn open(&self, options: TConnectionOptions) -> Result<JdbcConnection<'a>> {
        let jvm = Jvm::boot()?;
        let env = &jvm.env;

        let url = env.new_string(options.get_jdbc_url())?;
        let props = env
            .new_object("java/util/Properties", "()V", &[])
            .context("Failed to create java properties")?;

        for (key, val) in options.get_jdbc_props().into_iter() {
            env.call_method(
                props,
                "setProperty",
                "(Ljava/lang/String;Ljava/lang/String;)V",
                &[
                    JValue::Object(*env.new_string(key)?),
                    JValue::Object(*env.new_string(val)?),
                ],
            )
            .context("Failed to set property")?;
        }

        let jdbc_con = env
            .new_object(
                "com/ansilo/connectors/JdbcConnection",
                "(Ljava/lang/String;Ljava/util/Properties;)V",
                &[JValue::Object(*url), JValue::Object(props)],
            )
            .context("Failed to initialise JDBC connection")?;

        let jdbc_con = env.new_global_ref(jdbc_con)?;

        Ok(JdbcConnection { jvm, jdbc_con })
    }
}

/// Implementation of the JDBC connection
pub struct JdbcConnection<'a> {
    jvm: Jvm<'a>,
    jdbc_con: GlobalRef,
}

impl<'a> Connection<'a, JdbcQuery, JdbcPreparedQuery<'a>> for JdbcConnection<'a> {
    fn prepare(&'a self, query: JdbcQuery) -> Result<JdbcPreparedQuery<'a>> {
        let env = &self.jvm.env;

        let param_types = env
            .new_object("java/util/ArrayList", "()V", &[])
            .context("Failed to create ArrayList")?;

        // TODO[minor]: use method id and unchecked call
        for val in query.params.iter() {
            let data_type_id = env
                .new_object(
                    "java/lang/Integer",
                    "(I)V",
                    &[JValue::Int(JdbcDataType(val.clone()).try_into()?)],
                )
                .context("Failed to convert data type id to java int")?;

            env.call_method(
                param_types,
                "add",
                "(Ljava/lang/Integer)V",
                &[JValue::Object(data_type_id)],
            )
            .context("Failed to add Integer to array list")?;
        }

        let jdbc_prepared_query = env
            .call_method(
                self.jdbc_con.as_obj(),
                "prepare",
                "(Ljava/lang/String;Ljava/util/List;)Lcom/ansilo/connectors/query/JdbcPreparedQuery;",
                &[JValue::Object(*env.new_string(query.query)?), JValue::Object(param_types)],
            )
            .context("Failed to invoke JdbcConnection::prepare")?
            .l()
            .context("Failed to convert JdbcPreparedQuery into object")?;

        // TODO: exception handling

        let jdbc_prepared_query = env.new_global_ref(jdbc_prepared_query)?;

        Ok(JdbcPreparedQuery::new(
            &self.jvm,
            jdbc_prepared_query,
            query.params,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    struct MockJdbcConnectionConfig;

    impl JdbcConnectionConfig for MockJdbcConnectionConfig {
        fn get_jdbc_url(&self) -> String {
            "jdbc:sqlite::memory:".to_string()
        }

        fn get_jdbc_props(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    #[test]
    fn test_init_jdbc_connection() {
        let con = JdbcConnectionOpener::new()
            .open(MockJdbcConnectionConfig)
            .unwrap();
    }
}

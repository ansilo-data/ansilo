use ansilo_core::err::{Context, Result};
use jni::{
    objects::{GlobalRef, JObject, JString, JValue},
    strings::JNIString,
    JNIEnv,
};

use crate::interface::{Connection, ConnectionOpener};

use super::{result_set::JdbcResultSet, JdbcConnectionConfig, Jvm};

/// Implementation for opening JDBC connections
pub struct JdbcConnectionOpener;

impl JdbcConnectionOpener {
    fn new () -> Self {Self {}}
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

impl<'a, TQuery> Connection<TQuery, JdbcResultSet> for JdbcConnection<'a> {
    fn execute(&self, query: TQuery) -> Result<JdbcResultSet> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    struct MockJdbcConnectionConfig;

    impl JdbcConnectionConfig for MockJdbcConnectionConfig {
        fn get_jdbc_url(&self) -> String {
            "test".to_string()
        }

        fn get_jdbc_props(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    #[test]
    fn test_init_jdbc_connection() {
        let con = JdbcConnectionOpener::new().open(MockJdbcConnectionConfig).unwrap();
    }
}

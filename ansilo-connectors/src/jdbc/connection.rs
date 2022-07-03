use ansilo_core::err::{Context, Result};
use ansilo_logging::warn;
use jni::objects::{GlobalRef, JValue};

use crate::interface::{Connection, ConnectionPool};

use super::{JdbcConnectionConfig, JdbcPreparedQuery, JdbcQuery, Jvm};

/// Implementation for opening JDBC connections
pub struct JdbcConnectionPool<TConnectionOptions: JdbcConnectionConfig> {
    options: TConnectionOptions,
}

impl<TConnectionOptions: JdbcConnectionConfig> JdbcConnectionPool<TConnectionOptions> {
    pub fn new(options: TConnectionOptions) -> Result<Self> {
        let pool = Self { options };

        if let Some(_conf) = pool.options.get_pool_config().as_ref() {
            // TODO: Implement connection pool
            todo!();
        }

        Ok(pool)
    }
}

impl<'a, TConnectionOptions: JdbcConnectionConfig> JdbcConnectionPool<TConnectionOptions> {
    fn create_connection(&mut self) -> Result<JdbcConnection<'a>> {
        let jvm = Jvm::boot()?;

        let jdbc_con = jvm.with_local_frame(32, |env| {
            let url = env.new_string(self.options.get_jdbc_url())?;
            let props = env
                .new_object("java/util/Properties", "()V", &[])
                .context("Failed to create java properties")?;

            for (key, val) in self.options.get_jdbc_props().into_iter() {
                env.call_method(
                    props,
                    "setProperty",
                    "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/Object;",
                    &[
                        JValue::Object(env.auto_local(env.new_string(key)?).as_obj()),
                        JValue::Object(env.auto_local(env.new_string(val)?).as_obj()),
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

            Ok(jdbc_con)
        })?;

        Ok(JdbcConnection { jvm, jdbc_con })
    }
}

impl<'a, TConnectionOptions: JdbcConnectionConfig> ConnectionPool<JdbcConnection<'a>>
    for JdbcConnectionPool<TConnectionOptions>
{
    fn acquire(&mut self) -> Result<JdbcConnection<'a>> {
        self.create_connection()
    }
}

/// Implementation of the JDBC connection
pub struct JdbcConnection<'a> {
    jvm: Jvm<'a>,
    jdbc_con: GlobalRef,
}

impl<'a> Connection<'a, JdbcQuery, JdbcPreparedQuery<'a>> for JdbcConnection<'a> {
    fn prepare(&'a self, query: JdbcQuery) -> Result<JdbcPreparedQuery<'a>> {
        let jdbc_prepared_query = self.jvm.with_local_frame(32, |env| {
            let param_types = env
                .new_object("java/util/ArrayList", "()V", &[])
                .context("Failed to create ArrayList")?;

            // TODO[minor]: use method id and unchecked call
            for (idx, param) in query.params.iter().enumerate() {
                let data_type_id = env.auto_local(
                    param.to_java_jdbc_parameter(idx + 1, &self.jvm)?
                );

                env.call_method(
                    param_types,
                    "add",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(data_type_id.as_obj())],
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

            Ok(jdbc_prepared_query)
        })?;

        Ok(JdbcPreparedQuery::new(
            &self.jvm,
            jdbc_prepared_query,
            query.params,
        ))
    }
}

impl<'a> JdbcConnection<'a> {
    /// Checks whether the connection is valid
    pub fn is_valid(&'a self) -> Result<bool> {
        let env = &self.jvm.env;
        let timeout_sec = 30; // TODO: make configurable

        let res = env
            .call_method(
                self.jdbc_con.as_obj(),
                "isValid",
                "(I)Z",
                &[JValue::Int(timeout_sec)],
            )
            .context("Failed to invoke JdbcConnection::isValid")?
            .z()
            .context("Failed to convert JdbcConnection::isValid return value")?;
        Ok(res)
    }

    /// Checks whether the connection is closed
    pub fn is_closed(&'a self) -> Result<bool> {
        let env = &self.jvm.env;

        let res = env
            .call_method(self.jdbc_con.as_obj(), "isClosed", "()Z", &[])
            .context("Failed to invoke JdbcConnection::isClosed")?
            .z()
            .context("Failed to convert JdbcConnection::isClosed return value")?;
        Ok(res)
    }
}

impl<'a> JdbcConnection<'a> {
    fn close(&mut self) -> Result<()> {
        self.jvm
            .env
            .call_method(self.jdbc_con.as_obj(), "close", "()V", &[])
            .context("Failed to call JdbcConnection::close")?;
        Ok(())
    }
}

impl<'a> Drop for JdbcConnection<'a> {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            warn!("Failed to close JDBC connection: {:?}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ansilo_core::{common::data::DataType, config::NodeConfig};

    use crate::{
        interface::{QueryHandle, QueryInputStructure},
        jdbc::{JdbcConnectionPoolConfig, JdbcQueryParam},
    };

    use super::*;

    struct MockJdbcConnectionConfig(String, HashMap<String, String>);

    impl JdbcConnectionConfig for MockJdbcConnectionConfig {
        fn get_jdbc_url(&self) -> String {
            self.0.clone()
        }

        fn get_jdbc_props(&self) -> HashMap<String, String> {
            self.1.clone()
        }

        fn get_pool_config(&self) -> Option<JdbcConnectionPoolConfig> {
            None
        }
    }

    fn init_sqlite_connection<'a>() -> JdbcConnection<'a> {
        JdbcConnectionPool::new(MockJdbcConnectionConfig(
            "jdbc:sqlite::memory:".to_owned(),
            HashMap::new(),
        ))
        .unwrap()
        .acquire()
        .unwrap()
    }

    #[test]
    fn test_jdbc_connection_init_sqlite() {
        init_sqlite_connection();
    }

    #[test]
    fn test_jdbc_connection_init_invalid() {
        let res = JdbcConnectionPool::new(MockJdbcConnectionConfig(
            "invalid".to_owned(),
            HashMap::new(),
        ))
        .unwrap()
        .acquire();

        assert!(res.is_err());
    }

    #[test]
    fn test_jdbc_connection_prepare_statement() {
        let con = init_sqlite_connection();

        let query = JdbcQuery::new("SELECT 1 as num", vec![]);
        let statement = con.prepare(query).unwrap();

        assert_eq!(
            statement.get_structure().unwrap(),
            QueryInputStructure::new(vec![])
        );
    }

    #[test]
    fn test_jdbc_connection_prepare_statement_with_param() {
        let con = init_sqlite_connection();

        let mut query = JdbcQuery::new("SELECT ? as num", vec![]);
        query.params.push(JdbcQueryParam::Dynamic(DataType::Int32));
        let statement = con.prepare(query).unwrap();

        assert_eq!(
            statement.get_structure().unwrap(),
            QueryInputStructure::new(vec![DataType::Int32])
        );
    }

    #[test]
    fn test_jdbc_connection_prepare_statement_invalid() {
        let con = init_sqlite_connection();

        let query = JdbcQuery::new("INVALID QUERY", vec![]);
        let res = con.prepare(query);
        assert!(res.is_err());
    }

    #[test]
    fn test_jdbc_connection_close() {
        let mut con = init_sqlite_connection();

        con.close().unwrap();
    }

    #[test]
    fn test_jdbc_connection_is_valid() {
        let mut con = init_sqlite_connection();

        assert_eq!(con.is_valid().unwrap(), true);

        con.close().unwrap();

        assert_eq!(con.is_valid().unwrap(), false);
    }

    #[test]
    fn test_jdbc_connection_is_closed() {
        let mut con = init_sqlite_connection();

        assert_eq!(con.is_closed().unwrap(), false);

        con.close().unwrap();

        assert_eq!(con.is_closed().unwrap(), true);
    }
}

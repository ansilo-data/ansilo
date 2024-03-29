use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use ansilo_core::{
    auth::AuthContext,
    config::ResourceConfig,
    data::DataValue,
    err::{bail, Context, Result},
};
use ansilo_logging::{debug, trace, warn};
use ansilo_util_r2d2::manager::{OurManageConnection, R2d2Adaptor};
use jni::objects::{GlobalRef, JValue};
use r2d2::PooledConnection;

use ansilo_connectors_base::{
    common::query::QueryParam,
    interface::{Connection, ConnectionPool, QueryHandle, TransactionManager},
};

use crate::{to_java_jdbc_parameter, JdbcResultSet};

use super::{JdbcConnectionConfig, JdbcPreparedQuery, JdbcQuery, Jvm};

/// Implementation for opening JDBC connections
#[derive(Clone)]
pub struct JdbcConnectionPool {
    pool: r2d2::Pool<R2d2Adaptor<Manager>>,
}

struct Manager {
    jvm: Arc<Jvm>,
    jdbc_url: String,
    jdbc_props: HashMap<String, String>,
    init_queries: Vec<String>,
    connection_class: String,
    data_mapping_class: String,
    supports_batching: bool,
}

impl JdbcConnectionPool {
    pub fn new<TConnectionOptions: JdbcConnectionConfig>(
        conf: &ResourceConfig,
        options: TConnectionOptions,
    ) -> Result<Self> {
        let jvm = Jvm::boot(Some(conf))?;
        let manager = Manager {
            jvm: Arc::new(jvm),
            jdbc_url: options.get_jdbc_url(),
            jdbc_props: options.get_jdbc_props(),
            init_queries: options.get_initialisation_queries(),
            connection_class: options.get_java_connection().replace('.', "/"),
            data_mapping_class: options.get_java_jdbc_data_mapping().replace('.', "/"),
            supports_batching: options.supports_query_batching(),
        }
        .adaptor();

        let pool = if let Some(conf) = options.get_pool_config().as_ref() {
            r2d2::Builder::new()
                .min_idle(Some(conf.min_cons))
                .max_size(conf.max_cons)
                .max_lifetime(conf.max_lifetime)
                .idle_timeout(conf.idle_timeout)
                .connection_timeout(conf.connect_timeout.unwrap_or(Duration::from_secs(30)))
                .build(manager)
                .context("Failed to build connection pool")?
        } else {
            r2d2::Builder::new()
                .min_idle(Some(0))
                .max_size(1000) // TODO: fix constant max for unpooled connections
                .max_lifetime(Some(Duration::from_micros(1))) // TODO: fix constant values
                .connection_timeout(Duration::from_secs(60))
                .build(manager)
                .context("Failed to build connection pool")?
        };

        Ok(Self { pool })
    }
}

impl OurManageConnection for Manager {
    type Connection = Arc<JdbcConnectionState>;

    fn connect(&self) -> Result<Self::Connection> {
        let jdbc_con = self
            .jvm
            .with_local_frame(32, |env| {
                let url = env.new_string(self.jdbc_url.clone())?;
                let props = env
                    .new_object("java/util/Properties", "()V", &[])
                    .context("Failed to create java properties")?;

                for (key, val) in self.jdbc_props.iter() {
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

                    self.jvm.check_exceptions(env)?;
                }

                let data_map = env
                    .new_object(&self.data_mapping_class, "()V", &[])
                    .context("Failed to initialise JDBC data mapping")?;

                self.jvm.check_exceptions(env)?;

                let jdbc_con = env
                    .new_object(
                        &self.connection_class,
                        "(Ljava/lang/String;Ljava/util/Properties;Lcom/ansilo/connectors/mapping/JdbcDataMapping;)V",
                        &[JValue::Object(*url), JValue::Object(props), JValue::Object(data_map)],
                    )
                    .context("Failed to initialise JDBC connection")?;

                self.jvm.check_exceptions(env)?;

                let jdbc_con = env.new_global_ref(jdbc_con)?;

                Ok(jdbc_con)
            })?;

        let state = Arc::new(JdbcConnectionState {
            jvm: Arc::clone(&self.jvm),
            supports_batching: self.supports_batching,
            jdbc_con,
            closed: Mutex::new(false),
        });

        if !self.init_queries.is_empty() {
            for query in self.init_queries.iter() {
                debug!("Running connection init query: {query}");

                prepare_query(JdbcQuery::new(query, vec![]), &state)
                    .and_then(|mut q| q.execute_modify())
                    .with_context(|| format!("Failed to run initialisation query: '{query}'"))?;
            }
        }

        Ok(state)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<()> {
        conn.is_valid()
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed().unwrap_or(true)
    }
}

impl ConnectionPool for JdbcConnectionPool {
    type TConnection = JdbcConnection;

    fn acquire(&mut self, _auth: Option<&AuthContext>) -> Result<JdbcConnection> {
        let state = self
            .pool
            .get()
            .context("Failed to get connection from pool")?;
        let tm_state = state.clone();
        Ok(JdbcConnection(state, JdbcTransactionManager(tm_state)))
    }
}

/// Wrapper of of the JDBC connection
pub struct JdbcConnection(
    PooledConnection<R2d2Adaptor<Manager>>,
    JdbcTransactionManager,
);

impl JdbcConnection {
    /// Gets a reference to the jvm
    pub fn jvm(&self) -> &Arc<Jvm> {
        &self.1 .0.jvm
    }
}

/// Implementation of the JDBC connection
struct JdbcConnectionState {
    jvm: Arc<Jvm>,
    jdbc_con: GlobalRef,
    supports_batching: bool,
    closed: Mutex<bool>,
}

impl Connection for JdbcConnection {
    type TQuery = JdbcQuery;
    type TQueryHandle = JdbcPreparedQuery;
    type TTransactionManager = JdbcTransactionManager;

    fn prepare(&mut self, query: JdbcQuery) -> Result<JdbcPreparedQuery> {
        let state = &*self.0;
        prepare_query(query, state)
    }

    fn transaction_manager(&mut self) -> Option<&mut Self::TTransactionManager> {
        Some(&mut self.1)
    }
}

fn prepare_query(query: JdbcQuery, state: &JdbcConnectionState) -> Result<JdbcPreparedQuery> {
    debug!("Preparing query: {}", query.query);

    let capacity = query.params.len() * 2 + 5;
    let jdbc_prepared_query = state.jvm.with_local_frame(capacity as _, |env| {
        let param_types = env
            .new_object("java/util/ArrayList", "()V", &[])
            .context("Failed to create ArrayList")?;

        state.jvm.check_exceptions(env)?;

        // TODO[minor]: use method id and unchecked call
        for (idx, param) in query.params.iter().enumerate() {
            let data_type_id = env.auto_local(
                to_java_jdbc_parameter(param, idx + 1, &state.jvm)?
            );

            env.call_method(
                param_types,
                "add",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(data_type_id.as_obj())],
            )
            .context("Failed to add Integer to array list")?;

            state.jvm.check_exceptions(env)?;
        }

        let jdbc_prepared_query = env
            .call_method(
                state.jdbc_con.as_obj(),
                "prepare",
                "(Ljava/lang/String;Ljava/util/List;)Lcom/ansilo/connectors/query/JdbcPreparedQuery;",
                &[JValue::Object(*env.new_string(query.query.clone())?), JValue::Object(param_types)],
            )
            .context("Failed to invoke JdbcConnection::prepare")?
            .l()
            .context("Failed to convert JdbcPreparedQuery into object")?;

        state.jvm.check_exceptions(env)?;

        let jdbc_prepared_query = env.new_global_ref(jdbc_prepared_query)?;

        Ok(jdbc_prepared_query)
    })?;

    Ok(JdbcPreparedQuery::new(
        Arc::clone(&state.jvm),
        jdbc_prepared_query,
        query,
        state.supports_batching,
    ))
}

impl JdbcConnection {
    /// Executes the supplied sql on the connection
    pub fn execute(
        &mut self,
        query: impl Into<String>,
        params: Vec<DataValue>,
    ) -> Result<JdbcResultSet> {
        let jdbc_params = params
            .iter()
            .map(|p| QueryParam::constant(p.clone()))
            .collect::<Vec<_>>();

        let mut prepared = self.prepare(JdbcQuery::new(query, jdbc_params))?;

        prepared.execute_query()
    }
}

impl JdbcConnectionState {
    /// Checks whether the connection is valid
    pub fn is_valid(&self) -> Result<()> {
        if *self.closed()? {
            bail!("Connection closed");
        }

        let env = self.jvm.env()?;
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

        self.jvm.check_exceptions(&env)?;

        if !res {
            bail!("Connection is not valid")
        }

        Ok(())
    }

    /// Checks whether the connection is closed
    pub fn is_closed(&self) -> Result<bool> {
        if *self.closed()? {
            return Ok(true);
        }

        let env = self.jvm.env()?;
        let res = env
            .call_method(self.jdbc_con.as_obj(), "isClosed", "()Z", &[])
            .context("Failed to invoke JdbcConnection::isClosed")?
            .z()
            .context("Failed to convert JdbcConnection::isClosed return value")?;

        self.jvm.check_exceptions(&env)?;

        Ok(res)
    }

    fn close(&self) -> Result<()> {
        let mut closed = self.closed()?;

        if *closed {
            return Ok(());
        }

        let env = self.jvm.env()?;
        env.call_method(self.jdbc_con.as_obj(), "close", "()V", &[])
            .context("Failed to call JdbcConnection::close")?;

        self.jvm.check_exceptions(&env)?;

        *closed = true;
        Ok(())
    }

    fn closed<'a>(&'a self) -> Result<MutexGuard<'a, bool>> {
        match self.closed.lock() {
            Ok(g) => Ok(g),
            Err(_) => bail!("Failed to lock connection closed mutex"),
        }
    }
}

impl Drop for JdbcConnectionState {
    fn drop(&mut self) {
        trace!("Dropping JDBC connection");
        if let Err(err) = self.close() {
            warn!("Failed to close JDBC connection: {:?}", err);
        }
    }
}

/// Transaction manager for a JDBC connection
pub struct JdbcTransactionManager(Arc<JdbcConnectionState>);

impl TransactionManager for JdbcTransactionManager {
    fn is_in_transaction(&mut self) -> Result<bool> {
        let env = self.0.jvm.env()?;
        let res = env
            .call_method(self.0.jdbc_con.as_obj(), "isInTransaction", "()Z", &[])
            .context("Failed to invoke JdbcConnection::isInTransaction")?
            .z()
            .context("Failed to convert JdbcConnection::isInTransaction return value")?;
        self.0.jvm.check_exceptions(&env)?;

        Ok(res)
    }

    fn begin_transaction(&mut self) -> Result<()> {
        let env = self.0.jvm.env()?;
        env.call_method(self.0.jdbc_con.as_obj(), "beginTransaction", "()V", &[])
            .context("Failed to invoke JdbcConnection::beginTransaction")?;
        self.0.jvm.check_exceptions(&env)?;

        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        let env = self.0.jvm.env()?;
        env.call_method(self.0.jdbc_con.as_obj(), "rollBackTransaction", "()V", &[])
            .context("Failed to invoke JdbcConnection::rollBackTransaction")?;
        self.0.jvm.check_exceptions(&env)?;

        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        let env = self.0.jvm.env()?;
        env.call_method(self.0.jdbc_con.as_obj(), "commitTransaction", "()V", &[])
            .context("Failed to invoke JdbcConnection::commitTransaction")?;
        self.0.jvm.check_exceptions(&env)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ansilo_core::data::{DataType, DataValue};

    use crate::JdbcConnectionPoolConfig;
    use ansilo_connectors_base::{
        common::data::ResultSetReader,
        interface::{QueryHandle, QueryInputStructure, ResultSet},
    };

    use super::*;

    #[derive(Clone)]
    struct MockSqliteJdbcConnectionConfig(String, HashMap<String, String>);

    impl JdbcConnectionConfig for MockSqliteJdbcConnectionConfig {
        fn get_jdbc_url(&self) -> String {
            self.0.clone()
        }

        fn get_jdbc_props(&self) -> HashMap<String, String> {
            self.1.clone()
        }

        fn get_pool_config(&self) -> Option<JdbcConnectionPoolConfig> {
            None
        }

        fn get_java_jdbc_data_mapping(&self) -> String {
            "com.ansilo.connectors.mapping.SqliteJdbcDataMapping".into()
        }
    }

    fn init_sqlite_connection() -> JdbcConnection {
        JdbcConnectionPool::new(
            &ResourceConfig::default(),
            MockSqliteJdbcConnectionConfig("jdbc:sqlite::memory:".to_owned(), HashMap::new()),
        )
        .unwrap()
        .acquire(None)
        .unwrap()
    }

    #[test]
    fn test_jdbc_connection_init_sqlite() {
        init_sqlite_connection();
    }

    #[test]
    fn test_jdbc_connection_init_invalid() {
        let res = JdbcConnectionPool::new(
            &ResourceConfig::default(),
            MockSqliteJdbcConnectionConfig("invalid".to_owned(), HashMap::new()),
        )
        .unwrap()
        .acquire(None);

        assert!(res.is_err());
    }

    #[test]
    fn test_jdbc_connection_prepare_statement() {
        let mut con = init_sqlite_connection();

        let query = JdbcQuery::new("SELECT 1 as num", vec![]);
        let statement = con.prepare(query).unwrap();

        assert_eq!(
            statement.get_structure().unwrap(),
            QueryInputStructure::new(vec![])
        );
    }

    #[test]
    fn test_jdbc_connection_prepare_statement_with_param() {
        let mut con = init_sqlite_connection();

        let mut query = JdbcQuery::new("SELECT ? as num", vec![]);
        query.params.push(QueryParam::dynamic2(1, DataType::Int32));
        let statement = con.prepare(query).unwrap();

        assert_eq!(
            statement.get_structure().unwrap(),
            QueryInputStructure::new(vec![(1, DataType::Int32)])
        );
    }

    #[test]
    fn test_jdbc_connection_prepare_statement_invalid() {
        let mut con = init_sqlite_connection();

        let query = JdbcQuery::new("INVALID QUERY", vec![]);
        let res = con.prepare(query);
        assert!(res.is_err());
    }

    #[test]
    fn test_jdbc_connection_transaction() {
        let mut con = init_sqlite_connection();

        let query = JdbcQuery::new("CREATE TABLE dummy (x INT);", vec![]);
        con.prepare(query).unwrap().execute_query().unwrap();

        {
            let tm = con.transaction_manager().unwrap();
            assert_eq!(tm.is_in_transaction().unwrap(), false);
            tm.begin_transaction().unwrap();
            assert_eq!(tm.is_in_transaction().unwrap(), true);
        }

        let query = JdbcQuery::new("INSERT INTO dummy VALUES (1);", vec![]);
        con.prepare(query).unwrap().execute_query().unwrap();

        con.transaction_manager()
            .unwrap()
            .rollback_transaction()
            .unwrap();

        let query = JdbcQuery::new("SELECT COUNT(*) FROM dummy", vec![]);
        let res = con.prepare(query).unwrap().execute_query().unwrap();
        let mut res = ResultSetReader::new(res).unwrap();
        assert_eq!(res.read_data_value().unwrap().unwrap(), DataValue::Int32(0));

        con.transaction_manager()
            .unwrap()
            .begin_transaction()
            .unwrap();

        let query = JdbcQuery::new("INSERT INTO dummy VALUES (1);", vec![]);
        con.prepare(query).unwrap().execute_query().unwrap();

        con.transaction_manager()
            .unwrap()
            .commit_transaction()
            .unwrap();

        let query = JdbcQuery::new("SELECT COUNT(*) FROM dummy", vec![]);
        let res = con.prepare(query).unwrap().execute_query().unwrap();
        let mut res = ResultSetReader::new(res).unwrap();
        assert_eq!(res.read_data_value().unwrap().unwrap(), DataValue::Int32(1));
    }

    #[test]
    fn test_jdbc_connection_execute_no_params() {
        let mut con = init_sqlite_connection();

        let results = con.execute("SELECT 123 as num", vec![]).unwrap();

        assert_eq!(
            results.reader().unwrap().read_data_value().unwrap(),
            Some(DataValue::Int32(123))
        );
    }

    #[test]
    fn test_jdbc_connection_execute_with_params() {
        let mut con = init_sqlite_connection();

        let mut results = con
            .execute(
                "SELECT ? as num, ? as str",
                vec![DataValue::Int32(123), DataValue::Utf8String("foo".into())],
            )
            .unwrap()
            .reader()
            .unwrap();

        assert_eq!(
            results.read_data_value().unwrap(),
            Some(DataValue::Int32(123))
        );

        assert_eq!(
            results.read_data_value().unwrap(),
            Some(DataValue::Utf8String("foo".into()))
        );

        assert_eq!(results.read_data_value().unwrap(), None);
    }

    #[test]
    fn test_jdbc_connection_close() {
        let con = init_sqlite_connection();
        let con = con.0;

        con.close().unwrap();
    }

    #[test]
    fn test_jdbc_connection_is_valid() {
        let con = init_sqlite_connection();
        let con = con.0;

        con.is_valid().unwrap();

        con.close().unwrap();

        con.is_valid().unwrap_err();
    }

    #[test]
    fn test_jdbc_connection_is_closed() {
        let mut con = init_sqlite_connection();
        let con = &mut *con.0;

        assert_eq!(con.is_closed().unwrap(), false);

        con.close().unwrap();

        assert_eq!(con.is_closed().unwrap(), true);
    }
}

use ansilo_core::err::{Context, Result};
use jni::{InitArgsBuilder, JNIVersion, JavaVM};

use crate::interface::{Connection, ConnectionOpener};

use super::{result_set::JdbcResultSet, JdbcConnectionConfig, Jvm};

/// Implementation for opening JDBC connections
pub struct JdbcConnectionOpener {}

impl<TConnectionOptions> ConnectionOpener<TConnectionOptions, JdbcConnection>
    for JdbcConnectionOpener
where
    TConnectionOptions: JdbcConnectionConfig,
{
    fn open(&self, options: TConnectionOptions) -> Result<JdbcConnection> {
        let jvm = Jvm::boot()?;

        // TODO: instatiate connection class
        // jvm.instance.get_env()?.new_ob

        Ok(JdbcConnection { jvm })
    }
}

/// Implementation of the JDBC connection
pub struct JdbcConnection {
    jvm: Jvm,
}

impl<TQuery> Connection<TQuery, JdbcResultSet> for JdbcConnection {
    fn execute(&self, query: TQuery) -> Result<JdbcResultSet> {
        todo!()
    }
}

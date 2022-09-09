use std::{collections::HashMap, time::Duration};

use crate::conf::PostgresConf;
use ansilo_core::err::{bail, Result};
use ansilo_logging::warn;
use deadpool::managed::Object;

use super::pool::{
    LlPostgresConnectionManager, LlPostgresConnectionPool, LlPostgresConnectionPoolConfig,
};

/// We support authenticating to postgres as mutliple users.
/// Each connection pool contains connections authenticated
/// under a particular user.
#[derive(Clone)]
pub struct MultiUserPostgresConnectionPool {
    /// The connection pools
    pools: HashMap<String, LlPostgresConnectionPool>,
}

/// Configuration options for the pool
#[derive(Debug, Clone, PartialEq)]
pub struct MultiUserPostgresConnectionPoolConfig {
    pub pg: &'static PostgresConf,
    pub users: Vec<String>,
    pub database: String,
    pub max_cons_per_user: usize,
    pub connect_timeout: Duration,
}

impl MultiUserPostgresConnectionPool {
    /// Creates a new multi-user connection pool
    pub fn new(conf: MultiUserPostgresConnectionPoolConfig) -> Result<Self> {
        let pools = conf
            .users
            .iter()
            .map(|user| {
                Ok((
                    user.to_string(),
                    LlPostgresConnectionPool::new(LlPostgresConnectionPoolConfig {
                        pg: conf.pg,
                        user: user.into(),
                        database: conf.database.clone(),
                        max_size: conf.max_cons_per_user,
                        connect_timeout: conf.connect_timeout,
                    })?,
                ))
            })
            .collect::<Result<HashMap<String, _>>>()?;

        Ok(Self { pools })
    }

    /// Acquires a connection which has been authenticated as the supplied user
    pub async fn acquire(&self, username: &str) -> Result<Object<LlPostgresConnectionManager>> {
        let pool = match self.pools.get(username) {
            Some(pool) => pool,
            None => {
                warn!(
                    "User '{}' has not been configured in the connecton pool",
                    username
                );
                bail!(
                    "User '{}' has not been configured in the connecton pool",
                    username
                )
            }
        };

        pool.acquire().await
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn test_pg_config(test_name: &'static str) -> &'static PostgresConf {
        let conf = PostgresConf {
            install_dir: PathBuf::from("/home/vscode/.pgx/14.5/pgx-install/"),
            postgres_conf_path: None,
            data_dir: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-multi-user-ll-connection-pool/{}",
                test_name
            )),
            socket_dir_path: PathBuf::from(format!(
                "/tmp/ansilo-tests/pg-multi-user-ll-connection-pool/{}",
                test_name
            )),
            fdw_socket_path: PathBuf::from("not-used"),
            app_users: vec![],
            init_db_sql: vec![],
        };
        Box::leak(Box::new(conf))
    }

    #[tokio::test]
    async fn test_postgres_connection_pool_new() {
        let conf = test_pg_config("new");
        let pool = MultiUserPostgresConnectionPool::new(MultiUserPostgresConnectionPoolConfig {
            pg: conf,
            users: vec!["user1".into(), "user2".into()],
            database: "postgres".into(),
            max_cons_per_user: 5,
            connect_timeout: Duration::from_secs(1),
        })
        .unwrap();

        assert!(pool.pools.contains_key("user1"));
        assert!(pool.pools.contains_key("user2"));
    }
}

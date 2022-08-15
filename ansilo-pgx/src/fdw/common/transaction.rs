use ::std::os::raw::c_void;
use std::{
    collections::HashMap,
    ptr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use ansilo_core::err::{bail, Context, Error, Result};
use ansilo_pg::fdw::proto::{ClientMessage, ServerMessage};

use lazy_static::lazy_static;
use pgx::{
    pg_sys::{SubTransactionId, SubXactEvent, XactEvent},
    *,
};

use super::FdwIpcConnection;

/// Postgres allows us to handle transaction events via callbacks.
/// These need to be registered so upon the first remote transaction
/// we register our callbacks.
static REGISTERED_CALLBACKS: AtomicBool = AtomicBool::new(false);

lazy_static! {
    /// The global list of all active remote transactions that are
    /// managed by the top level postgres transaction.
    /// Currently we do not support sub transactions.
    static ref ACTIVE_TRANSACTIONS: Mutex<HashMap<String, RemoteTransaction>> = Mutex::new(HashMap::new());
}

/// An active transaction on a data source
struct RemoteTransaction {
    /// The connection to the data source
    /// We keep a strong reference to the connection so as to keep it
    /// from being dropped until the transaction is completed.
    con: Arc<FdwIpcConnection>,
}

impl RemoteTransaction {
    fn new(con: Arc<FdwIpcConnection>) -> Self {
        Self { con }
    }
}

/// Starts a remote transaction on the supplied data source if it
/// supports transactions.
///
/// If transactions are not supported we display a warning.
pub(crate) unsafe fn begin_remote_transaction(con: &Arc<FdwIpcConnection>) -> Result<()> {
    if !REGISTERED_CALLBACKS.load(Ordering::SeqCst) {
        pg_sys::RegisterXactCallback(Some(handle_transaction_event), ptr::null_mut());
        pg_sys::RegisterSubXactCallback(Some(handle_sub_transaction_event), ptr::null_mut());
        REGISTERED_CALLBACKS.store(true, Ordering::SeqCst);
    }

    let mut active = get_active_transactions()?;

    // If there is already a transaction registered for this data source
    // no further action is needed.
    if active.contains_key(&con.data_source_id) {
        return Ok(());
    }
    pgx::debug1!(
        "Starting transaction on connection {}",
        con.data_source_id.clone()
    );

    match con.send(ClientMessage::BeginTransaction) {
        Ok(ServerMessage::TransactionBegun) => {
            pgx::debug1!(
                "Transaction started on connection {}",
                con.data_source_id.clone()
            );

            // After starting the remote transaction we store the connection
            // in our global state, with a strong reference, to keep the connection
            // alive until the postgres transaction is completed.
            active.insert(
                con.data_source_id.clone(),
                RemoteTransaction::new(Arc::clone(&con)),
            );
        }
        Ok(ServerMessage::TransactionsNotSupported) => {
            let log_level = if pg_sys::GetCurrentTransactionNestLevel() == 0 {
                PgLogLevel::DEBUG1
            } else {
                PgLogLevel::WARNING
            };
            pgx::elog(
                log_level,
                &format!(
                    "Transactions are not supported on connection {}",
                    con.data_source_id.clone()
                ),
            );
        }
        Ok(res) => bail!(
            "Failed to start transaction on connection {}, unexpected response: {:?}",
            con.data_source_id.clone(),
            res
        ),
        Err(err) => bail!(
            "Failed to start transaction on connection {}: {:?}",
            con.data_source_id.clone(),
            err
        ),
    };

    Ok(())
}

/// Handles transaction events from postgres
unsafe extern "C" fn handle_transaction_event(event: XactEvent, _arg: *mut c_void) {
    match event {
        // If we are committing the postgres transaction we try
        // commit any active remote transactions
        pg_sys::XactEvent_XACT_EVENT_PARALLEL_PRE_COMMIT
        | pg_sys::XactEvent_XACT_EVENT_PRE_COMMIT => {
            if let Err(err) = commit_remote_transactions() {
                pgx::error!("Failed to commit remote transactions: {:?}", err);
            }
        }
        // If we are aborting the current transaction we rollback
        // any active remote transactions
        pg_sys::XactEvent_XACT_EVENT_PARALLEL_ABORT | pg_sys::XactEvent_XACT_EVENT_ABORT => {
            if let Err(err) = rollback_remote_transactions() {
                pgx::error!("Failed to rollback remote transactions: {:?}", err);
            }
        }
        pg_sys::XactEvent_XACT_EVENT_PRE_PREPARE | pg_sys::XactEvent_XACT_EVENT_PREPARE => {
            pgx::error!("Prepared transactions are not supported within ansilo")
        }
        _ => {}
    }
}

/// Commit all active remote transactions
fn commit_remote_transactions() -> Result<()> {
    let mut active = get_active_transactions()?;

    for id in active.keys().cloned().collect::<Vec<_>>() {
        let trans = active.get_mut(&id).unwrap();

        pgx::debug1!("Committing transaction on connection {}", id.clone());

        trans
            .con
            .send(ClientMessage::CommitTransaction)
            .and_then(|res| match res {
                ServerMessage::TransactionCommitted => Ok(()),
                _ => bail!("Unexpected response: {:?}", res),
            })
            .with_context(|| {
                format!(
                    "Committing remote transactions on connection {}",
                    id.clone()
                )
            })?;

        // We remove and drop each transaction after they are committed
        // This is important for the connection to ultimately be
        // dropped when it is no longer needed by the transaction
        // or any queries.
        active.remove(&id);

        pgx::debug1!("Committed transaction on connection {}", id.clone());
    }

    Ok(())
}

/// Rolls back all active remote transactions
fn rollback_remote_transactions() -> Result<()> {
    let mut active = get_active_transactions()?;

    for id in active.keys().cloned().collect::<Vec<_>>() {
        let trans = active.get_mut(&id).unwrap();

        pgx::debug1!("Rolling back transaction on connection {}", id.clone());

        trans
            .con
            .send(ClientMessage::RollbackTransaction)
            .and_then(|res| match res {
                ServerMessage::TransactionRolledBack => Ok(()),
                _ => bail!("Unexpected response: {:?}", res),
            })
            .with_context(|| {
                format!(
                    "Rolling back remote transactions on connection {}",
                    id.clone()
                )
            })?;

        // We remove and drop each transaction after they are rolled back
        // This is important for the connection to ultimately be
        // dropped when it is no longer needed by the transaction
        // or any queries.
        active.remove(&id);

        pgx::debug1!("Rolled back transaction on connection {}", id.clone());
    }

    Ok(())
}

/// Handles sub transaction events from postgres
///
/// Currently we disallow sub transactions.
unsafe extern "C" fn handle_sub_transaction_event(
    _event: SubXactEvent,
    _my_subid: SubTransactionId,
    _parent_subid: SubTransactionId,
    _arg: *mut ::std::os::raw::c_void,
) {
    pgx::error!("Sub-transactions are not supported within ansilo remote data sources.")
}

fn get_active_transactions<'a>(
) -> Result<std::sync::MutexGuard<'a, HashMap<String, RemoteTransaction>>, Error> {
    ACTIVE_TRANSACTIONS
        .lock()
        .map_err(|e| Error::msg(format!("Failed to lock active transactions: {:?}", e)))
}

/// PGX, SPI and transactions do not play well together.
/// For these tests we use a non-SPI postgres client to run
/// queries for testing purposes. This is a bit silly as the tests
/// are executed within postgres and then connect back to itself
/// but it maintains a consistent workflow with other tests.
#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::fdw::test::server::start_fdw_server;
    use ansilo_connectors_all::ConnectionPools;
    use ansilo_connectors_base::{
        common::entity::{ConnectorEntityConfig, EntitySource},
        interface::Connector,
    };
    use ansilo_connectors_memory::{
        MemoryConnector, MemoryConnectorEntitySourceConfig, MemoryDatabase,
    };
    use ansilo_core::{
        config::{EntityAttributeConfig, EntityConfig, EntitySourceConfig, NodeConfig},
        data::{DataType, DataValue},
    };

    fn create_memory_connection_pool() -> ConnectionPools {
        let conf = MemoryDatabase::new();
        let mut entities = ConnectorEntityConfig::new();

        entities.add(EntitySource::new(
            EntityConfig::minimal(
                "x",
                vec![EntityAttributeConfig::minimal("x", DataType::UInt32)],
                EntitySourceConfig::minimal(""),
            ),
            MemoryConnectorEntitySourceConfig::new(None),
        ));

        conf.set_data("x", vec![vec![DataValue::UInt32(1)]]);

        let pool = MemoryConnector::create_connection_pool(conf, &NodeConfig::default(), &entities)
            .unwrap();

        ConnectionPools::Memory(pool, entities)
    }

    fn setup_db<'a>(
        test_name: impl Into<String>,
        socket_path: impl Into<String>,
    ) -> (postgres::Client, std::sync::MutexGuard<'a, ()>) {
        lazy_static! {
            static ref LOCK: Mutex<()> = Mutex::new(());
        }

        let lock = LOCK.lock().unwrap();
        let test_name = test_name.into();
        let socket_path = socket_path.into();
        let (mut client, _) = pgx_tests::client();

        client
            .batch_execute(&format!(
                r#"
                CREATE SCHEMA IF NOT EXISTS {test_name}_tests;
                SET SCHEMA '{test_name}_tests';

                DROP SERVER IF EXISTS {test_name}_test_srv CASCADE;
                CREATE SERVER {test_name}_test_srv FOREIGN DATA WRAPPER ansilo_fdw OPTIONS (
                    socket '{socket_path}',
                    data_source 'memory'
                );

                CREATE FOREIGN TABLE "x" (
                    x BIGINT
                ) SERVER {test_name}_test_srv;
                "#
            ))
            .unwrap();

        (client, lock)
    }

    fn setup_test<'a>(
        test_name: impl Into<String>,
    ) -> (postgres::Client, std::sync::MutexGuard<'a, ()>) {
        let test_name = test_name.into();
        let sock_path = format!("/tmp/ansilo/fdw_server/{}", test_name.clone());
        start_fdw_server(create_memory_connection_pool(), sock_path.clone());
        setup_db::<'a>(test_name, sock_path)
    }

    #[pg_test]
    fn test_fdw_transaction_auto_commit() {
        let (mut client, _lock) = setup_test("transaction_auto_commit");

        client
            .batch_execute(
                r#"
            DO $$BEGIN
                ASSERT (SELECT x FROM "x") = 1;
            END$$;

            UPDATE "x" SET x = 123;

            DO $$BEGIN
                ASSERT (SELECT x FROM "x") = 123;
            END$$;
        "#,
            )
            .unwrap();
    }

    #[pg_test]
    fn test_fdw_transaction_begin_commit() {
        let (mut client, _lock) = setup_test("transaction_begin_commit");

        client
            .batch_execute(
                r#"
            BEGIN;

            UPDATE "x" SET x = 123;

            DO $$BEGIN
                ASSERT (SELECT x FROM "x") = 123;
            END$$;

            COMMIT;

            DO $$BEGIN
                ASSERT (SELECT x FROM "x") = 123;
            END$$;
            
            BEGIN;
        "#,
            )
            .unwrap();
    }

    #[pg_test]
    fn test_fdw_transaction_begin_rollback() {
        let (mut client, _lock) = setup_test("transaction_begin_rollback");

        client
            .batch_execute(
                r#"
            BEGIN;

            UPDATE "x" SET x = 123;

            DO $$BEGIN
                ASSERT (SELECT x FROM "x") = 123;
            END$$;

            ROLLBACK;

            DO $$BEGIN
                ASSERT (SELECT x FROM "x") = 1;
            END$$;
        "#,
            )
            .unwrap();
    }
}

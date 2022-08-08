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
use ansilo_pg::fdw::{
    channel::IpcClientChannel,
    proto::{ClientMessage, ServerMessage},
};

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
unsafe extern "C" fn handle_transaction_event(event: XactEvent, arg: *mut c_void) {
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
    event: SubXactEvent,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
    arg: *mut ::std::os::raw::c_void,
) {
    pgx::error!("Sub-transactions are not supported within ansilo remote data sources.")
}

fn get_active_transactions<'a>(
) -> Result<std::sync::MutexGuard<'a, HashMap<String, RemoteTransaction>>, Error> {
    ACTIVE_TRANSACTIONS
        .lock()
        .map_err(|e| Error::msg(format!("Failed to lock active transactions: {:?}", e)))
}

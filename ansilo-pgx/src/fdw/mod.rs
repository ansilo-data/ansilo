/// The implementation of the FDW for connecting to external data sources through Ansilo
/// We have a generic FDW implementation that abstracts across all our sources
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c_source.html
/// For the reference postgres_fdw implementation
#[allow(unused)]
mod analyze;
#[allow(unused)]
mod explain;
#[allow(unused)]
mod r#async;
#[allow(unused)]
mod import;
#[allow(unused)]
mod modify;
#[allow(unused)]
mod scan;

pub mod common;
pub mod ctx;

#[cfg(any(test, feature = "pg_test"))]
pub mod test;

use scan::*;
use analyze::*;
use explain::*;
use r#async::*;
use import::*;
use modify::*;


use pgx::{
    pg_sys::{self, FdwRoutine},
    *,
};

/// The fdw handler initialisation function
/// @see https://www.postgresql.org/docs/14/fdw-functions.html
#[pg_extern]
fn ansilo_fdw_handler() -> pg_sys::Datum {
    // Initialise the FDW routine struct with memory allocated by rust
    let mut handler = PgBox::<FdwRoutine>::alloc_node(pg_sys::NodeTag_T_FdwRoutine);

    // Assign pointers to our FDW handler functions
    handler.GetForeignRelSize = Some(self::get_foreign_rel_size);
    handler.GetForeignPaths = Some(self::get_foreign_paths);
    handler.GetForeignPlan = Some(self::get_foreign_plan);
    handler.BeginForeignScan = Some(self::begin_foreign_scan);
    handler.IterateForeignScan = Some(self::iterate_foreign_scan);
    handler.ReScanForeignScan = Some(self::re_scan_foreign_scan);
    handler.EndForeignScan = Some(self::end_foreign_scan);
    handler.GetForeignJoinPaths = Some(self::get_foreign_join_paths);
    handler.GetForeignUpperPaths = Some(self::get_foreign_upper_paths);
    handler.AddForeignUpdateTargets = None; // Some(self::add_foreign_update_targets);
    handler.PlanForeignModify = None; // Some(self::plan_foreign_modify);
    handler.BeginForeignModify = None; // Some(self::begin_foreign_modify);
    handler.ExecForeignInsert = None; // Some(self::exec_foreign_insert);
    handler.ExecForeignBatchInsert = None; // Some(self::exec_foreign_batch_insert);
    handler.GetForeignModifyBatchSize = None; // Some(self::get_foreign_modify_batch_size);
    handler.ExecForeignUpdate = None; // Some(self::exec_foreign_update);
    handler.ExecForeignDelete = None; // Some(self::exec_foreign_delete);
    handler.EndForeignModify = None; // Some(self::end_foreign_modify);
    handler.BeginForeignInsert = None; // Some(self::begin_foreign_insert);
    handler.EndForeignInsert = None; // Some(self::end_foreign_insert);
    handler.IsForeignRelUpdatable = None; // Some(self::is_foreign_rel_updatable);
    handler.PlanDirectModify = None; // Some(self::plan_direct_modify);
    handler.BeginDirectModify = None; // Some(self::begin_direct_modify);
    handler.IterateDirectModify = None; // Some(self::iterate_direct_modify);
    handler.EndDirectModify = None; // Some(self::end_direct_modify);
    handler.GetForeignRowMarkType = None; // Some(self::get_foreign_row_mark_type);
    handler.RefetchForeignRow = None; // Some(self::refetch_foreign_row);
    handler.RecheckForeignScan = Some(self::recheck_foreign_scan);
    handler.ExplainForeignScan = Some(self::explain_foreign_scan);
    handler.ExplainForeignModify = None; // Some(self::explain_foreign_modify);
    handler.ExplainDirectModify = None; // Some(self::explain_direct_modify);
    handler.AnalyzeForeignTable = None; // Some(self::analyze_foreign_table);
    handler.ImportForeignSchema = None; // Some(self::import_foreign_schema);
    handler.ExecForeignTruncate = None; // Some(self::exec_foreign_truncate);
    handler.IsForeignScanParallelSafe = None; // Some(self::is_foreign_scan_parallel_safe);
    handler.EstimateDSMForeignScan = None; // Some(self::estimate_dsm_foreign_scan);
    handler.InitializeDSMForeignScan = None; // Some(self::initialize_dsm_foreign_scan);
    handler.ReInitializeDSMForeignScan = None; // Some(self::re_initialize_dsm_foreign_scan);
    handler.InitializeWorkerForeignScan = None; // Some(self::initialize_worker_foreign_scan);
    handler.ShutdownForeignScan = None; // Some(self::shutdown_foreign_scan);
    handler.ReparameterizeForeignPathByChild = None; // Some(self::reparameterize_foreign_path_by_child);
    handler.IsForeignPathAsyncCapable = None; // Some(self::is_foreign_path_async_capable);
    handler.ForeignAsyncRequest = None; // Some(self::foreign_async_request);
    handler.ForeignAsyncConfigureWait = None; // Some(self::foreign_async_configure_wait);
    handler.ForeignAsyncNotify = None; // Some(self::foreign_async_notify);

    // Convert the ownership to postgres, so it is not dropped by rust
    handler.into_pg_boxed().into_datum().unwrap()
}

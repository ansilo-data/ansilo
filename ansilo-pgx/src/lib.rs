#[allow(unused)]
mod fdw;

use pgx::{*, pg_sys::{self, FdwRoutine}};

pg_module_magic!();

extension_sql!(
    r#"
        -- Register our FDW
        CREATE FUNCTION "ansilo_fdw_handler_typed"() RETURNS fdw_handler STRICT LANGUAGE c /* Rust */ AS 'MODULE_PATHNAME', 'ansilo_fdw_handler_wrapper';
        CREATE FOREIGN DATA WRAPPER ansilo_fdw HANDLER ansilo_fdw_handler_typed;
        -- Create the server
        CREATE SERVER IF NOT EXISTS ansilo_srv FOREIGN DATA WRAPPER ansilo_fdw;
        -- Import the foreign schema (TODO)
        -- IMPORT FOREIGN SCHEMA _ FROM SERVER ansilo_srv INTO public;
"#,
    name="ansilo_fdw"
);

/// This can be used to sense check the extension is running
/// ```sql
/// SELECT hello_ansilo();
/// ```
#[pg_extern]
fn hello_ansilo() -> &'static str {
    "Hello from Ansilo"
}

/// The fdw handler initialisation function
/// @see https://www.postgresql.org/docs/14/fdw-functions.html
#[pg_extern]
fn ansilo_fdw_handler() -> pg_sys::Datum {
    // Initialise the FDW routine struct with memory allocated by rust
    let mut handler = PgBox::<FdwRoutine>::alloc_node(pg_sys::NodeTag_T_FdwRoutine);

    // Assign pointers to our FDW handler functions
    handler.GetForeignRelSize = Some(fdw::get_foreign_rel_size);
    handler.GetForeignPaths = Some(fdw::get_foreign_paths);
    handler.GetForeignPlan = Some(fdw::get_foreign_plan);
    handler.BeginForeignScan = Some(fdw::begin_foreign_scan);
    handler.IterateForeignScan = Some(fdw::iterate_foreign_scan);
    handler.ReScanForeignScan = Some(fdw::re_scan_foreign_scan);
    handler.EndForeignScan = Some(fdw::end_foreign_scan);
    handler.GetForeignJoinPaths = Some(fdw::get_foreign_join_paths);
    handler.GetForeignUpperPaths = Some(fdw::get_foreign_upper_paths);
    handler.AddForeignUpdateTargets = None; // Some(fdw::add_foreign_update_targets);
    handler.PlanForeignModify = None; // Some(fdw::plan_foreign_modify);
    handler.BeginForeignModify = None; // Some(fdw::begin_foreign_modify);
    handler.ExecForeignInsert = None; // Some(fdw::exec_foreign_insert);
    handler.ExecForeignBatchInsert = None; // Some(fdw::exec_foreign_batch_insert);
    handler.GetForeignModifyBatchSize = None; // Some(fdw::get_foreign_modify_batch_size);
    handler.ExecForeignUpdate = None; // Some(fdw::exec_foreign_update);
    handler.ExecForeignDelete = None; // Some(fdw::exec_foreign_delete);
    handler.EndForeignModify = None; // Some(fdw::end_foreign_modify);
    handler.BeginForeignInsert = None; // Some(fdw::begin_foreign_insert);
    handler.EndForeignInsert = None; // Some(fdw::end_foreign_insert);
    handler.IsForeignRelUpdatable = None; // Some(fdw::is_foreign_rel_updatable);
    handler.PlanDirectModify = None; // Some(fdw::plan_direct_modify);
    handler.BeginDirectModify = None; // Some(fdw::begin_direct_modify);
    handler.IterateDirectModify = None; // Some(fdw::iterate_direct_modify);
    handler.EndDirectModify = None; // Some(fdw::end_direct_modify);
    handler.GetForeignRowMarkType = None; // Some(fdw::get_foreign_row_mark_type);
    handler.RefetchForeignRow = None; // Some(fdw::refetch_foreign_row);
    handler.RecheckForeignScan = None; // Some(fdw::recheck_foreign_scan);
    handler.ExplainForeignScan = None; // Some(fdw::explain_foreign_scan);
    handler.ExplainForeignModify = None; // Some(fdw::explain_foreign_modify);
    handler.ExplainDirectModify = None; // Some(fdw::explain_direct_modify);
    handler.AnalyzeForeignTable = None; // Some(fdw::analyze_foreign_table);
    handler.ImportForeignSchema = None; // Some(fdw::import_foreign_schema);
    handler.ExecForeignTruncate = None; // Some(fdw::exec_foreign_truncate);
    handler.IsForeignScanParallelSafe = None; // Some(fdw::is_foreign_scan_parallel_safe);
    handler.EstimateDSMForeignScan = None; // Some(fdw::estimate_dsm_foreign_scan);
    handler.InitializeDSMForeignScan = None; // Some(fdw::initialize_dsm_foreign_scan);
    handler.ReInitializeDSMForeignScan = None; // Some(fdw::re_initialize_dsm_foreign_scan);
    handler.InitializeWorkerForeignScan = None; // Some(fdw::initialize_worker_foreign_scan);
    handler.ShutdownForeignScan = None; // Some(fdw::shutdown_foreign_scan);
    handler.ReparameterizeForeignPathByChild = None; // Some(fdw::reparameterize_foreign_path_by_child);
    handler.IsForeignPathAsyncCapable = None; // Some(fdw::is_foreign_path_async_capable);
    handler.ForeignAsyncRequest = None; // Some(fdw::foreign_async_request);
    handler.ForeignAsyncConfigureWait = None; // Some(fdw::foreign_async_configure_wait);
    handler.ForeignAsyncNotify = None; // Some(fdw::foreign_async_notify);

    // Convert the ownership to postgres, so it is not dropped by rust
    handler.into_pg_boxed().into_datum().unwrap()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_hello_ansilopg() {
        // GetForeignRelSize();
        assert_eq!("Hello, ansilopg", crate::hello_ansilo());
    }

}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
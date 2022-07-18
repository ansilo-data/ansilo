use pgx::{
    pg_sys::{
        self, FdwRoutine, ForeignPath, ForeignScan, List, Oid, Plan, PlannerInfo, RelOptInfo,
    },
    *,
};

extension_sql!(
    r#"
        -- Register our FDW
        CREATE FUNCTION "null_fdw_handler_typed"() RETURNS fdw_handler STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'null_fdw_handler_wrapper';
        CREATE FOREIGN DATA WRAPPER null_fdw HANDLER null_fdw_handler_typed;
"#,
    name = "null_fdw"
);

/// In testing mode we create a "null_fdw" that returns no data
/// This is useful for some tests
#[pg_extern]
fn null_fdw_handler() -> pg_sys::Datum {
    // Initialise the FDW routine struct with memory allocated by rust
    let mut handler = PgBox::<FdwRoutine>::alloc_node(pg_sys::NodeTag_T_FdwRoutine);

    handler.GetForeignRelSize = Some(get_foreign_rel_size);
    handler.GetForeignPaths = Some(get_foreign_paths);
    handler.GetForeignPlan = Some(get_foreign_plan);

    // Convert the ownership to postgres, so it is not dropped by rust
    handler.into_pg_boxed().into_datum().unwrap()
}

unsafe extern "C" fn get_foreign_rel_size(
    _root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    _foreigntableid: Oid,
) {
    (*baserel).rows = 0.0;
}

unsafe extern "C" fn get_foreign_paths(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    _foreigntableid: Oid,
) {
    pg_sys::add_path(
        baserel,
        pg_sys::create_foreignscan_path(
            root,
            baserel,
            std::ptr::null_mut(),
            (*baserel).rows,
            pg_sys::Cost::from(10),
            pg_sys::Cost::from(0),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        ) as *mut pg_sys::Path,
    )
}

unsafe extern "C" fn get_foreign_plan(
    _root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    _foreigntableid: Oid,
    _best_path: *mut ForeignPath,
    tlist: *mut List,
    scan_clauses: *mut List,
    outer_plan: *mut Plan,
) -> *mut ForeignScan {
    let scan_relid = (*baserel).relid;
    let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

    pg_sys::make_foreignscan(
        tlist,
        scan_clauses,
        scan_relid,
        scan_clauses,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        outer_plan,
    )
}

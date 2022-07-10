use pgx::pg_sys::*;

pub unsafe extern "C" fn get_foreign_rel_size(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    unimplemented!()
}

pub unsafe extern "C" fn get_foreign_paths(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    unimplemented!()
}

pub unsafe extern "C" fn get_foreign_plan(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
    best_path: *mut ForeignPath,
    tlist: *mut List,
    scan_clauses: *mut List,
    outer_plan: *mut Plan,
) -> *mut ForeignScan {
    unimplemented!()
}

pub unsafe extern "C" fn begin_foreign_scan(node: *mut ForeignScanState, eflags: ::std::os::raw::c_int) {
    unimplemented!()
}

pub unsafe extern "C" fn iterate_foreign_scan(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    unimplemented!()
}

pub unsafe extern "C" fn recheck_foreign_scan(
    node: *mut ForeignScanState,
    slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub unsafe extern "C" fn re_scan_foreign_scan(node: *mut ForeignScanState) {
    unimplemented!()
}

pub unsafe extern "C" fn end_foreign_scan(node: *mut ForeignScanState) {
    unimplemented!()
}

pub unsafe extern "C" fn get_foreign_join_paths(
    root: *mut PlannerInfo,
    joinrel: *mut RelOptInfo,
    outerrel: *mut RelOptInfo,
    innerrel: *mut RelOptInfo,
    jointype: JoinType,
    extra: *mut JoinPathExtraData,
) {
    unimplemented!()
}

pub unsafe extern "C" fn get_foreign_upper_paths(
    root: *mut PlannerInfo,
    stage: UpperRelationKind,
    input_rel: *mut RelOptInfo,
    output_rel: *mut RelOptInfo,
    extra: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

pub unsafe extern "C" fn explain_foreign_scan(node: *mut ForeignScanState, es: *mut ExplainState) {
    unimplemented!()
}

pub unsafe extern "C" fn estimate_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
) -> Size {
    unimplemented!()
}

pub unsafe extern "C" fn initialize_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

pub unsafe extern "C" fn re_initialize_dsm_foreign_scan(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

pub unsafe extern "C" fn initialize_worker_foreign_scan(
    node: *mut ForeignScanState,
    toc: *mut shm_toc,
    coordinate: *mut ::std::os::raw::c_void,
) {
    unimplemented!()
}

pub unsafe extern "C" fn shutdown_foreign_scan(node: *mut ForeignScanState) {
    unimplemented!()
}

pub unsafe extern "C" fn is_foreign_scan_parallel_safe(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rte: *mut RangeTblEntry,
) -> bool {
    unimplemented!()
}

pub unsafe extern "C" fn reparameterize_foreign_path_by_child(
    root: *mut PlannerInfo,
    fdw_private: *mut List,
    child_rel: *mut RelOptInfo,
) -> *mut List {
    unimplemented!()
}

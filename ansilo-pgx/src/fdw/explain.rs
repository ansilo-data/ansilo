use pgx::*;
use pgx::pg_sys::{ExplainState, ModifyTableState, ResultRelInfo, ForeignScanState, List};

#[pg_guard]
pub unsafe extern "C" fn explain_foreign_scan(node: *mut ForeignScanState, es: *mut ExplainState) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn explain_foreign_modify(
    mtstate: *mut ModifyTableState,
    rinfo: *mut ResultRelInfo,
    fdw_private: *mut List,
    subplan_index: ::std::os::raw::c_int,
    es: *mut ExplainState,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn explain_direct_modify(node: *mut ForeignScanState, es: *mut ExplainState) {
    unimplemented!()
}

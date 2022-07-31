use pgx::{
    pg_sys::{
        DropBehavior, EState, ExecRowMark, ForeignScanState, Index, List, LockClauseStrength,
        ModifyTable, ModifyTableState, PlannerInfo, RangeTblEntry, Relation, ResultRelInfo,
        RowMarkType, TupleTableSlot,
    },
    *,
};

#[pg_guard]
pub unsafe extern "C" fn add_foreign_update_targets(
    root: *mut PlannerInfo,
    rtindex: Index,
    target_rte: *mut RangeTblEntry,
    target_relation: Relation,
) {
    let var = pg_sys::makeVar(
        rtindex,
        pg_sys::SelfItemPointerAttributeNumber as _,
        pg_sys::TIDOID,
        -1,
        pg_sys::InvalidOid,
        0,
    );

    pg_sys::add_row_identity_var(root, var, rtindex, cstr::cstr!("ctid").as_ptr());
}

#[pg_guard]
pub unsafe extern "C" fn is_foreign_rel_updatable(rel: Relation) -> ::std::os::raw::c_int {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn plan_foreign_modify(
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    subplan_index: ::std::os::raw::c_int,
) -> *mut List {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn begin_foreign_modify(
    mtstate: *mut ModifyTableState,
    rinfo: *mut ResultRelInfo,
    fdw_private: *mut List,
    subplan_index: ::std::os::raw::c_int,
    eflags: ::std::os::raw::c_int,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_insert(
    estate: *mut EState,
    rinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    plan_slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_batch_insert(
    estate: *mut EState,
    rinfo: *mut ResultRelInfo,
    slots: *mut *mut TupleTableSlot,
    plan_slots: *mut *mut TupleTableSlot,
    num_slots: *mut ::std::os::raw::c_int,
) -> *mut *mut TupleTableSlot {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn get_foreign_modify_batch_size(
    rinfo: *mut ResultRelInfo,
) -> ::std::os::raw::c_int {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_update(
    estate: *mut EState,
    rinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    plan_slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_delete(
    estate: *mut EState,
    rinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    plan_slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn end_foreign_modify(estate: *mut EState, rinfo: *mut ResultRelInfo) {
    unimplemented!()
}
#[pg_guard]
pub unsafe extern "C" fn begin_foreign_insert(
    mtstate: *mut ModifyTableState,
    rinfo: *mut ResultRelInfo,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn end_foreign_insert(estate: *mut EState, rinfo: *mut ResultRelInfo) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn plan_direct_modify(
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    subplan_index: ::std::os::raw::c_int,
) -> bool {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn begin_direct_modify(
    node: *mut ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn iterate_direct_modify(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn end_direct_modify(node: *mut ForeignScanState) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn get_foreign_row_mark_type(
    rte: *mut RangeTblEntry,
    strength: LockClauseStrength,
) -> RowMarkType {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn refetch_foreign_row(
    estate: *mut EState,
    erm: *mut ExecRowMark,
    rowid: Datum,
    slot: *mut TupleTableSlot,
    updated: *mut bool,
) {
    unimplemented!()
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_truncate(
    rels: *mut List,
    behavior: DropBehavior,
    restart_seqs: bool,
) {
    unimplemented!()
}

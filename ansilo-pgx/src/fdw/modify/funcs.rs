use ansilo_core::{data::DataValue, sqlil};
use ansilo_pg::fdw::proto::*;
use pgx::{
    pg_sys::{
        DropBehavior, EState, ExecRowMark, ForeignScanState, Index, List, LockClauseStrength,
        ModifyTable, ModifyTableState, PlannerInfo, RangeTblEntry, Relation, ResultRelInfo,
        RowMarkType, TupleTableSlot,
    },
    *,
};

use crate::{
    fdw::{
        common,
        ctx::{
            from_fdw_private_modify, into_fdw_private_modify, FdwContext, FdwModifyContext,
            FdwQueryContext, FdwQueryType,
        },
    },
    sqlil::{from_datum, from_pg_type},
    util::{table::PgTable, tuple::slot_get_attr},
};

#[pg_guard]
pub unsafe extern "C" fn add_foreign_update_targets(
    root: *mut PlannerInfo,
    rtindex: Index,
    target_rte: *mut RangeTblEntry,
    target_relation: Relation,
) {
    // Add a var for with varattno as SelfItemPointerAttributeNumber to the tlist
    // This is picked up in get_foreign_plan and mapped to row ID expressions from the data source
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
    // TODO: Determine from data source
    (1 << pg_sys::CmdType_CMD_INSERT)
        | (1 << pg_sys::CmdType_CMD_UPDATE)
        | (1 << pg_sys::CmdType_CMD_DELETE)
}

#[pg_guard]
pub unsafe extern "C" fn plan_foreign_modify(
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    subplan_index: ::std::os::raw::c_int,
) -> *mut List {
    let rte = pg_sys::planner_rt_fetch(result_relation, root);

    let table = PgTable::open((*rte).relid as _, pg_sys::NoLock as _).unwrap();

    // Currently we do not support WITH CHECK OPTION
    if !(*plan).withCheckOptionLists.is_null() {
        panic!("WITH CHECK OPTION is currently not supported");
    }

    if !(*plan).returningLists.is_null() {
        panic!("RETURNING clauses are currently not supported");
    }

    if (*plan).onConflictAction != pg_sys::OnConflictAction_ONCONFLICT_NONE {
        panic!("ON CONFLICT clause is currently not supported");
    }

    // TODO: See how we can avoid having multipe connections to ansilo within the same plan tree
    // This will be vital once we start dealing with foreign locking or transactions
    let mut ctx = common::connect(table.rd_id);

    let query = match (*plan).operation {
        pg_sys::CmdType_CMD_INSERT => plan_foreign_insert(&mut ctx, root, plan, table),
        // pg_sys::CmdType_CMD_UPDATE => plan_foreign_update(ctx, root, plan, table),
        // pg_sys::CmdType_CMD_DELETE => plan_foreign_delete(ctx, root, plan, table),
        _ => panic!("Unexpected operation: {}", (*plan).operation),
    };

    into_fdw_private_modify(ctx, query, FdwModifyContext::new())
}

fn plan_foreign_insert(
    ctx: &mut PgBox<FdwContext>,
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    table: PgTable,
) -> FdwQueryContext {
    let mut query = FdwQueryContext::insert(table.rd_id);

    // Create an insert query to insert a single row
    ctx.create_query(
        query.cvt.register_alias(table.relid()),
        sqlil::QueryType::Insert,
    )
    .unwrap();

    // Add a parameter for each column
    for att in table.attrs() {
        let col_name = att.name().to_string();
        let att_type = att.atttypid;
        let data_type = from_pg_type(att_type as _).unwrap();
        let param = sqlil::Parameter::new(data_type, query.cvt.create_param());

        let op = InsertQueryOperation::AddColumn((col_name, sqlil::Expr::Parameter(param.clone())));

        match ctx.apply_query_op(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                panic!("Failed to create insert query on data source: unsupported")
            }
        }

        let insert = query.as_insert_mut().unwrap();
        insert.remote_ops.push(op);
        insert.params.push((param, att_type));
    }

    query
}

#[pg_guard]
pub unsafe extern "C" fn begin_foreign_modify(
    mtstate: *mut ModifyTableState,
    rinfo: *mut ResultRelInfo,
    fdw_private: *mut List,
    subplan_index: ::std::os::raw::c_int,
    eflags: ::std::os::raw::c_int,
) {
    let (mut ctx, _query, _state) = from_fdw_private_modify(fdw_private);

    ctx.prepare_query().unwrap();

    (*rinfo).ri_FdwState = fdw_private as *mut _;
}

#[pg_guard]
pub unsafe extern "C" fn get_foreign_modify_batch_size(
    rinfo: *mut ResultRelInfo,
) -> ::std::os::raw::c_int {
    // TODO: Determine from data source
    return 1;
}

#[pg_guard]
pub unsafe extern "C" fn begin_foreign_insert(
    mtstate: *mut ModifyTableState,
    rinfo: *mut ResultRelInfo,
) {
    // not used as initialisation occurs in begin_foreign_modify
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_insert(
    estate: *mut EState,
    rinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    plan_slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let (mut ctx, query, _state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);
    let insert = query.as_insert().unwrap();
    let mut query_input = vec![];

    for (idx, (param, type_oid)) in insert.params.iter().enumerate() {
        let (is_null, datum) = slot_get_attr(slot, idx);

        if is_null {
            query_input.push(DataValue::Null);
        } else {
            let data_val = from_datum(*type_oid, datum).unwrap();
            debug_assert_eq!(&data_val.r#type(), &param.r#type);

            query_input.push(data_val);
        }
    }

    ctx.write_query_input(query_input).unwrap();
    ctx.execute_query().unwrap();
    ctx.restart_query().unwrap();

    slot
}

#[pg_guard]
pub unsafe extern "C" fn end_foreign_insert(estate: *mut EState, rinfo: *mut ResultRelInfo) {
    // not used as clean up occurs in end_foreign_modify
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_batch_insert(
    estate: *mut EState,
    rinfo: *mut ResultRelInfo,
    slots: *mut *mut TupleTableSlot,
    plan_slots: *mut *mut TupleTableSlot,
    num_slots: *mut ::std::os::raw::c_int,
) -> *mut *mut TupleTableSlot {
    // TODO:
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
    if (*rinfo).ri_FdwState.is_null() {
        return;
    }

    let (mut ctx, _query, _state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);

    ctx.disconnect().unwrap();
}

#[pg_guard]
pub unsafe extern "C" fn plan_direct_modify(
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    subplan_index: ::std::os::raw::c_int,
) -> bool {
    // TODO
    return false;
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

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
    sqlil::{from_datum, from_pg_type, into_pg_type},
    util::{table::PgTable, tuple::slot_get_attr},
};

#[pg_guard]
pub unsafe extern "C" fn add_foreign_update_targets(
    root: *mut PlannerInfo,
    rtindex: Index,
    target_rte: *mut RangeTblEntry,
    target_relation: Relation,
) {
    // Noop here. This is handled in get_foriegn_plan for ForeignScan
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
        pg_sys::CmdType_CMD_UPDATE => plan_foreign_update(&mut ctx, root, plan, rte, table),
        // pg_sys::CmdType_CMD_DELETE => plan_foreign_delete(&mut ctx, root, plan, rte, table),
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
    ctx.create_query(query.base_rel_alias(), sqlil::QueryType::Insert)
        .unwrap();

    // Add a parameter for each column
    for att in table.attrs() {
        let (col_name, att_type, param) = create_param_for_col(att, &mut query);

        let op = InsertQueryOperation::AddColumn((col_name, sqlil::Expr::Parameter(param.clone())));

        match ctx.apply_query_op(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                panic!("Failed to create insert query on data source: unable to add query parameter for insert value")
            }
        }

        let insert = query.as_insert_mut().unwrap();
        insert.remote_ops.push(op);
        insert.params.push((param, att_type));
    }

    query
}

unsafe fn plan_foreign_update(
    ctx: &mut PgBox<FdwContext>,
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    rte: *mut RangeTblEntry,
    table: PgTable,
) -> FdwQueryContext {
    let mut query = FdwQueryContext::update(table.rd_id);

    // Create an insert query to insert a single row
    ctx.create_query(query.base_rel_alias(), sqlil::QueryType::Update)
        .unwrap();

    // Determine the columns which are updated by the query
    let updated_cols = if !table.trigdesc.is_null() && (*table.trigdesc).trig_update_before_row {
        // If the target table has a BEFORE UPDATE trigger we have to include all columns
        // as the trigger may change columns not specified in the query itself.
        table.attrs().collect::<Vec<_>>()
    } else {
        // Otherwise we use the columns specified from the query itself
        let cols = pg_sys::bms_union((*rte).updatedCols, (*rte).extraUpdatedCols);

        table
            .attrs()
            .filter(|col| {
                // From pg src:
                // updatedCols are bitmapsets, which cannot have negative integer members,
                // so we subtract FirstLowInvalidHeapAttributeNumber from column
                // numbers before storing them in these fields.
                // @see https://doxygen.postgresql.org/parsenodes_8h_source.html#l01180
                pg_sys::bms_is_member(
                    col.attnum as i32 - pg_sys::FirstLowInvalidHeapAttributeNumber,
                    cols,
                )
            })
            .collect()
    };

    // Add a parameter for each column to update
    for att in updated_cols.into_iter() {
        let (col_name, att_type, param) = create_param_for_col(att, &mut query);

        let op = UpdateQueryOperation::AddSet((col_name, sqlil::Expr::Parameter(param.clone())));

        match ctx.apply_query_op(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                panic!("Failed to create update query on data source: unable to add query parameter for update value")
            }
        }

        let update = query.as_update_mut().unwrap();
        update.remote_ops.push(op);
        update.update_params.push((param, att_type));
    }

    // Add a conditions to filter the row to by the row id
    let row_id_exprs = ctx.get_row_id_exprs(query.base_rel_alias()).unwrap();
    for (expr, r#type) in row_id_exprs.into_iter() {
        let param = query.create_param(r#type.clone());
        let op = UpdateQueryOperation::AddWhere(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
            expr,
            sqlil::BinaryOpType::Equal,
            sqlil::Expr::Parameter(param.clone()),
        )));

        match ctx.apply_query_op(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                panic!("Failed to create update query on data source: unable to add query parameter for row id condition")
            }
        }

        let update = query.as_update_mut().unwrap();
        update.remote_ops.push(op);
        update
            .rowid_params
            .push((param, into_pg_type(&r#type).unwrap()))
    }

    query
}

fn create_param_for_col(
    att: &pg_sys::FormData_pg_attribute,
    query: &mut FdwQueryContext,
) -> (String, u32, sqlil::Parameter) {
    let col_name = att.name().to_string();
    let att_type = att.atttypid;
    let data_type = from_pg_type(att_type as _).unwrap();
    let param = query.create_param(data_type);

    (col_name, att_type, param)
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

    // (*rinfo).ri

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
            query_input.push((param.id, DataValue::Null));
        } else {
            let data_val = from_datum(*type_oid, datum).unwrap();
            debug_assert_eq!(&data_val.r#type(), &param.r#type);

            query_input.push((param.id, data_val));
        }
    }

    ctx.write_query_input_unordered(query_input).unwrap();
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
    let (mut ctx, query, _state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);
    let update = query.as_update().unwrap();
    let mut query_input = vec![];

    // We assume the rowid's are the first
    // as we ensure this is the case in get_foreign_plan
    let all_params = update
        .rowid_params
        .iter()
        .chain(update.update_params.iter());
    
    for (idx, (param, type_oid)) in all_params.enumerate() {
        let (is_null, datum) = slot_get_attr(slot, idx);

        if is_null {
            query_input.push((param.id, DataValue::Null));
        } else {
            let data_val = from_datum(*type_oid, datum)
                .unwrap()
                .try_coerce_into(&param.r#type)
                .unwrap();

            query_input.push((param.id, data_val));
        }
    }

    ctx.write_query_input_unordered(query_input).unwrap();
    ctx.execute_query().unwrap();
    ctx.restart_query().unwrap();

    slot
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

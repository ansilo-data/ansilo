use ansilo_core::{
    data::{DataType, DataValue},
    sqlil,
};
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
    util::{string::to_pg_cstr, table::PgTable, tuple::slot_get_attr},
};

#[pg_guard]
pub unsafe extern "C" fn add_foreign_update_targets(
    root: *mut PlannerInfo,
    rtindex: Index,
    target_rte: *mut RangeTblEntry,
    target_relation: Relation,
) {
    // TODO: See how we can avoid having multiple connections to ansilo within the same plan tree
    // This will be vital once we start dealing with foreign locking or transactions
    let mut ctx = common::connect((*target_relation).rd_id);

    let row_ids = match ctx.get_row_id_exprs("unused") {
        Ok(r) => r,
        Err(err) => panic!("Failed to get row ID's for table: {err}"),
    };

    for (idx, (expr, r#type)) in row_ids.into_iter().enumerate() {
        let col = pg_sys::makeVar(
            rtindex,
            pg_sys::SelfItemPointerAttributeNumber as _,
            into_pg_type(&r#type).unwrap(),
            -1,
            pg_sys::InvalidOid,
            0,
        );

        // HACK: we could have multiple row id vars using the same varattno
        // We want to keep these distinct so we need a way to disambiguate them
        // We use the location attribute to reference which row id var this is.
        (*col).location = idx as _;

        // Append each rowid as a resjunk tle
        // We give each rowid a unique name in the format below so
        // that when planning our table modification, it can find
        // the appropriate tle's in the subplan output tlist
        let res_name = to_pg_cstr(&format!("ctid_ansilo_{idx}")).unwrap();

        // Register it as a row-identity column needed by this rel
        pg_sys::add_row_identity_var(root, col, rtindex, res_name as *const _);
    }

    ctx.disconnect().unwrap();
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

    // if !(*plan).returningLists.is_null() {
    //     panic!("RETURNING clauses are currently not supported");
    // }

    if (*plan).onConflictAction != pg_sys::OnConflictAction_ONCONFLICT_NONE {
        panic!("ON CONFLICT clause is currently not supported");
    }

    // TODO: See how we can avoid having multiple connections to ansilo within the same plan tree
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
    // let subplan = (plan *mut pg_sys::PlanState

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
        update
            .update_params
            .push((param, att.attnum as _, att_type));
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

        // We cannot determine the attr no at this stage in planning as
        // it dependent on the output subplan tlist.
        // We put a placeholder for now and defer until begin_foreign_modify
        let attnum = 0;

        let update = query.as_update_mut().unwrap();
        update.remote_ops.push(op);
        update
            .rowid_params
            .push((param, attnum, into_pg_type(&r#type).unwrap()))
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
    let (mut ctx, mut query, _state) = from_fdw_private_modify(fdw_private);

    // If this is an UPDATE/DELETE query we need to find the attr no's for the row id's
    // from the subplan tlist
    let row_id_params = match &mut query.q {
        FdwQueryType::Update(q) => Some(&mut q.rowid_params),
        FdwQueryType::Delete(q) => todo!(),
        _ => None,
    };

    if let Some(row_id_params) = row_id_params {
        // Here we find the attr no's of the row id's from the subplan
        // This should be output in the tlist with names using the format below.
        // @see get_foreign_plan function.
        let subplan = (*(*(mtstate as *mut pg_sys::PlanState)).lefttree).plan;

        for (idx, (param, attnum, r#type)) in row_id_params.iter_mut().enumerate() {
            let num = pg_sys::ExecFindJunkAttributeInTlist(
                (*subplan).targetlist,
                to_pg_cstr(&format!("ctid_ansilo_{idx}")).unwrap(),
            );

            if num == pg_sys::InvalidAttrNumber as i16 {
                panic!("Unable to find row id #{} entry in subplan tlist", idx + 1)
            }

            *attnum = num as _;
        }
    }

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
        query_input.push((
            param.id,
            slot_datum_into_data_val(slot, idx, *type_oid, &param.r#type),
        ));
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

    // We first bind the parameters for updating the row columns
    for (param, att_num, type_oid) in update.update_params.iter() {
        query_input.push((
            param.id,
            slot_datum_into_data_val(slot, (att_num - 1) as _, *type_oid, &param.r#type),
        ));
    }

    // Then bind the row id parameters (rowid's are stored as resjunk in the plan slot)
    for (param, att_num, type_oid) in update.rowid_params.iter() {
        query_input.push((
            param.id,
            slot_datum_into_data_val(plan_slot, (att_num - 1) as _, *type_oid, &param.r#type),
        ));
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

/// Retrieves a datum from a slot and converts it to the requested
/// type for a query parameter
unsafe fn slot_datum_into_data_val(
    slot: *mut TupleTableSlot,
    att_idx: usize,
    type_oid: pg_sys::Oid,
    param_type: &DataType,
) -> DataValue {
    let (is_null, datum) = slot_get_attr(slot, att_idx);

    if is_null {
        DataValue::Null
    } else {
        let data_val = from_datum(type_oid, datum)
            .unwrap()
            .try_coerce_into(param_type)
            .unwrap();

        data_val
    }
}

use std::{cmp, os::raw::c_int, ptr};

use ansilo_core::{
    data::{DataType, DataValue},
    err::{bail, Result},
    sqlil,
};
use ansilo_pg::fdw::proto::*;
use pgx::{
    pg_sys::{
        DropBehavior, EState, ExecRowMark, ForeignScan, ForeignScanState, Index, List,
        LockClauseStrength, ModifyTable, ModifyTableState, Plan, PlanState, PlannerInfo,
        RangeTblEntry, Relation, ResultRelInfo, RowMarkType, TupleTableSlot,
    },
    *,
};

use crate::{
    fdw::{
        common::{self, begin_remote_transaction, prepare_query_params, send_query_params},
        ctx::{
            from_fdw_private_modify, from_fdw_private_rel, into_fdw_private_modify,
            into_fdw_private_rel, mem::pg_transaction_scoped, FdwContext, FdwModifyContext,
            FdwQueryContext, FdwQueryType, FdwScanContext, FdwSelectQuery, PlannerContext,
        },
    },
    sqlil::{convert, from_datum, from_pg_type, into_pg_type},
    util::{string::to_pg_cstr, table::PgTable, tuple::slot_get_attr},
};

/// Number of executions of a single-row insert query we should "batch" together
/// This reduces the overhead of communicating with the FDW server over the unix socket.
const SINGLE_INSERT_BATCH_SIZE: usize = 100;

/// The data source could support batching of very high volume inserts but we dont
/// necessarily want to batch everything together due to memory constraints.
/// This is the upper limit we apply to batch inserts mapped to a single bulk query.
const MAX_BULK_INSERT_BATCH_SIZE: usize = 100;

#[pg_guard]
pub unsafe extern "C" fn add_foreign_update_targets(
    root: *mut PlannerInfo,
    rtindex: Index,
    target_rte: *mut RangeTblEntry,
    target_relation: Relation,
) {
    let mut ctx = pg_transaction_scoped(common::connect_table((*target_relation).rd_id));

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

    let rte = pg_sys::planner_rt_fetch(result_relation, root);

    let table = PgTable::open((*rte).relid as _, pg_sys::NoLock as _).unwrap();

    // We scope the connection to the top-level transaction
    // so all queries use the same connection which is required
    // for remote transactions or locking.
    let mut ctx = pg_transaction_scoped(common::connect_table(table.rd_id));

    let query = match (*plan).operation {
        pg_sys::CmdType_CMD_INSERT => {
            plan_foreign_insert(&mut ctx, root, plan, result_relation, rte, table)
        }
        pg_sys::CmdType_CMD_UPDATE => {
            plan_foreign_update(&mut ctx, root, plan, result_relation, rte, table)
        }
        pg_sys::CmdType_CMD_DELETE => {
            plan_foreign_delete(&mut ctx, root, plan, result_relation, rte, table)
        }
        _ => panic!("Unexpected operation: {}", (*plan).operation),
    };

    into_fdw_private_modify(
        ctx,
        pg_transaction_scoped(query),
        pg_transaction_scoped(FdwModifyContext::new()),
    )
}

unsafe fn plan_foreign_insert(
    ctx: &mut PgBox<FdwContext>,
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    rte: *mut RangeTblEntry,
    table: PgTable,
) -> FdwQueryContext {
    // Create an insert query to insert a single row
    let mut query = ctx
        .create_query(result_relation, sqlil::QueryType::Insert)
        .unwrap();

    // Determine columns specified in the insert
    let inserted_cols = if !table.trigdesc.is_null() && (*table.trigdesc).trig_insert_before_row {
        // If the target table has a BEFORE INSERT trigger we have to include all columns
        // as the trigger may change columns not specified in the query itself.
        table.attrs().collect::<Vec<_>>()
    } else {
        // Otherwise we use the columns specified from the query itself
        // TODO: This is potentially incompatible with columns with default values defined
        // in the postgres schema, we dont support this as of now but may do later!
        filtered_table_columns(&table, (*rte).insertedCols)
    };

    // Add a parameter for each column
    for att in inserted_cols {
        let (col_name, att_type, param) = create_param_for_col(att, &mut query);

        let op = InsertQueryOperation::AddColumn((col_name, sqlil::Expr::Parameter(param.clone())));

        match query.apply(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                panic!("Failed to create insert query on data source: unable to add query parameter for insert value")
            }
        }

        let insert = query.as_insert_mut().unwrap();
        insert.remote_ops.push(op);
        insert.params.push((param, att.attnum as _, att_type));
        insert.inserted_cols.push(att.attnum as _);
    }

    let insert = query.as_insert_mut().unwrap();
    insert.relid = (*rte).relid;

    query
}

unsafe fn plan_foreign_update(
    ctx: &mut PgBox<FdwContext>,
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    rte: *mut RangeTblEntry,
    table: PgTable,
) -> FdwQueryContext {
    // Create an update query to update a single row
    let mut query = ctx
        .create_query(result_relation, sqlil::QueryType::Update)
        .unwrap();

    // Determine the columns which are updated by the query
    let updated_cols = if !table.trigdesc.is_null() && (*table.trigdesc).trig_update_before_row {
        // If the target table has a BEFORE UPDATE trigger we have to include all columns
        // as the trigger may change columns not specified in the query itself.
        table.attrs().collect::<Vec<_>>()
    } else {
        // Otherwise we use the columns specified from the query itself
        let cols = pg_sys::bms_union((*rte).updatedCols, (*rte).extraUpdatedCols);

        filtered_table_columns(&table, cols)
    };

    // Add a parameter for each column to update
    for att in updated_cols.into_iter() {
        let (col_name, att_type, param) = create_param_for_col(att, &mut query);

        let op = UpdateQueryOperation::AddSet((col_name, sqlil::Expr::Parameter(param.clone())));

        match query.apply(op.clone().into()).unwrap() {
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

        match query.apply(op.clone().into()).unwrap() {
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

unsafe fn filtered_table_columns(
    table: &PgTable,
    cols: *mut pg_sys::Bitmapset,
) -> Vec<&pg_sys::FormData_pg_attribute> {
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
}

unsafe fn plan_foreign_delete(
    ctx: &mut PgBox<FdwContext>,
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    rte: *mut RangeTblEntry,
    table: PgTable,
) -> FdwQueryContext {
    // Create an delete query to delete a single row
    let mut query = ctx
        .create_query(result_relation, sqlil::QueryType::Delete)
        .unwrap();

    // Add a conditions to filter the row to by the row id
    let row_id_exprs = ctx.get_row_id_exprs(query.base_rel_alias()).unwrap();

    for (expr, r#type) in row_id_exprs.into_iter() {
        let param = query.create_param(r#type.clone());
        let op = DeleteQueryOperation::AddWhere(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
            expr,
            sqlil::BinaryOpType::Equal,
            sqlil::Expr::Parameter(param.clone()),
        )));

        match query.apply(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                panic!("Failed to create update query on data source: unable to add query parameter for row id condition")
            }
        }

        // We cannot determine the attr no at this stage in planning as
        // it dependent on the output subplan tlist.
        // We put a placeholder for now and defer until begin_foreign_modify
        let attnum = 0;

        let delete = query.as_delete_mut().unwrap();
        delete.remote_ops.push(op);
        delete
            .rowid_params
            .push((param, attnum, into_pg_type(&r#type).unwrap()))
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
    let (ctx, mut query, mut state) = from_fdw_private_modify(fdw_private);

    if query.as_insert().is_some() {
        // Save the singular insert query for later in case the batch size
        // needs to be changed
        state.singular_insert = Some(query.duplicate().unwrap());
    }

    // We still want to do batch size calculations for EXPLAIN
    // but skip the actual preparation of the queries.
    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        (*rinfo).ri_FdwState = fdw_private as *mut _;
        return;
    }

    // Upon the first modification query we begin a remote transaction
    begin_remote_transaction(&ctx.connection);

    // If this is an UPDATE/DELETE query we need to find the attr no's for the row id's
    // from the subplan tlist
    let row_id_params = match &mut query.q {
        FdwQueryType::Update(q) => Some(&mut q.rowid_params),
        FdwQueryType::Delete(q) => Some(&mut q.rowid_params),
        _ => None,
    };

    if let Some(row_id_params) = row_id_params {
        // Here we find the attr no's of the row id's from the subplan
        // This should be output in the tlist with names using the format below.
        // @see get_foreign_plan function.
        let subplan = (*outer_plan_state(mtstate as *mut _)).plan;

        for (idx, (_, attnum, _)) in row_id_params.iter_mut().enumerate() {
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

    query.prepare().unwrap();

    (*rinfo).ri_FdwState = fdw_private as *mut _;
}

/// We support 2 types of "batching":
///  1. A bulk insert query which inserts rows in single query, this is the preferred option
///     as it reduces network roundtrips to the data source but must be supported by the connector.
///  2. Performing multiple executions of insert query, each inserting a single row. This reduces
///     the overhead of communicating with the FDW server over the unix socket but can be supported
///     by connectors which dont support bulk inserts.
#[pg_guard]
pub unsafe extern "C" fn get_foreign_modify_batch_size(
    rinfo: *mut ResultRelInfo,
) -> ::std::os::raw::c_int {
    let (mut ctx, query, mut state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);

    // Batching is only supported for inserts
    if query.as_insert().is_none() {
        return 1;
    }

    // Disabling batching if the query semantics cannot support it.
    // Disable batching when we have to use RETURNING, there are any
    // BEFORE/AFTER ROW INSERT triggers on the foreign table, or there are any
    // WITH CHECK OPTION constraints from parent views.
    //
    // When there are any BEFORE ROW INSERT triggers on the table, we can't
    // support it, because such triggers might query the table we're inserting
    // into and act differently if the tuples that have already been processed
    // and prepared for insertion are not there.
    if !(*rinfo).ri_projectReturning.is_null()
        || !(*rinfo).ri_WithCheckOptions.is_null()
        || (!(*rinfo).ri_TrigDesc.is_null()
            && ((*(*rinfo).ri_TrigDesc).trig_insert_before_row
                || (*(*rinfo).ri_TrigDesc).trig_insert_after_row))
    {
        return 1;
    }

    // Get the unprepared insert query
    let singular_insert = state.singular_insert.as_mut().unwrap();

    // First get the max batch size for the current insert query
    let batch_size = singular_insert.get_max_batch_size().unwrap();

    // If the batch size is 1, the connector does not support bulk inserts
    if batch_size == 1 {
        return SINGLE_INSERT_BATCH_SIZE as _;
    }

    // Get the maximum batch size we support
    let batch_size = cmp::min(batch_size, MAX_BULK_INSERT_BATCH_SIZE as _);

    batch_size as _
}

unsafe fn create_bulk_insert(
    ctx: &mut FdwContext,
    singular_insert: &FdwQueryContext,
    batch_size: u32,
) -> Result<FdwQueryContext> {
    let mut query = ctx.create_query(singular_insert.base_varno, sqlil::QueryType::BulkInsert)?;

    let insert = singular_insert.as_insert().unwrap();

    let table = PgTable::open(insert.relid as _, pg_sys::NoLock as _).unwrap();

    // Determine columns specified in the insert
    let inserted_cols = table
        .attrs()
        .filter(|i| insert.inserted_cols.contains(&(i.attnum as _)))
        .collect::<Vec<_>>();

    let cols = inserted_cols.iter().map(|c| c.name().to_string()).collect();
    let mut values = vec![];

    // Create a parameter for each column in each
    for _ in 0..batch_size {
        for att in inserted_cols.iter() {
            let (col_name, att_type, param) = create_param_for_col(att, &mut query);

            let bulk_insert = query.as_bulk_insert_mut().unwrap();
            bulk_insert
                .params
                .push((param.clone(), att.attnum as _, att_type));
            values.push(sqlil::Expr::Parameter(param));
        }
    }

    // Pass the expressions to the connector
    let op = BulkInsertQueryOperation::SetBulkRows((cols, values));
    let res = query.apply(op.clone().into())?;

    let bulk_insert = query.as_bulk_insert_mut().unwrap();
    bulk_insert.remote_ops.push(op);
    bulk_insert.batch_size = batch_size;

    match res {
        QueryOperationResult::Ok(_) => Ok(query),
        QueryOperationResult::Unsupported => {
            bail!("Failed to create bulk insert query: connector returned unsupported")
        }
    }
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
    let (ctx, mut query, state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);

    // In case we started the query with a bulk insert but somehow
    // ended up here, we reset the query batch size to one
    if query.as_bulk_insert().is_some() {
        query = pg_transaction_scoped(state.singular_insert.as_ref().unwrap().duplicate().unwrap());
        query.prepare().unwrap();
    }

    let insert = query.as_insert().unwrap();
    let mut query_input = vec![];

    for (param, att_num, type_oid) in insert.params.iter() {
        query_input.push((
            param.id,
            slot_datum_into_data_val(slot, (att_num - 1) as _, *type_oid, &param.r#type),
        ));
    }

    query.execute_batch(vec![query_input]).unwrap();

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
    let (mut ctx, mut query, mut state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);
    let mut query_input = vec![];

    // Try create a bulk insert with the desired batch size
    // We could do this multiple times during a query execution.
    // Such as the first batch and the last batch which could have
    // different sizes.
    if (query.as_bulk_insert().is_none()
        || (*num_slots as u32) % query.as_bulk_insert().unwrap().batch_size != 0)
        && state.bulk_insert_supported != Some(false)
    {
        let singular_insert = state.singular_insert.as_ref().unwrap();

        query = if *num_slots == 1 {
            pg_transaction_scoped(singular_insert.duplicate().unwrap())
        } else {
            match create_bulk_insert(&mut *ctx, singular_insert, *num_slots as _) {
                Ok(mut query) => {
                    // We were able to create the bulk insert so we use this.
                    let query = pg_transaction_scoped(query);
                    state.bulk_insert_supported = Some(true);
                    query
                }
                Err(_) => {
                    // We failed to create the bulk insert for this connector,
                    // so we use a singular insert query.
                    // Mark it as unsupported so we dont attempt it again on the next batch
                    let query = pg_transaction_scoped(singular_insert.duplicate().unwrap());
                    state.bulk_insert_supported = Some(false);
                    query
                }
            }
        };

        // Prepare the query for execution
        query.prepare().unwrap();

        // Now update the fdw private state with the batched query
        (*rinfo).ri_FdwState = into_fdw_private_modify(ctx, query, state) as *mut _;
        (ctx, query, state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);
    }

    let batch_size = match &query.q {
        FdwQueryType::Insert(_) => 1,
        FdwQueryType::BulkInsert(i) => i.batch_size,
        _ => unreachable!(),
    };

    let params = match &query.q {
        FdwQueryType::Insert(i) => &i.params,
        FdwQueryType::BulkInsert(i) => &i.params,
        _ => unreachable!(),
    };

    let cols_per_row = state
        .singular_insert
        .as_ref()
        .unwrap()
        .as_insert()
        .unwrap()
        .inserted_cols
        .len();

    // At this point the number of slots will be divisible by the insert batch size
    // So we divide up the slots into executions of the query according to the batch size.
    let batches = (*num_slots) as u32 / batch_size;
    let mut slot_num = 0;

    for i in 0..batches {
        let mut batch_input = vec![];

        for (j, (param, att_num, type_oid)) in params.iter().enumerate() {
            let slot = *slots.add(slot_num);

            batch_input.push((
                param.id,
                slot_datum_into_data_val(slot, (att_num - 1) as _, *type_oid, &param.r#type),
            ));

            if (j + 1) % cols_per_row == 0 {
                slot_num += 1;
            }
        }

        query_input.push(batch_input);
    }

    query.execute_batch(query_input).unwrap();

    slots
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_update(
    estate: *mut EState,
    rinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    plan_slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let (ctx, mut query, _state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);
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

    query.execute_batch(vec![query_input]).unwrap();

    slot
}

#[pg_guard]
pub unsafe extern "C" fn exec_foreign_delete(
    estate: *mut EState,
    rinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    plan_slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let (ctx, mut query, _state) = from_fdw_private_modify((*rinfo).ri_FdwState as *mut _);
    let delete = query.as_delete().unwrap();
    let mut query_input = vec![];

    // Then bind the row id parameters (rowid's are stored as resjunk in the plan slot)
    for (param, att_num, type_oid) in delete.rowid_params.iter() {
        query_input.push((
            param.id,
            slot_datum_into_data_val(plan_slot, (att_num - 1) as _, *type_oid, &param.r#type),
        ));
    }

    query.execute_batch(vec![query_input]).unwrap();

    slot
}

#[pg_guard]
pub unsafe extern "C" fn end_foreign_modify(estate: *mut EState, rinfo: *mut ResultRelInfo) {
    // No manual clean up is needed as all items should be dropped
    // at the end of the memory contexts in which they were scoped to
}

#[pg_guard]
pub unsafe extern "C" fn plan_direct_modify(
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    subplan_index: ::std::os::raw::c_int,
) -> bool {
    // Currently, we do not support RETURNING in direct modifications
    if !(*plan).returningLists.is_null() {
        return false;
    }

    // We do not support conflict resolution in direct modifications
    if (*plan).onConflictAction != pg_sys::OnConflictAction_ONCONFLICT_NONE {
        return false;
    }

    // Try find the matching foreign scan node
    // which outputs the rows to be modified
    let foreign_scan = find_modify_table_subplan(root, plan, result_relation, subplan_index);

    if foreign_scan.is_null() {
        return false;
    }

    let (ctx, inner_select, planner) = from_fdw_private_rel((*foreign_scan).fdw_private);

    // If any quals need to be locally evaluated we cannot perform
    // the modification remotely
    if !inner_select.local_conds.is_empty() {
        return false;
    }

    // The only operations we support for direct modifications
    // are WHERE clauses
    if inner_select
        .as_select()
        .unwrap()
        .remote_ops
        .iter()
        .any(|op| !op.is_add_column() && !op.is_add_where() && !op.is_set_row_lock_mode())
    {
        return false;
    }

    let rte = pg_sys::planner_rt_fetch(result_relation, root);
    let planner =
        PlannerContext::base_rel(root, *(*root).simple_rel_array.add(result_relation as _));

    let table = PgTable::open((*rte).relid as _, pg_sys::NoLock as _).unwrap();

    let mut ctx = pg_transaction_scoped(common::connect_table(table.rd_id));

    let query = match (*plan).operation {
        pg_sys::CmdType_CMD_UPDATE => plan_direct_foreign_update(
            &mut ctx,
            root,
            plan,
            result_relation,
            &planner,
            &inner_select,
            table,
        ),
        pg_sys::CmdType_CMD_DELETE => plan_direct_foreign_delete(
            &mut ctx,
            root,
            plan,
            result_relation,
            &planner,
            &inner_select,
            table,
        ),
        _ => return false,
    };

    let query = match query {
        Some(q) => q,
        None => return false,
    };

    // Update the scan operation and result relation info
    (*foreign_scan).operation = (*plan).operation;
    (*foreign_scan).resultRelation = result_relation;

    // Update join relationed fields
    if (*foreign_scan).scan.scanrelid == 0 {
        (*foreign_scan).scan.plan.lefttree = ptr::null_mut();
    }

    // Update the fdw_private state with the modification query state
    (*foreign_scan).fdw_private = into_fdw_private_modify(
        ctx,
        pg_transaction_scoped(query),
        pg_transaction_scoped(FdwModifyContext::new()),
    );

    return true;
}

unsafe fn plan_direct_foreign_update(
    ctx: &mut PgBox<FdwContext>,
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    planner: &PlannerContext,
    inner_select: &FdwQueryContext,
    table: PgTable,
) -> Option<FdwQueryContext> {
    // Create an update query to update all rows specified by the
    // inner select query
    let mut query = ctx
        .create_query(result_relation, sqlil::QueryType::Update)
        .unwrap();

    // The expressions of concern are the first N columns of the processed
    // targetlist, where N is the length of the rel's update_colnos.
    let mut processed_tlist = ptr::null_mut();
    let mut target_attrs = ptr::null_mut();
    pg_sys::get_translated_update_targetlist(
        root,
        result_relation,
        &mut processed_tlist,
        &mut target_attrs,
    );
    let mut processed_tlist = PgList::<pg_sys::TargetEntry>::from_pg(processed_tlist);
    let mut target_attrs = PgList::<c_int>::from_pg(target_attrs);

    for (tle, attno) in processed_tlist.iter_ptr().zip(target_attrs.iter_int()) {
        if attno <= pg_sys::InvalidAttrNumber as _ {
            panic!("system-column update is not supported");
        }

        let col_attr = table.attrs().find(|i| i.attnum == attno as i16)?;

        // Try convert the tle expr to sqlil, if this fails we bail out
        let expr = match convert((*tle).expr as *mut _, &mut query.cvt, planner, ctx) {
            Ok(expr) => expr,
            Err(_) => return None,
        };

        // Try apply this as a SET expression to the update query
        let op = UpdateQueryOperation::AddSet((col_attr.name().to_string(), expr));

        match query.apply(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                return None;
            }
        }

        query.as_update_mut().unwrap().remote_ops.push(op);
    }

    // We apply the remote conditions of the inner select query to the update query
    for remote_cond in inner_select.remote_conds.iter().cloned() {
        // Try convert the cond to sqlil, if this fails we bail out
        let expr = match convert(
            (*remote_cond).clause as *mut _,
            &mut query.cvt,
            planner,
            ctx,
        ) {
            Ok(expr) => expr,
            Err(_) => return None,
        };

        // Try push down the where clause
        let op = UpdateQueryOperation::AddWhere(expr);

        match query.apply(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                return None;
            }
        }

        query.remote_conds.push(remote_cond);
        query.as_update_mut().unwrap().remote_ops.push(op);
    }

    // If we made it this far, we have been able to push down the entire update query
    Some(query)
}

unsafe fn plan_direct_foreign_delete(
    ctx: &mut PgBox<FdwContext>,
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    planner: &PlannerContext,
    inner_select: &FdwQueryContext,
    table: PgTable,
) -> Option<FdwQueryContext> {
    // Create an delete query to delete all rows specified by the
    // inner select query
    let mut query = ctx
        .create_query(result_relation, sqlil::QueryType::Delete)
        .unwrap();

    // We apply the remote conditions of the inner select query to the delete query
    for remote_cond in inner_select.remote_conds.iter().cloned() {
        // Try convert the cond to sqlil, if this fails we bail out
        let expr = match convert(
            (*remote_cond).clause as *mut _,
            &mut query.cvt,
            planner,
            ctx,
        ) {
            Ok(expr) => expr,
            Err(_) => return None,
        };

        // Try push down the where clause
        let op = DeleteQueryOperation::AddWhere(expr);

        match query.apply(op.clone().into()).unwrap() {
            QueryOperationResult::Ok(_) => {}
            QueryOperationResult::Unsupported => {
                return None;
            }
        }

        query.remote_conds.push(remote_cond);
        query.as_delete_mut().unwrap().remote_ops.push(op);
    }

    // If we made it this far, we have been able to push down the entire update query
    Some(query)
}

#[pg_guard]
pub unsafe extern "C" fn begin_direct_modify(
    node: *mut ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {
    // Skip if EXPLAIN query
    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        return;
    }

    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let (ctx, mut query, mut state) = from_fdw_private_modify((*plan).fdw_private);

    // Upon the first modification query we begin a remote transaction
    begin_remote_transaction(&ctx.connection);

    query.prepare().unwrap();

    prepare_query_params(&mut state.scan, &query, node);

    (*node).fdw_state = (*plan).fdw_private as *mut _;
}

#[pg_guard]
pub unsafe extern "C" fn iterate_direct_modify(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    let (ctx, mut query, state) = from_fdw_private_modify((*node).fdw_state as *mut _);

    // Send query params
    send_query_params(&mut query, &state.scan, node);

    // Execute the direct modification
    query.execute().unwrap();

    // Currently, we do not support RETURNING data from direct modifications
    // So we just clear the tuple and return.
    // equivalent of ExecClearTuple(slot) (symbol is not exposed)
    let slot = (*node).ss.ss_ScanTupleSlot;
    (*(*slot).tts_ops).clear.unwrap()(slot);

    return slot;
}

#[pg_guard]
pub unsafe extern "C" fn end_direct_modify(node: *mut ForeignScanState) {
    // Check if this is an EXPLAIN query and skip if so
    if (*node).fdw_state.is_null() {
        return;
    }
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

/// Creates a new query parameter for the supplied column
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

/// Finds a matching foreign scan node for a modify table node
/// used to try perform direct modification of the data source.
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a0fcf6729b47c456eec40d026d091255b
unsafe fn find_modify_table_subplan(
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    rtindex: Index,
    subplan_index: c_int,
) -> *mut ForeignScan {
    let mut subplan = outer_plan(plan as *mut _);

    // The cases we support are (1) the desired ForeignScan is the immediate
    // child of ModifyTable, or (2) it is the subplan_index'th child of an
    // Append node that is the immediate child of ModifyTable.  There is no
    // point in looking further down, as that would mean that local joins are
    // involved, so we can't do the update directly.
    //
    // There could be a Result atop the Append too, acting to compute the
    // UPDATE targetlist values.  We ignore that here; the tlist will be
    // checked by our caller.
    //
    // In principle we could examine all the children of the Append, but it's
    // currently unlikely that the core planner would generate such a plan
    // with the children out-of-order.  Moreover, such a search risks costing
    // O(N^2) time when there are a lot of children.
    if (*subplan).type_ == pg_sys::NodeTag_T_Append {
        let appendplan = subplan as *mut pg_sys::Append;
        let appendlist = PgList::<Plan>::from_pg((*appendplan).appendplans);

        if subplan_index < appendlist.len() as _ {
            subplan = appendlist.get_ptr(subplan_index as _).unwrap();
        }
    } else if (*subplan).type_ == pg_sys::NodeTag_T_Result
        && !outer_plan(subplan as *mut _).is_null()
        && (*outer_plan(subplan as *mut _)).type_ == pg_sys::NodeTag_T_Append
    {
        let appendplan = outer_plan(subplan as *mut _) as *mut pg_sys::Append;
        let appendlist = PgList::<Plan>::from_pg((*appendplan).appendplans);

        if subplan_index < appendlist.len() as _ {
            subplan = appendlist.get_ptr(subplan_index as _).unwrap();
        }
    }

    // Now, have we got a ForeignScan on the desired rel?
    if (*subplan).type_ == pg_sys::NodeTag_T_ForeignScan {
        let fscan = subplan as *mut ForeignScan;

        if (pg_sys::bms_is_member(rtindex as _, (*fscan).fs_relids)) {
            return fscan;
        }
    }

    ptr::null_mut()
}

#[inline]
unsafe fn outer_plan(plan: *mut pg_sys::Plan) -> *mut Plan {
    (*plan).lefttree
}

#[inline]
unsafe fn outer_plan_state(plan: *mut PlanState) -> *mut PlanState {
    (*plan).lefttree
}

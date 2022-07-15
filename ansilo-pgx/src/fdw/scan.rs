use std::{ffi::c_void, mem, ops::ControlFlow, ptr};

use ansilo_pg::fdw::proto::{
    ClientMessage, ClientSelectMessage, OperationCost, QueryOperationResult, SelectQueryOperation,
    ServerMessage, ServerSelectMessage,
};
use pgx::{
    pg_sys::{
        add_path, shm_toc, EquivalenceClass, EquivalenceMember, ForeignPath, ForeignScan,
        ForeignScanState, JoinPathExtraData, JoinType, List, Node, Oid, ParallelContext, Plan,
        PlannerInfo, RangeTblEntry, RelOptInfo, RestrictInfo, Size, TupleTableSlot,
        UpperRelationKind,
    },
    *,
};

use crate::sqlil::{
    convert, convert_list, parse_entity_version_id_from_foreign_table,
    parse_entity_version_id_from_rel, ConversionContext, PlannerContext,
};

use super::{
    common,
    ctx::{FdwContext, FdwQueryContext, FdwSelectQuery},
};

macro_rules! unexpected_response {
    ($res:expr) => {
        error!("Unexpected response from server: {:?}", $res)
    };
}

/// Default cost values in case they cant be estimated
/// Values borroed from
/// @see https://doxygen.postgresql.org/postgres__fdw_8c_source.html#l03570
const DEFAULT_FDW_STARTUP_COST: u64 = 100;
const DEFAULT_FDW_TUPLE_COST: f64 = 0.01;

/// Estimate # of rows and width of the result of the scan
///
/// We should consider the effect of all baserestrictinfo clauses here, but
/// not any join clauses.
pub unsafe extern "C" fn get_foreign_rel_size(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    let mut ctx = common::connect(foreigntableid);
    let planner = PlannerContext::base_rel(root, baserel);

    let baserel_conds = PgList::<Node>::from_pg((*baserel).baserestrictinfo);

    let entity = parse_entity_version_id_from_foreign_table(foreigntableid).unwrap();

    // If no conditions we can use the cheap path
    let res = ctx.send(ClientMessage::EstimateSize(entity)).unwrap();

    let mut base_cost = match res {
        ServerMessage::EstimatedSizeResult(e) => e,
        _ => unexpected_response!(res),
    };

    // We have to evaluate the possibility and costs of pushing down the restriction clauses
    let mut cvt = ConversionContext::new();
    let conds = baserel_conds
        .iter_ptr()
        .filter_map(|i| convert(i, &mut cvt, &planner, &*ctx).ok())
        .map(|i| SelectQueryOperation::AddWhere(i))
        .collect::<Vec<_>>();
    let mut base_query = estimate_path_cost(&mut ctx, baserel, &planner, None, conds);

    // Default to base cost
    {
        let cost = &mut base_query.cost;
        cost.rows = cost.rows.or(base_cost.rows);
        cost.row_width = cost.row_width.or(base_cost.row_width);
        cost.connection_cost = cost
            .connection_cost
            .or(base_cost.connection_cost)
            .or(Some(DEFAULT_FDW_STARTUP_COST));
        cost.total_cost = cost.total_cost.or(base_cost.total_cost);
    }

    if let Some(rows) = base_query.cost.rows {
        (*baserel).rows = rows as _;
    }

    if let Some(row_width) = base_query.cost.row_width {
        (*(*baserel).reltarget).width = row_width as _;
    }

    (*baserel).fdw_private = into_fdw_private(ctx, base_query) as *mut _;
}

/// Create possible scan paths for a scan on the foreign table
///
/// @see https://doxygen.postgresql.org/postgres__fdw_8c.html#a5e0a23f5638e9b82a7e8c6c5be3389a2
pub unsafe extern "C" fn get_foreign_paths(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    let (mut ctx, base_query) = from_fdw_private((*baserel).fdw_private as *mut _);
    let base_cost = base_query.cost.clone();
    let planner = PlannerContext::base_rel(root, baserel);

    // Create a default full-scan path for the rel
    let path = pg_sys::create_foreignscan_path(
        root,
        baserel,
        ptr::null_mut(),
        base_cost.rows.unwrap() as f64,
        base_cost.connection_cost.unwrap() as f64,
        base_cost.total_cost.unwrap() as f64,
        ptr::null_mut(),
        (*baserel).lateral_relids,
        ptr::null_mut(),
        into_fdw_private_path(PgBox::new(base_query.clone()).into_pg_boxed()),
    );
    add_path(baserel, path as *mut pg_sys::Path);

    // Generate parameterised paths for nested-loop joins
    // with few rows on the outer rel
    let mut join_restrictions = PgList::<RestrictInfo>::from_pg((*baserel).joininfo)
        .iter_ptr()
        .collect::<Vec<_>>();

    if (*baserel).has_eclass_joins {
        /* Callback argument for ec_member_matches_foreign */
        #[repr(C)]
        struct EcMemberCallback {
            current: *mut pg_sys::Expr, /* current expr, or NULL if not yet found */
            already_used: *mut pg_sys::List, /* expressions already dealt with */
        }

        unsafe extern "C" fn member_matches_foreign_cb(
            root: *mut PlannerInfo,
            rel: *mut RelOptInfo,
            ec: *mut EquivalenceClass,
            em: *mut EquivalenceMember,
            arg: *mut ::std::os::raw::c_void,
        ) -> bool {
            let arg: *mut EcMemberCallback = arg as *mut _;
            let expr = (*em).em_expr;

            if !(*arg).current.is_null() {
                return pg_sys::equal(expr as _, (*arg).current as _);
            }

            if pg_sys::list_member((*arg).already_used, expr as _) {
                return false;
            }

            (*arg).current = expr;
            return true;
        }

        let mut arg: EcMemberCallback = mem::zeroed();

        loop {
            arg.current = ptr::null_mut();

            let clauses = pg_sys::generate_implied_equalities_for_column(
                root,
                baserel,
                Some(member_matches_foreign_cb),
                &mut arg as *mut EcMemberCallback as *mut _,
                (*baserel).lateral_referencers,
            );

            if arg.current.is_null() {
                break;
            }

            let clauses = PgList::<RestrictInfo>::from_pg(clauses);
            join_restrictions.append(&mut clauses.iter_ptr().collect());

            arg.already_used = pg_sys::lappend(arg.already_used, arg.current as _);
        }
    }

    let mut cvt = ConversionContext::new();
    let param_paths = join_restrictions
        .into_iter()
        .filter(|i| pg_sys::join_clause_is_movable_to(*i, baserel))
        .filter(|i| convert((**i).clause as *mut Node, &mut cvt, &planner, &ctx).is_ok())
        .filter_map(|i| {
            let required_outer = pg_sys::bms_union((*i).clause_relids, (*baserel).lateral_relids);
            let required_outer = pg_sys::bms_del_member(required_outer, (*baserel).relid as _);

            if pg_sys::bms_is_empty(required_outer) {
                None
            } else {
                Some(required_outer)
            }
        })
        .map(|i| pg_sys::get_baserel_parampathinfo(root, baserel, i))
        .collect::<Vec<_>>();

    for ppi in param_paths.into_iter() {
        // Create a path for each parameterised path option
        let mut cvt = ConversionContext::new();
        let ops = convert_list((*ppi).ppi_clauses, &mut cvt, &planner, &ctx)
            .unwrap()
            .into_iter()
            .map(|i| SelectQueryOperation::AddWhere(i))
            .collect::<Vec<_>>();

        let query = estimate_path_cost(&mut ctx, baserel, &planner, Some(&base_query), ops);

        let path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(),
            query.cost.rows.unwrap() as f64,
            query.cost.connection_cost.unwrap() as f64,
            query.cost.total_cost.unwrap() as f64,
            ptr::null_mut(),
            (*ppi).ppi_req_outer,
            ptr::null_mut(),
            into_fdw_private_path(query),
        );
        add_path(baserel, path as *mut pg_sys::Path);
    }

    // TODO: explore value of exploiting query_pathkeys
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

pub unsafe extern "C" fn begin_foreign_scan(
    node: *mut ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {
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

pub unsafe extern "C" fn shutdown_foreign_scan(node: *mut ForeignScanState) {
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

// Generate a path cost estimation based on the supplied conditions
unsafe fn estimate_path_cost(
    ctx: &mut FdwContext,
    rel: *mut RelOptInfo,
    planner: &PlannerContext,
    base_query: Option<&FdwQueryContext>,
    query_ops: Vec<SelectQueryOperation>,
) -> PgBox<FdwQueryContext, AllocatedByPostgres> {
    let mut query = base_query
        .cloned()
        .unwrap_or_else(|| FdwQueryContext::select());
    let select = query.as_select().unwrap();

    // Initialise a new select query
    let entity = parse_entity_version_id_from_rel((*rel).relid).unwrap();
    let res = ctx
        .send(ClientMessage::Select(ClientSelectMessage::Create(entity)))
        .unwrap();

    let mut cost = match res {
        ServerMessage::Select(ServerSelectMessage::Result(
            QueryOperationResult::PerformedRemotely(cost),
        )) => cost,
        _ => unexpected_response!(res),
    };

    let mut cost = OperationCost::default();

    // Apply each of the query operations and evaluate the cost
    for query_op in query_ops {
        if let Some(new_cost) = apply_query_operation(ctx, select, query_op) {
            cost = new_cost;
        }
    }

    query.cost = cost;

    PgBox::new(query).into_pg_boxed()
}

fn apply_query_operation(
    ctx: &mut FdwContext,
    select: &mut FdwSelectQuery,
    query_op: SelectQueryOperation,
) -> Option<OperationCost> {
    let response = ctx
        .send(ClientMessage::Select(ClientSelectMessage::Apply(
            query_op.clone(),
        )))
        .unwrap();

    let result = match response {
        ServerMessage::Select(ServerSelectMessage::Result(result)) => result,
        _ => unexpected_response!(response),
    };

    match result {
        QueryOperationResult::PerformedRemotely(cost) => {
            select.remote_ops.push(query_op);
            Some(cost)
        }
        QueryOperationResult::PerformedLocally => {
            select.local_ops.push(query_op);
            // TODO:
            // evaluate_local_cost()
            None
        }
    }
}

/// Converts the supplied context data to a pointer suitable
/// to be stored in fdw_private fields
unsafe fn into_fdw_private(ctx: PgBox<FdwContext>, query: PgBox<FdwQueryContext>) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ctx.into_pg() as *mut _);
    list.push(query.into_pg() as *mut _);

    list.into_pg()
}

unsafe fn from_fdw_private(
    list: *mut List,
) -> (
    PgBox<FdwContext, AllocatedByPostgres>,
    PgBox<FdwQueryContext, AllocatedByPostgres>,
) {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 2);

    let ctx = PgBox::<FdwContext>::from_pg(list.get_ptr(0).unwrap() as *mut _);
    let query = PgBox::<FdwQueryContext>::from_pg(list.get_ptr(1).unwrap() as *mut _);

    (ctx, query)
}

unsafe fn into_fdw_private_path(query: PgBox<FdwQueryContext>) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(query.into_pg() as *mut _);

    list.into_pg()
}

unsafe fn from_fdw_private_path(list: *mut List) -> PgBox<FdwQueryContext, AllocatedByPostgres> {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 1);

    let query = PgBox::<FdwQueryContext>::from_pg(list.get_ptr(0).unwrap() as *mut _);

    query
}

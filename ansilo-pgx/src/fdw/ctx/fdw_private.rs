use std::ffi::c_void;

use pgx::{
    pg_sys::{makeConst, List},
    *,
};

use super::{
    FdwContext, FdwModifyContext, FdwPlanContext, FdwQueryContext, FdwScanContext, PlannerContext,
};

/// Converts the supplied context data to a pointer suitable
/// to be stored in fdw_private fields
pub(crate) unsafe fn into_fdw_private_rel(
    ctx: PgBox<FdwContext, AllocatedByPostgres>,
    query: PgBox<FdwQueryContext, AllocatedByPostgres>,
    planner: PgBox<PlannerContext, AllocatedByPostgres>,
) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ptr_to_node(ctx.into_pg()));
    list.push(ptr_to_node(query.into_pg()));
    list.push(ptr_to_node(planner.into_pg()));

    list.into_pg()
}

#[track_caller]
pub(crate) unsafe fn from_fdw_private_rel(
    list: *mut List,
) -> (
    PgBox<FdwContext, AllocatedByPostgres>,
    PgBox<FdwQueryContext, AllocatedByPostgres>,
    PgBox<PlannerContext, AllocatedByPostgres>,
) {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 3);

    let ctx = PgBox::<FdwContext>::from_pg(node_to_ptr(list.get_ptr(0).unwrap()));
    let query = PgBox::<FdwQueryContext>::from_pg(node_to_ptr(list.get_ptr(1).unwrap()));
    let planner = PgBox::<PlannerContext>::from_pg(node_to_ptr(list.get_ptr(2).unwrap()));

    (ctx, query, planner)
}

pub(crate) unsafe fn into_fdw_private_path(
    planner: PgBox<PlannerContext, AllocatedByPostgres>,
    query: PgBox<FdwQueryContext, AllocatedByPostgres>,
) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ptr_to_node(planner.into_pg()));
    list.push(ptr_to_node(query.into_pg()));

    list.into_pg()
}

#[track_caller]
pub(crate) unsafe fn from_fdw_private_path(
    list: *mut List,
) -> (
    PgBox<PlannerContext, AllocatedByPostgres>,
    PgBox<FdwQueryContext, AllocatedByPostgres>,
) {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 2);

    let planner = PgBox::<PlannerContext>::from_pg(node_to_ptr(list.get_ptr(0).unwrap()));
    let query = PgBox::<FdwQueryContext>::from_pg(node_to_ptr(list.get_ptr(1).unwrap()));

    (planner, query)
}

pub(crate) unsafe fn into_fdw_private_scan(
    query: PgBox<FdwQueryContext, AllocatedByPostgres>,
    scan: PgBox<FdwScanContext, AllocatedByPostgres>,
) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ptr_to_node(query.into_pg()));
    list.push(ptr_to_node(scan.into_pg()));

    list.into_pg()
}

#[track_caller]
pub(crate) unsafe fn from_fdw_private_scan(
    list: *mut List,
) -> (
    PgBox<FdwQueryContext, AllocatedByPostgres>,
    PgBox<FdwScanContext, AllocatedByPostgres>,
) {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 2);

    let query = PgBox::<FdwQueryContext>::from_pg(node_to_ptr(list.get_ptr(0).unwrap()));
    let scan = PgBox::<FdwScanContext>::from_pg(node_to_ptr(list.get_ptr(1).unwrap()));

    (query, scan)
}

pub(crate) unsafe fn into_fdw_private_modify(
    ctx: PgBox<FdwContext, AllocatedByPostgres>,
    query: PgBox<FdwQueryContext, AllocatedByPostgres>,
    modify: PgBox<FdwModifyContext, AllocatedByPostgres>,
) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ptr_to_node(ctx.into_pg()));
    list.push(ptr_to_node(query.into_pg()));
    list.push(ptr_to_node(modify.into_pg()));

    list.into_pg()
}

#[track_caller]
pub(crate) unsafe fn from_fdw_private_modify(
    list: *mut List,
) -> (
    PgBox<FdwContext, AllocatedByPostgres>,
    PgBox<FdwQueryContext, AllocatedByPostgres>,
    PgBox<FdwModifyContext, AllocatedByPostgres>,
) {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 3);

    let ctx = PgBox::<FdwContext>::from_pg(node_to_ptr(list.get_ptr(0).unwrap()));
    let query = PgBox::<FdwQueryContext>::from_pg(node_to_ptr(list.get_ptr(1).unwrap()));
    let modify = PgBox::<FdwModifyContext>::from_pg(node_to_ptr(list.get_ptr(2).unwrap()));

    (ctx, query, modify)
}

pub(crate) unsafe fn into_fdw_private_plan(
    plan: PgBox<FdwPlanContext, AllocatedByPostgres>,
) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ptr_to_node(plan.into_pg()));

    list.into_pg()
}

#[track_caller]
pub(crate) unsafe fn from_fdw_private_plan(
    list: *mut List,
) -> (PgBox<FdwPlanContext, AllocatedByPostgres>,) {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 1);

    let plan = PgBox::<FdwPlanContext>::from_pg(node_to_ptr(list.get_ptr(0).unwrap()));

    (plan,)
}

/// Our fdw_private lists need to be copyable via postgres internal copyObject
/// So we stuff our ptr's to rust structs in pg Const nodes
unsafe fn ptr_to_node<T>(ptr: *mut T) -> *mut c_void {
    let datum: Datum = ptr.into();
    let node = makeConst(pg_sys::INT8OID, 0, 0, 0, datum, false, true);

    // makeConst will pgalloc memory so we can just return the pointer
    node as *mut _
}

/// Takes a pointer to a const node and returns the original pointer
/// stored within
unsafe fn node_to_ptr<T>(node: *mut c_void) -> *mut T {
    let node = node as *mut pg_sys::Const;

    assert!(!node.is_null());
    assert!((*node).xpr.type_ == pg_sys::NodeTag_T_Const);
    assert!((*node).consttype == pg_sys::INT8OID);
    assert!((*node).constbyval);

    (*node).constvalue.cast_mut_ptr::<T>()
}

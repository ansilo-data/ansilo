use std::ffi::c_void;

use pgx::{*, pg_sys::List};

use crate::sqlil::PlannerContext;

use super::{FdwContext, FdwQueryContext, FdwScanContext};

/// Converts the supplied context data to a pointer suitable
/// to be stored in fdw_private fields
pub(crate) unsafe fn into_fdw_private_rel(ctx: PgBox<FdwContext>, query: PgBox<FdwQueryContext>) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ctx.into_pg() as *mut _);
    list.push(query.into_pg() as *mut _);

    list.into_pg()
}

pub(crate) unsafe fn from_fdw_private_rel(
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

pub(crate) unsafe fn into_fdw_private_path(planner: PgBox<PlannerContext>) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(planner.into_pg() as *mut _);

    list.into_pg()
}

pub(crate) unsafe fn from_fdw_private_path(list: *mut List) -> PgBox<PlannerContext, AllocatedByPostgres> {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 1);

    let query = PgBox::<PlannerContext>::from_pg(list.get_ptr(0).unwrap() as *mut _);

    query
}

pub(crate) unsafe fn into_fdw_private_scan(
    ctx: PgBox<FdwContext>,
    query: PgBox<FdwQueryContext>,
    scan: PgBox<FdwScanContext>,
) -> *mut List {
    let mut list = PgList::<c_void>::new();

    list.push(ctx.into_pg() as *mut _);
    list.push(query.into_pg() as *mut _);
    list.push(scan.into_pg() as *mut _);

    list.into_pg()
}

pub(crate) unsafe fn from_fdw_private_scan(
    list: *mut List,
) -> (
    PgBox<FdwContext, AllocatedByPostgres>,
    PgBox<FdwQueryContext, AllocatedByPostgres>,
    PgBox<FdwScanContext, AllocatedByPostgres>,
) {
    let list = PgList::<c_void>::from_pg(list);
    assert!(list.len() == 3);

    let ctx = PgBox::<FdwContext>::from_pg(list.get_ptr(0).unwrap() as *mut _);
    let query = PgBox::<FdwQueryContext>::from_pg(list.get_ptr(1).unwrap() as *mut _);
    let scan = PgBox::<FdwScanContext>::from_pg(list.get_ptr(2).unwrap() as *mut _);

    (ctx, query, scan)
}

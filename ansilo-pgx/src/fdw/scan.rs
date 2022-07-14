use ansilo_pg::fdw::proto::{
    ClientMessage, ClientSelectMessage, QueryOperationResult, SelectQueryOperation, ServerMessage,
    ServerSelectMessage,
};
use pgx::{
    pg_sys::{
        shm_toc, ForeignPath, ForeignScan, ForeignScanState, JoinPathExtraData, JoinType, List,
        Node, Oid, ParallelContext, Plan, PlannerInfo, RangeTblEntry, RelOptInfo, Size,
        TupleTableSlot, UpperRelationKind,
    },
    *,
};

use crate::sqlil::{
    convert, parse_entity_version_id_from_foreign_table, ConversionContext, PlannerContext,
};

use super::{common, ctx::FdwQuery};

macro_rules! unexpected_response {
    ($res:expr) => {
        error!("Unexpected response from server: {:?}", $res)
    };
}

/// Estimate # of rows and width of the result of the scan
///
/// We should consider the effect of all baserestrictinfo clauses here, but
/// not any join clauses.
pub unsafe extern "C" fn get_foreign_rel_size(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
    let mut ctx = common::connect(foreigntableid, FdwQuery::select());
    (*baserel).fdw_private = ctx.as_ptr() as _;
    let planner = PlannerContext::base_rel(root, baserel);

    let query = ctx.query.as_select().unwrap();

    let baserel_conds = PgList::<Node>::from_pg((*baserel).baserestrictinfo);

    let entity = parse_entity_version_id_from_foreign_table(foreigntableid).unwrap();

    if baserel_conds.is_empty() {
        // If no conditions we can use the cheap path
        let res = ctx.send(ClientMessage::EstimateSize(entity)).unwrap();

        let estimate = match res {
            ServerMessage::EstimatedSizeResult(e) => e,
            _ => unexpected_response!(res),
        };

        if let Some(rows) = estimate.rows {
            (*baserel).rows = rows as _;
        }

        if let Some(row_width) = estimate.row_width {
            (*(*baserel).reltarget).width = row_width as _;
        }
    } else {
        // We have to evaluate the possibility and costs of pushing down the restriction clauses
        let res = ctx
            .send(ClientMessage::Select(ClientSelectMessage::Create(entity)))
            .unwrap();

        let mut cost = match res {
            ServerMessage::Select(ServerSelectMessage::Result(
                QueryOperationResult::PerformedRemotely(cost),
            )) => cost,
            _ => unexpected_response!(res),
        };

        let mut cvt = ConversionContext::new();
        let conds = PgList::<Node>::from_pg((*baserel).baserestrictinfo);
        let conds = conds
            .iter_ptr()
            .filter_map(|i| convert(i, &mut cvt, &planner, &ctx).ok())
            .collect::<Vec<_>>();

        for cond in conds.into_iter() {
            let res = ctx
                .send(ClientMessage::Select(ClientSelectMessage::Apply(
                    SelectQueryOperation::AddWhere(cond),
                )))
                .unwrap();

            cost = match res {
                ServerMessage::Select(ServerSelectMessage::Result(
                    QueryOperationResult::PerformedRemotely(cost),
                )) => cost,
                ServerMessage::Select(ServerSelectMessage::Result(
                    QueryOperationResult::PerformedLocally,
                )) => continue,
                _ => unexpected_response!(res),
            };
        }

        if let Some(rows) = cost.rows {
            (*baserel).rows = rows as _;
        }

        // TODO: calc row width on this path?
    }
}

pub unsafe extern "C" fn get_foreign_paths(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreigntableid: Oid,
) {
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

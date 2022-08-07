use std::{cmp, collections::HashMap, ffi::c_void, mem, ops::ControlFlow, ptr};

use ansilo_core::{
    data::DataValue,
    err::{bail, Context, Result},
    sqlil::{self, JoinType, Ordering, OrderingType, QueryType},
};
use ansilo_pg::fdw::{
    data::DataWriter,
    proto::{
        ClientMessage, OperationCost, QueryInputStructure, QueryOperationResult, RowStructure,
        SelectQueryOperation, ServerMessage,
    },
};
use pgx::{
    pg_sys::{
        add_path, shm_toc, EquivalenceClass, EquivalenceMember, ForeignPath, ForeignScan,
        ForeignScanState, JoinPathExtraData, List, Node, Oid, ParallelContext, Path, PathKey, Plan,
        PlannerInfo, RangeTblEntry, RelOptInfo, RestrictInfo, Size, TargetEntry, TupleTableSlot,
        UpperRelationKind,
    },
    *,
};

use crate::{
    fdw::{common, ctx::*},
    sqlil::{
        convert, convert_list, from_datum, into_datum, into_pg_type,
        parse_entity_version_id_from_foreign_table, parse_entity_version_id_from_rel,
        ConversionContext,
    },
    util::{list::vec_to_pg_list, string::to_pg_cstr, table::PgTable},
};

pub(crate) unsafe fn prepare_query_params(
    scan: &mut FdwScanContext,
    query: &FdwQueryContext,
    node: *mut ForeignScanState,
) {
    // Prepare the query param expr's for evaluation
    let param_nodes = query.cvt.param_nodes();
    let param_exprs = PgList::<pg_sys::ExprState>::from_pg(pg_sys::ExecInitExprList(
        vec_to_pg_list(param_nodes.clone()),
        node as _,
    ));

    // Collect list of param id's to their respective ExprState nodes
    let param_map = param_exprs
        .iter_ptr()
        .zip(param_nodes.into_iter())
        .enumerate()
        .flat_map(|(idx, (expr, node))| {
            query
                .cvt
                .param_ids(node)
                .into_iter()
                .map(|id| (id, (expr, pg_sys::exprType(node))))
                .collect::<Vec<_>>()
                .into_iter()
        })
        .collect::<HashMap<_, _>>();

    scan.param_exprs = Some(param_map);
}

pub(crate) unsafe fn send_query_params(
    query: &mut FdwQueryContext,
    scan: &FdwScanContext,
    node: *mut ForeignScanState,
) {
    let input_data = {
        let input_structure = query
            .get_input_structure()
            .expect("Failed to send query params");

        if input_structure.params.is_empty() {
            return;
        }

        // Evaluate each parameter to a datum
        // We do so in a short-lived memory context so as not to leak the memory
        let param_exprs = scan.param_exprs.as_ref().unwrap();
        let econtext = (*node).ss.ps.ps_ExprContext;

        PgMemoryContexts::For((*econtext).ecxt_per_tuple_memory).switch_to(|context| {
            input_structure
                .params
                .iter()
                .map(|(id, r#type)| {
                    let (expr, type_oid) = *param_exprs.get(id).unwrap();
                    let mut is_null = false;

                    let datum = (*expr).evalfunc.unwrap()(expr, econtext, &mut is_null as *mut _);
                    from_datum(type_oid, datum).unwrap()
                })
                .collect::<Vec<_>>()
        })
    };

    // Finally, serialise and send the query params
    query.write_query_input(input_data).unwrap();
}

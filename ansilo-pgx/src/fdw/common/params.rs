use std::collections::HashMap;

use pgx::{pg_sys::ForeignScanState, *};

use crate::{fdw::ctx::*, sqlil::from_datum, util::list::vec_to_pg_list};

pub(crate) unsafe fn prepare_query_params(
    scan: &mut FdwScanContext,
    query: &FdwQueryContext,
    node: *mut ForeignScanState,
) {
    pgx::debug1!("Preparing query params");

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
        .flat_map(|(_idx, (expr, node))| {
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
    pgx::debug1!("Sending query params");

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

        PgMemoryContexts::For((*econtext).ecxt_per_tuple_memory).switch_to(|_context| {
            input_structure
                .params
                .iter()
                .map(|(id, r#type)| {
                    let (expr, type_oid) = *param_exprs.get(id).unwrap();
                    let mut is_null = false;

                    let datum = (*expr).evalfunc.unwrap()(expr, econtext, &mut is_null as *mut _);
                    from_datum(type_oid, datum)
                        .unwrap()
                        .try_coerce_into(r#type)
                        .unwrap()
                })
                .collect::<Vec<_>>()
        })
    };

    // Finally, serialise and send the query params
    query.write_params(input_data).unwrap();
    pgx::debug1!("Sending query sent");
}

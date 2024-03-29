use std::{
    ffi::CString,
    os::unix::{net::UnixStream, prelude::FromRawFd},
    ptr,
    sync::Arc,
};

use ansilo_core::{data::DataType, err::Result, sqlil};
use ansilo_pg::fdw::channel::IpcClientChannel;
use pgx::{
    pg_sys::{self, Node},
    *,
};

use crate::{
    fdw::{
        common::{FdwIpcConnection, TableOptions},
        ctx::{FdwContext, PlannerContext},
    },
    sqlil::datum::into_pg_type,
};

use super::ConversionContext;

/// Converts the first target expr from the supplied select query to SQLIL for testing
pub(super) fn convert_simple_expr(select: &'static str) -> Result<sqlil::Expr> {
    let mut ctx = ConversionContext::new();
    convert_simple_expr_with_context(select, &mut ctx, vec![])
}

pub(super) fn convert_simple_expr_with_context(
    select: &'static str,
    ctx: &mut ConversionContext,
    params: Vec<DataType>,
) -> Result<sqlil::Expr> {
    unsafe {
        let (node, planner) = parse_pg_expr(select, params);

        let client = IpcClientChannel::new(UnixStream::from_raw_fd(1234));
        let con = FdwIpcConnection::new("data_source", client);

        let fdw = FdwContext::new(
            Arc::new(con),
            sqlil::entity("entity"),
            123 as _,
            TableOptions::default(),
        );

        super::convert(node.as_ptr() as *const _, ctx, &planner, &fdw)
    }
}

fn parse_pg_expr(select: &'static str, params: Vec<DataType>) -> (PgBox<Node>, PlannerContext) {
    unsafe {
        let cstr = CString::new(select.to_string()).unwrap();

        let parse_tree = pg_sys::pg_parse_query(cstr.as_ptr());
        let parse_tree = PgList::<pg_sys::RawStmt>::from_pg(parse_tree);
        let stmt_node = parse_tree.head().unwrap();

        let mut params = params
            .iter()
            .map(|i| into_pg_type(i).unwrap())
            .collect::<Vec<_>>();
        let query_tree = pg_sys::pg_analyze_and_rewrite_fixedparams(
            stmt_node,
            cstr.as_ptr(),
            params.as_mut_slice().as_mut_ptr(),
            params.len() as _,
            ptr::null_mut(),
        );
        let query_tree = PgList::<pg_sys::Query>::from_pg(query_tree);
        let query = query_tree.head().unwrap();

        let planner_info = pg_sys::subquery_planner(
            &mut pg_sys::PlannerGlobal::default() as *mut _,
            query,
            ptr::null_mut(),
            false,
            0.0,
        );
        pg_sys::setup_simple_rel_arrays(planner_info);

        let base_rel = pg_sys::build_simple_rel(planner_info, 1, ptr::null_mut());

        let target_node = PgList::<Node>::from_pg((*query).targetList).head().unwrap()
            as *mut pg_sys::TargetEntry;
        let expr_node = (*target_node).expr as *mut Node;

        (
            PgBox::from_pg(expr_node).into_pg_boxed(),
            PlannerContext::base_rel(planner_info, base_rel),
        )
    }
}

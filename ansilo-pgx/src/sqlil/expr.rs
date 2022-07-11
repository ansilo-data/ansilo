use ansilo_core::{err::Result, sqlil};

use crate::fdw::ctx::FdwContext;

/// Try convert a postgres expression to a SQLIL expr
pub unsafe fn convert(node: *mut pgx::pg_sys::Node, ctx: &FdwContext) -> Result<sqlil::Expr> {
    todo!()
}

use ansilo_core::{err::{Result, bail}, sqlil, common::data::DataValue};
use pgx::pg_sys::{Node, self};

use crate::fdw::ctx::FdwContext;

use super::SqlilContext;

/// Try convert a postgres expression to a SQLIL expr
pub unsafe fn convert(node: *const Node, ctx: &SqlilContext, fdw: &FdwContext) -> Result<sqlil::Expr> {
    match (*node).type_ {
        pg_sys::NodeTag_T_A_Const => convert_const(node as *const pg_sys::Const, ctx, fdw),
        tag @ _ => bail!("Unknown node tag type: {}", tag)
    }
}

unsafe fn convert_const(node: *const pg_sys::Const, ctx: &SqlilContext, fdw: &FdwContext) -> Result<sqlil::Expr> {
    if (*node).constisnull {
        return Ok(sqlil::Expr::constant(DataValue::Null));
    }

    todo!()
    // match (*node).consttype {
    //     // pg_sys::INT2OID
    // }
}

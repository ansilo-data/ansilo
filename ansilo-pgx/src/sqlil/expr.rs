use ansilo_core::{
    err::{bail, Result},
    sqlil,
};
use pgx::{
    pg_sys::{self, List, Node},
    PgList,
};

use crate::fdw::ctx::{FdwContext, PlannerContext};

use super::*;

/// Try convert a postgres expression to a SQLIL expr
pub unsafe fn convert(
    node: *const Node,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    match (*node).type_ {
        pg_sys::NodeTag_T_Const => convert_const(node as *const pg_sys::Const, ctx, planner, fdw),
        pg_sys::NodeTag_T_Param => convert_param(node as *const pg_sys::Param, ctx, planner, fdw),
        pg_sys::NodeTag_T_Var => convert_var(node as *const pg_sys::Var, ctx, planner, fdw),
        pg_sys::NodeTag_T_FuncExpr => {
            convert_func_expr(node as *const pg_sys::FuncExpr, ctx, planner, fdw)
        }
        pg_sys::NodeTag_T_CoalesceExpr => {
            convert_coalesce_expr(node as *const pg_sys::CoalesceExpr, ctx, planner, fdw)
        }
        pg_sys::NodeTag_T_OpExpr => {
            convert_op_expr(node as *const pg_sys::OpExpr, ctx, planner, fdw)
        }
        pg_sys::NodeTag_T_ScalarArrayOpExpr => convert_scalar_op_array_expr(
            node as *const pg_sys::ScalarArrayOpExpr,
            ctx,
            planner,
            fdw,
        ),
        pg_sys::NodeTag_T_DistinctExpr => {
            convert_distinct_expr(node as *const pg_sys::DistinctExpr, ctx, planner, fdw)
        }
        pg_sys::NodeTag_T_RelabelType => {
            convert_relabel_type(node as *const pg_sys::RelabelType, ctx, planner, fdw)
        }
        pg_sys::NodeTag_T_BoolExpr => {
            convert_bool_expr(node as *const pg_sys::BoolExpr, ctx, planner, fdw)
        }
        pg_sys::NodeTag_T_NullTest => {
            convert_null_test(node as *const pg_sys::NullTest, ctx, planner, fdw)
        }
        pg_sys::NodeTag_T_CaseExpr => {
            convert_case_expr(node as *const pg_sys::CaseExpr, ctx, planner, fdw)
        }
        pg_sys::NodeTag_T_Aggref => {
            convert_aggref(node as *const pg_sys::Aggref, ctx, planner, fdw)
        }
        tag @ _ => bail!("Unknown node tag type: {}", tag),
    }
}

pub unsafe fn convert_list(
    list: *const List,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<Vec<sqlil::Expr>> {
    let list = PgList::<Node>::from_pg(list as *mut _);

    list.iter_ptr()
        .map(|i| convert(i, ctx, planner, fdw))
        .collect::<Result<Vec<_>>>()
}

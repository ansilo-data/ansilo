use ansilo_core::{
    common::data::DataValue,
    err::{bail, Context, Result},
    sqlil,
};
use pgx::pg_sys::{self, Node};

use crate::fdw::ctx::FdwContext;

use super::{datum::from_datum, r#type::from_pg_type, ConversionContext, PlannerContext};

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
        pg_sys::NodeTag_T_OpExpr => {
            convert_op_expr(node as *const pg_sys::OpExpr, ctx, planner, fdw)
        }
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

unsafe fn convert_const(
    node: *const pg_sys::Const,
    _ctx: &mut ConversionContext,
    _planner: &PlannerContext,
    _fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    if (*node).constisnull {
        return Ok(sqlil::Expr::constant(DataValue::Null));
    }

    let val = from_datum((*node).consttype, (*node).constvalue)
        .context("Failed to evaluation const expr")?;
    Ok(sqlil::Expr::constant(val))
}

unsafe fn convert_param(
    node: *const pg_sys::Param,
    ctx: &mut ConversionContext,
    _planner: &PlannerContext,
    _fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    // @see https://doxygen.postgresql.org/deparse_8c_source.html#l00405
    if (*node).paramkind == pg_sys::ParamKind_PARAM_MULTIEXPR {
        bail!("MULTIEXPR params are not supported");
    }

    let r#type =
        from_pg_type((*node).paramtype).context("Failed to determine type of query parameter")?;
    let param_id = ctx.register_param((*node).paramkind, (*node).paramid);

    Ok(sqlil::Expr::Parameter(sqlil::Parameter::new(
        r#type, param_id,
    )))
}

unsafe fn convert_var(
    node: *const pg_sys::Var,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

unsafe fn convert_func_expr(
    node: *const pg_sys::FuncExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

unsafe fn convert_op_expr(
    node: *const pg_sys::OpExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

unsafe fn convert_distinct_expr(
    node: *const pg_sys::OpExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

unsafe fn convert_relabel_type(
    node: *const pg_sys::RelabelType,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

unsafe fn convert_bool_expr(
    node: *const pg_sys::BoolExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

unsafe fn convert_null_test(
    node: *const pg_sys::NullTest,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

unsafe fn convert_case_expr(
    node: *const pg_sys::CaseExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

unsafe fn convert_aggref(
    node: *const pg_sys::Aggref,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    todo!()
}

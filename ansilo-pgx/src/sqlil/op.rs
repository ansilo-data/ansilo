use ansilo_core::{
    err::{bail, Context, Result},
    sqlil,
};
use pgx::{
    pg_sys::{self, FormData_pg_operator, Node},
    *,
};

use crate::{fdw::ctx::FdwContext, util::syscache::PgSysCacheItem};

use super::*;

pub unsafe fn convert_op_expr(
    node: *const pg_sys::OpExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let op = PgSysCacheItem::<FormData_pg_operator>::search(
        pg_sys::SysCacheIdentifier_OPEROID,
        [Datum::from((*node).opno)],
    )
    .context("Failed to lookup operator in sys cache")?;

    if op.oprkind == b'l' as i8 || op.oprkind == b'r' as i8 {
        return convert_unary_op_expr(node, op, ctx, planner, fdw);
    }

    if (*op).oprkind == b'b' as i8 {
        return convert_binary_op_expr(node, op, ctx, planner, fdw);
    }

    bail!("Unknown operator kind: {}", op.oprkind)
}

pub unsafe fn convert_unary_op_expr(
    node: *const pg_sys::OpExpr,
    op: PgSysCacheItem<FormData_pg_operator>,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let op = pg_sys::name_data_to_str(&op.oprname);
    let expr = PgList::<Node>::from_pg((*node).args).head().unwrap();
    let expr = convert(expr, ctx, planner, fdw)?;

    Ok(match op {
        "-" => sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(sqlil::UnaryOpType::Negate, expr)),
        // TODO: add operators
        _ => bail!("Unsupported unary operator: '{}'", op),
    })
}

pub unsafe fn convert_binary_op_expr(
    node: *const pg_sys::OpExpr,
    op: PgSysCacheItem<FormData_pg_operator>,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let op = pg_sys::name_data_to_str(&op.oprname);
    let operands = PgList::<Node>::from_pg((*node).args);
    let left = operands.get_ptr(0).unwrap();
    let right = operands.get_ptr(1).unwrap();
    let left = convert(left, ctx, planner, fdw)?;
    let right = convert(right, ctx, planner, fdw)?;

    Ok(match op {
        "+" => sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(left, sqlil::BinaryOpType::Add, right)),
        // TODO: add operators
        _ => bail!("Unsupported binary operator: '{}'", op),
    })
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::common::data::*;

    #[pg_test]
    fn test_sqlil_convert_op_unary_negate() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT -$1",
            &mut ConversionContext::new(),
            vec![DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
                sqlil::UnaryOpType::Negate,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_op_binary_add() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 + $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::Add,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }
}

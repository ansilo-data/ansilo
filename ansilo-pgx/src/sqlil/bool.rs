use ansilo_core::{
    err::{bail, Result},
    sqlil,
};
use pgx::{
    pg_sys::{self, Node},
    *,
};

use crate::fdw::ctx::FdwContext;

use super::*;

pub unsafe fn convert_bool_expr(
    node: *const pg_sys::BoolExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let operands = PgList::<Node>::from_pg((*node).args);

    if (*node).boolop == pg_sys::BoolExprType_NOT_EXPR {
        let expr = operands.get_ptr(0).unwrap();
        let expr = convert(expr, ctx, planner, fdw)?;
        return Ok(sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
            sqlil::UnaryOpType::LogicalNot,
            expr,
        )));
    }

    let left = operands.get_ptr(0).unwrap();
    let right = operands.get_ptr(1).unwrap();
    let left = convert(left, ctx, planner, fdw)?;
    let right = convert(right, ctx, planner, fdw)?;

    Ok(match (*node).boolop {
        pg_sys::BoolExprType_AND_EXPR => sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
            left,
            sqlil::BinaryOpType::LogicalAnd,
            right,
        )),
        pg_sys::BoolExprType_OR_EXPR => sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
            left,
            sqlil::BinaryOpType::LogicalOr,
            right,
        )),
        // TODO: add operators
        _ => bail!("Unsupported bool operator: '{}'", (*node).boolop),
    })
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::data::*;

    #[pg_test]
    fn test_sqlil_convert_bool_logical_and() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 AND $2",
            &mut ConversionContext::new(),
            vec![DataType::Boolean, DataType::Boolean],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Boolean, 1)),
                sqlil::BinaryOpType::LogicalAnd,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Boolean, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_bool_logical_or() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 OR $2",
            &mut ConversionContext::new(),
            vec![DataType::Boolean, DataType::Boolean],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Boolean, 1)),
                sqlil::BinaryOpType::LogicalOr,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Boolean, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_bool_logical_not() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT NOT $1",
            &mut ConversionContext::new(),
            vec![DataType::Boolean],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
                sqlil::UnaryOpType::LogicalNot,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Boolean, 1))
            ))
        );
    }
}

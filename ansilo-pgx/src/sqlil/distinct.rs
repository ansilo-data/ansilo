use ansilo_core::{
    err::{bail, Result},
    sqlil,
};
use pgx::{
    pg_sys::{self, Node},
    *,
};

use crate::fdw::ctx::{FdwContext, PlannerContext};

use super::*;

pub unsafe fn convert_distinct_expr(
    node: *const pg_sys::DistinctExpr,
    ctx: &mut ConversionContext,
    planner: &PlannerContext,
    fdw: &FdwContext,
) -> Result<sqlil::Expr> {
    let operands = PgList::<Node>::from_pg((*node).args);
    let left = operands.get_ptr(0).unwrap();
    let right = operands.get_ptr(1).unwrap();
    let left = convert(left, ctx, planner, fdw)?;
    let right = convert(right, ctx, planner, fdw)?;

    Ok(sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
        left,
        sqlil::BinaryOpType::NullSafeEqual,
        right,
    )))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    use crate::sqlil::test;
    use ansilo_core::data::*;

    #[pg_test]
    fn test_sqlil_convert_distinct() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 IS DISTINCT FROM $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                sqlil::BinaryOpType::NullSafeEqual,
                sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
            ))
        );
    }

    #[pg_test]
    fn test_sqlil_convert_distinct_not() {
        let expr = test::convert_simple_expr_with_context(
            "SELECT $1 IS NOT DISTINCT FROM $2",
            &mut ConversionContext::new(),
            vec![DataType::Int32, DataType::Int32],
        )
        .unwrap();

        assert_eq!(
            expr,
            sqlil::Expr::UnaryOp(sqlil::UnaryOp::new(
                sqlil::UnaryOpType::LogicalNot,
                sqlil::Expr::BinaryOp(sqlil::BinaryOp::new(
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 1)),
                    sqlil::BinaryOpType::NullSafeEqual,
                    sqlil::Expr::Parameter(sqlil::Parameter::new(DataType::Int32, 2))
                ))
            ))
        );
    }
}
